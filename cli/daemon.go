package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"manta-pipeline/brokerpb"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

//go:embed embed/mantapipeline
var mantapipelineFS embed.FS

const (
	mantaDir         = ".manta"
	daemonSocketPath = ".manta/manta-daemon.sock"
	daemonLogsDir    = ".manta/logs/daemon"
	apiLogsDir       = ".manta/logs/api"
)

type WorkRunState struct {
	Pipeline  string
	Stage     string
	Name      string
	StepIdx   int
	Status    string // "pending" | "running" | "done" | "failed"
	StartedAt time.Time
	EndedAt   time.Time
}

type lifecyclePhase struct {
	Name      string
	Status    string // "pending" | "running" | "done" | "failed"
	StartedAt time.Time
	EndedAt   time.Time
	Error     string
}

type runnerSession struct {
	ID          string
	FileStem    string
	Target      string
	Status      string // "running" | "done" | "failed"
	cancel      context.CancelFunc
	rayJobIDs   map[string]struct{}
	Works       []*WorkRunState
	WorkByCtx   map[string]*WorkRunState
	StepCount   int
	CurrentStep int
}

type daemonStatus struct {
	mu             sync.Mutex
	Phase          string // "starting" | "running" | "stopping"
	Tunnel         string // "connecting" | "up" | "reconnecting" | "stopped"
	SSHPid         int
	daemonID       string
	localOnly      bool
	sessions       map[string]*runnerSession
	brokerClient   brokerpb.BrokerClient
	brokerConn     *grpc.ClientConn
	brokerJobID    string
	brokerReady    chan struct{}
	disconnectOnly bool // if true, shutdown skips ray down and broker stop
	phases         []*lifecyclePhase
}

func (s *daemonStatus) setPhases(names []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phases = make([]*lifecyclePhase, len(names))
	for i, n := range names {
		s.phases[i] = &lifecyclePhase{Name: n, Status: "pending"}
	}
}

func (s *daemonStatus) startPhase(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.phases {
		if st.Name == name {
			st.Status = "running"
			st.StartedAt = time.Now()
			return
		}
	}
}

func (s *daemonStatus) endPhase(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.phases {
		if st.Name == name {
			st.EndedAt = time.Now()
			if err != nil {
				st.Status = "failed"
				st.Error = err.Error()
			} else {
				st.Status = "done"
			}
			return
		}
	}
}

func (s *daemonStatus) setPhase(phase string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Phase = phase
}

func (s *daemonStatus) setTunnel(tunnel string, pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Tunnel = tunnel
	s.SSHPid = pid
}

func (s *daemonStatus) addSession(sess *runnerSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sess.ID] = sess
}

func (s *daemonStatus) setSessionStatus(id, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[id]; ok {
		sess.Status = status
	}
}

func (s *daemonStatus) initWorks(sessionID string, steps [][]ResolvedWork) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return
	}
	sess.StepCount = len(steps)
	sess.CurrentStep = -1
	for wi, step := range steps {
		for _, rw := range step {
			ws := &WorkRunState{
				Pipeline: rw.Pipeline,
				Stage:    rw.Stage,
				Name:     rw.Work.Name,
				StepIdx:  wi,
				Status:   "pending",
			}
			sess.Works = append(sess.Works, ws)
			sess.WorkByCtx[rw.Context()] = ws
		}
	}
}

func (s *daemonStatus) setCurrentStep(sessionID string, stepIdx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[sessionID]; ok {
		sess.CurrentStep = stepIdx
	}
}

func (s *daemonStatus) markWorkRunning(sessionID, ctx string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[sessionID]; ok {
		if ws, ok2 := sess.WorkByCtx[ctx]; ok2 {
			ws.Status = "running"
			ws.StartedAt = time.Now()
		}
	}
}

func (s *daemonStatus) markWorkDone(sessionID, ctx string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, exists := s.sessions[sessionID]; exists {
		if ws, exists2 := sess.WorkByCtx[ctx]; exists2 {
			ws.EndedAt = time.Now()
			if ok {
				ws.Status = "done"
			} else {
				ws.Status = "failed"
			}
		}
	}
}

func (s *daemonStatus) addRayJob(sessionID, jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[sessionID]; ok {
		sess.rayJobIDs[jobID] = struct{}{}
	}
}

func (s *daemonStatus) removeRayJob(sessionID, jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[sessionID]; ok {
		delete(sess.rayJobIDs, jobID)
	}
}

func stopRayJobs(jobIDs []string) {
	for _, id := range jobIDs {
		streamCommand("uv", "run", "ray", "job", "stop",
			"--address", "http://localhost:8265", id)
	}
}

func (s *daemonStatus) cancelSession(id string) error {
	s.mu.Lock()
	sess, ok := s.sessions[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("session %s not found", id)
	}
	if sess.Status != "running" {
		s.mu.Unlock()
		return fmt.Errorf("session %s is not running (status: %s)", id, sess.Status)
	}
	jobIDs := make([]string, 0, len(sess.rayJobIDs))
	for jid := range sess.rayJobIDs {
		jobIDs = append(jobIDs, jid)
	}
	sess.cancel()
	s.mu.Unlock()
	if len(jobIDs) > 0 {
		go stopRayJobs(jobIDs)
	}
	return nil
}

func (s *daemonStatus) cancelAllSessions() int {
	s.mu.Lock()
	var allJobIDs []string
	count := 0
	for _, sess := range s.sessions {
		if sess.Status == "running" {
			for jid := range sess.rayJobIDs {
				allJobIDs = append(allJobIDs, jid)
			}
			sess.cancel()
			count++
		}
	}
	s.mu.Unlock()
	if len(allJobIDs) > 0 {
		go stopRayJobs(allJobIDs)
	}
	return count
}

func (s *daemonStatus) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessions := make([]map[string]any, 0, len(s.sessions))
	for _, sess := range s.sessions {
		works := make([]map[string]any, 0, len(sess.Works))
		for _, ws := range sess.Works {
			works = append(works, map[string]any{
				"pipeline":   ws.Pipeline,
				"stage":      ws.Stage,
				"name":       ws.Name,
				"step_idx":   ws.StepIdx,
				"status":     ws.Status,
				"started_at": tsOrEmpty(ws.StartedAt),
				"ended_at":   tsOrEmpty(ws.EndedAt),
			})
		}
		sessions = append(sessions, map[string]any{
			"id":           sess.ID,
			"file_stem":    sess.FileStem,
			"target":       sess.Target,
			"status":       sess.Status,
			"step_count":   sess.StepCount,
			"current_step": sess.CurrentStep,
			"works":        works,
		})
	}
	phasesOut := make([]map[string]any, 0, len(s.phases))
	for _, st := range s.phases {
		phasesOut = append(phasesOut, map[string]any{
			"name":       st.Name,
			"status":     st.Status,
			"started_at": tsOrEmpty(st.StartedAt),
			"ended_at":   tsOrEmpty(st.EndedAt),
			"error":      st.Error,
		})
	}
	return map[string]any{
		"phase":      s.Phase,
		"tunnel":     s.Tunnel,
		"ssh_pid":    s.SSHPid,
		"local_only": s.localOnly,
		"sessions":   sessions,
		"phases":     phasesOut,
	}
}

func tsOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

type stepView struct {
	SessionID   string     `json:"session_id"`
	Status      string     `json:"status"`
	StepCount   int        `json:"step_count"`
	CurrentStep int        `json:"current_step"`
	Works       []workView `json:"works"`
}

type workView struct {
	Pipeline  string `json:"pipeline"`
	Stage     string `json:"stage"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at"`
}

func (s *daemonStatus) stepSnapshot(sessionID string) (*stepView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %s not found", sessionID)
	}
	wv := &stepView{
		SessionID:   sess.ID,
		Status:      sess.Status,
		StepCount:   sess.StepCount,
		CurrentStep: sess.CurrentStep,
	}
	for _, ws := range sess.Works {
		if ws.StepIdx != sess.CurrentStep {
			continue
		}
		wv.Works = append(wv.Works, workView{
			Pipeline:  ws.Pipeline,
			Stage:     ws.Stage,
			Name:      ws.Name,
			Status:    ws.Status,
			StartedAt: tsOrEmpty(ws.StartedAt),
			EndedAt:   tsOrEmpty(ws.EndedAt),
		})
	}
	return wv, nil
}

func isDaemonRunning() bool {
	conn, err := net.Dial("unix", daemonSocketPath)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// spawnDaemon launches the background daemon process and waits up to 5s for
// the unix socket to appear. It does NOT wait for the daemon to reach phase
// "running" — that's the caller's responsibility (via the lifecycle view or
// a raw-log tail).
func spawnDaemon(connectOnly bool) (logPath string, err error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(daemonLogsDir, 0o755); err != nil {
		return "", err
	}

	daemonID := "manta-" + randHex(3)
	ts := time.Now().Format("20060102-150405")
	logFilePath := daemonLogsDir + "/" + ts + "-" + daemonID + ".log"
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", err
	}

	args := []string{"manta-daemon-run", daemonID}
	if connectOnly {
		args = append(args, "--connect")
	}
	cmd := exec.Command(exe, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin = nil
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return "", err
	}
	cmd.Process.Release()
	logFile.Close()

	socketDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(socketDeadline) {
		time.Sleep(100 * time.Millisecond)
		if isDaemonRunning() {
			return logFilePath, nil
		}
	}
	return "", fmt.Errorf("daemon did not start within 5s — check %s", logFilePath)
}

func sendClientCommand(command string) (string, error) {
	conn, err := net.Dial("unix", daemonSocketPath)
	if err != nil {
		return "", fmt.Errorf("daemon not running")
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	fmt.Fprintf(conn, "%s\n", command)

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("no response from daemon")
}

func runDaemon() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ldate | log.Ltime)

	daemonID := ""
	if len(os.Args) >= 3 {
		daemonID = os.Args[2]
	}
	connectOnly := len(os.Args) >= 4 && os.Args[3] == "--connect"

	state := &daemonStatus{
		Phase:       "starting",
		Tunnel:      "stopped",
		daemonID:    daemonID,
		sessions:    make(map[string]*runnerSession),
		brokerReady: make(chan struct{}),
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// tunnelCtx outlives ctx through the broker-stop phase so that
	// `ray job stop` (which talks to the broker via the localhost:8265
	// forward) doesn't lose its tunnel mid-call.
	tunnelCtx, tunnelCancel := context.WithCancel(context.Background())
	defer tunnelCancel()

	mf, err := loadMantafile()
	if err != nil {
		log.Printf("daemon: failed to load config: %v", err)
		os.Exit(1)
	}
	state.localOnly = mf.Cluster == ""

	var cfg *ClusterConfig
	if !state.localOnly {
		cfg, err = parseClusterConfig(mf.Cluster)
		if err != nil {
			log.Printf("daemon: failed to parse cluster config: %v", err)
			os.Exit(1)
		}
	}

	os.Remove(daemonSocketPath)
	ln, err := net.Listen("unix", daemonSocketPath)
	if err != nil {
		log.Printf("daemon: socket error: %v", err)
		os.Exit(1)
	}
	defer os.Remove(daemonSocketPath)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleDaemonConn(conn, state, ctx, cancel)
		}
	}()

	go func() {
		if state.localOnly {
			state.setPhases([]string{"ray start --head", "broker start", "broker connect"})
			state.startPhase("ray start --head")
			log.Printf("daemon: running ray start --head")
			err := streamCommand("uv", "run", "ray", "start", "--head")
			state.endPhase("ray start --head", err)
			if err != nil {
				log.Printf("daemon: ray start failed: %v", err)
				cancel()
				return
			}
		} else if connectOnly {
			state.setPhases([]string{"ssh tunnel", "cluster reachable", "broker attach"})
			state.startPhase("ssh tunnel")
			go runTunnelLoop(tunnelCtx, state, cfg)
			if err := waitForTunnelUp(ctx, state, 30*time.Second); err != nil {
				state.endPhase("ssh tunnel", err)
				log.Printf("daemon: ssh tunnel failed: %v", err)
				cancel()
				return
			}
			state.endPhase("ssh tunnel", nil)

			state.startPhase("cluster reachable")
			log.Printf("daemon: connect mode — verifying cluster is reachable")
			err := waitForTCP(ctx, "localhost:8265", 20*time.Second)
			state.endPhase("cluster reachable", err)
			if err != nil {
				log.Printf("daemon: cluster not running — use 'manta-pipeline up' first")
				cancel()
				return
			}
			state.mu.Lock()
			state.disconnectOnly = true
			state.mu.Unlock()
		} else {
			rayUpPhase := "ray up " + mf.Cluster
			state.setPhases([]string{rayUpPhase, "ssh tunnel", "broker start", "broker connect"})
			state.startPhase(rayUpPhase)
			log.Printf("daemon: running ray up")
			err := streamCommand("uv", "run", "ray", "up", "--yes", mf.Cluster)
			state.endPhase(rayUpPhase, err)
			if err != nil {
				log.Printf("daemon: ray up failed: %v", err)
				cancel()
				return
			}

			state.startPhase("ssh tunnel")
			go runTunnelLoop(tunnelCtx, state, cfg)
			if err := waitForTunnelUp(ctx, state, 30*time.Second); err != nil {
				state.endPhase("ssh tunnel", err)
				log.Printf("daemon: ssh tunnel failed: %v", err)
				cancel()
				return
			}
			state.endPhase("ssh tunnel", nil)
		}
		if err := startBroker(ctx, state); err != nil {
			log.Printf("daemon: broker failed: %v", err)
			cancel()
			return
		}
		state.setPhase("running")
	}()

	<-ctx.Done()
	state.setPhase("stopping")

	state.mu.Lock()
	brokerJobID := state.brokerJobID
	brokerConn := state.brokerConn
	disconnectOnly := state.disconnectOnly
	state.mu.Unlock()

	if disconnectOnly {
		state.setPhases([]string{"close broker connection"})
		state.startPhase("close broker connection")
		if brokerConn != nil {
			brokerConn.Close()
		}
		state.endPhase("close broker connection", nil)
		log.Printf("daemon: disconnected (cluster still running)")
	} else {
		rayDownPhase := "ray down"
		if state.localOnly {
			rayDownPhase = "ray stop"
		}
		state.setPhases([]string{"stop broker", rayDownPhase})

		state.startPhase("stop broker")
		if brokerConn != nil {
			brokerConn.Close()
		}
		var brokerErr error
		if brokerJobID != "" {
			log.Printf("daemon: stopping broker")
			brokerErr = streamCommand("uv", "run", "ray", "job", "stop", "--address", "http://localhost:8265", brokerJobID)
		}
		state.endPhase("stop broker", brokerErr)

		// Broker is gone — safe to tear the tunnel down. ray down does its own
		// direct SSH to head_ip, so it doesn't depend on the forward.
		tunnelCancel()

		state.startPhase(rayDownPhase)
		var rayErr error
		if state.localOnly {
			log.Printf("daemon: running ray stop")
			rayErr = streamCommand("uv", "run", "ray", "stop")
		} else {
			log.Printf("daemon: running ray down")
			rayErr = streamCommand("uv", "run", "ray", "down", "-y", mf.Cluster)
		}
		state.endPhase(rayDownPhase, rayErr)
	}

	log.Printf("daemon: stopped")
	ln.Close()
}

func waitForTunnelUp(ctx context.Context, state *daemonStatus, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		state.mu.Lock()
		t := state.Tunnel
		state.mu.Unlock()
		if t == "up" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ssh tunnel did not come up within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitForTCP(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", addr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// writeEmbeddedPackage writes the embedded mantapipeline package into
// destDir/mantapipeline so it can be imported as a package — its protobuf
// stubs use package-relative imports. Used for both work runs and the broker.
func writeEmbeddedPackage(destDir string) error {
	pkgDir := filepath.Join(destDir, "mantapipeline")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		return err
	}
	entries, err := mantapipelineFS.ReadDir("embed/mantapipeline")
	if err != nil {
		return err
	}
	for _, e := range entries {
		data, err := mantapipelineFS.ReadFile("embed/mantapipeline/" + e.Name())
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(pkgDir, e.Name()), data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func startBroker(ctx context.Context, state *daemonStatus) error {
	state.mu.Lock()
	connectMode := state.disconnectOnly
	state.mu.Unlock()

	currentPhase := "broker start"
	if connectMode {
		currentPhase = "broker attach"
	}
	state.startPhase(currentPhase)
	fail := func(err error) error {
		state.endPhase(currentPhase, err)
		return err
	}

	if connectMode {
		if waitForTCP(ctx, "localhost:5001", 5*time.Second) == nil {
			log.Printf("daemon: broker already running, attaching")
			conn, err := grpc.NewClient("localhost:5001",
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fail(fmt.Errorf("broker grpc dial: %w", err))
			}
			state.mu.Lock()
			state.brokerClient = brokerpb.NewBrokerClient(conn)
			state.brokerConn = conn
			// brokerJobID stays empty: we did not start this broker, shutdown skips stopping it
			state.mu.Unlock()
			close(state.brokerReady)
			state.endPhase(currentPhase, nil)
			return nil
		}
		log.Printf("daemon: broker not found, starting one")
		// Fall through; "broker attach" stays running until we connect to the freshly started broker.
	}

	brokerJobID := state.daemonID + "-broker"

	tmpDir, err := os.MkdirTemp("", "manta-broker-*")
	if err != nil {
		return fail(fmt.Errorf("broker tmpdir: %w", err))
	}

	if err := writeEmbeddedPackage(tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return fail(fmt.Errorf("write broker package: %w", err))
	}

	if err := streamToLogger(ctx, log.Default(), "uv", "run", "ray", "job", "submit",
		"--address", "http://localhost:8265",
		"--no-wait",
		"--submission-id", brokerJobID,
		"--working-dir", tmpDir,
		"--", "python", "-m", "mantapipeline.broker_server"); err != nil {
		os.RemoveAll(tmpDir)
		return fail(fmt.Errorf("broker submit: %w", err))
	}
	os.RemoveAll(tmpDir)

	go tailBrokerLogs(ctx, brokerJobID, log.Default())

	if err := waitForTCP(ctx, "localhost:5001", 60*time.Second); err != nil {
		return fail(fmt.Errorf("broker ready: %w", err))
	}
	log.Printf("daemon: broker ready")
	if !connectMode {
		state.endPhase("broker start", nil)
		currentPhase = "broker connect"
		state.startPhase(currentPhase)
	}

	conn, err := grpc.NewClient("localhost:5001",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fail(fmt.Errorf("broker grpc dial: %w", err))
	}

	state.mu.Lock()
	state.brokerClient = brokerpb.NewBrokerClient(conn)
	state.brokerConn = conn
	state.brokerJobID = brokerJobID
	state.mu.Unlock()
	close(state.brokerReady)
	state.endPhase(currentPhase, nil)
	return nil
}

func tailBrokerLogs(ctx context.Context, jobID string, logger *log.Logger) {
	cmd := exec.CommandContext(ctx, "uv", "run", "ray", "job", "logs",
		"--address", "http://localhost:8265",
		"--follow", jobID)
	pr, pw, err := os.Pipe()
	if err != nil {
		return
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return
	}
	pw.Close()
	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		logger.Printf("[broker] %s", scanner.Text())
	}
	pr.Close()
	cmd.Wait()
}

func streamToLogger(ctx context.Context, logger *log.Logger, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return err
	}
	pw.Close()
	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		logger.Print(scanner.Text())
	}
	pr.Close()
	return cmd.Wait()
}

func runWorkLocally(ctx context.Context, rw ResolvedWork, tmpDir, sessionID string, logger *log.Logger) error {
	cmd := exec.CommandContext(ctx, "uv", "run", "python", rw.Work.Entrypoint)
	cmd.Dir = tmpDir
	// Python's `python script.py` puts the script's directory on sys.path,
	// not tmpDir, so a subdirectory entrypoint can't import mantapipeline
	// from tmpDir/mantapipeline/. Mirror Ray's working-dir runtime env.
	pythonPath := tmpDir
	if existing := os.Getenv("PYTHONPATH"); existing != "" {
		pythonPath = tmpDir + ":" + existing
	}
	cmd.Env = append(os.Environ(),
		"MANTA_WORK_CONTEXT="+rw.Context(),
		"MANTA_SESSION_ID="+sessionID,
		"PYTHONPATH="+pythonPath,
	)
	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return err
	}
	pw.Close()
	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		logger.Print(scanner.Text())
	}
	pr.Close()
	return cmd.Wait()
}

func startSession(ctx context.Context, state *daemonStatus, target string) (sessionID, fileStem string, err error) {
	select {
	case <-state.brokerReady:
	case <-ctx.Done():
		return "", "", fmt.Errorf("daemon shutting down")
	}

	mf, err := loadMantafile()
	if err != nil {
		return "", "", fmt.Errorf("reload pipeline: %w", err)
	}
	t, err := parseRunTarget(target)
	if err != nil {
		return "", "", err
	}

	var pipeline *Pipeline
	for _, p := range mf.Pipelines {
		if p.Name == t.Pipeline {
			pipeline = p
			break
		}
	}
	if pipeline == nil {
		return "", "", fmt.Errorf("pipeline %q not found", t.Pipeline)
	}

	steps, err := resolveWorks(pipeline, t)
	if err != nil {
		return "", "", err
	}

	sessionID = state.daemonID + "-" + randHex(2)
	fileStem = time.Now().Format("20060102-150405") + "-" + sessionID
	if err := os.MkdirAll(runsDir, 0o755); err != nil {
		return "", "", err
	}

	logFile := runsDir + "/" + fileStem + ".log"
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", "", err
	}

	// Create the API log file eagerly so a client tailing it right after
	// receiving the session ID doesn't race the worker goroutine's open.
	if err := os.MkdirAll(apiLogsDir, 0o755); err != nil {
		f.Close()
		return "", "", err
	}
	af, err := os.OpenFile(apiLogsDir+"/"+fileStem+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		f.Close()
		return "", "", err
	}
	af.Close()

	sessCtx, sessCancel := context.WithCancel(ctx)
	sess := &runnerSession{ID: sessionID, FileStem: fileStem, Target: target, Status: "running", cancel: sessCancel, rayJobIDs: make(map[string]struct{}), WorkByCtx: make(map[string]*WorkRunState), CurrentStep: -1}
	state.addSession(sess)

	logger := log.New(f, "", log.Ldate|log.Ltime)

	go func() {
		defer f.Close()
		defer sessCancel()
		runSessionWorker(sessCtx, sessionID, fileStem, steps, pipeline, logger, state)
	}()

	return sessionID, fileStem, nil
}

func runSessionWorker(ctx context.Context, sessionID, fileStem string, steps [][]ResolvedWork, pipeline *Pipeline, logger *log.Logger, state *daemonStatus) {
	doneFile := runsDir + "/" + fileStem + ".done"
	defer os.WriteFile(doneFile, []byte("done"), 0o644)

	if err := os.MkdirAll(apiLogsDir, 0o755); err != nil {
		logger.Printf("session %s: failed to create api logs dir: %v", sessionID, err)
		state.setSessionStatus(sessionID, "failed")
		return
	}
	apiLogFile, err := os.OpenFile(apiLogsDir+"/"+fileStem+".log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		logger.Printf("session %s: failed to open api log: %v", sessionID, err)
		state.setSessionStatus(sessionID, "failed")
		return
	}
	defer apiLogFile.Close()
	apiLogger := log.New(apiLogFile, "", log.Ldate|log.Ltime)

	totalWorks := 0
	for _, w := range steps {
		totalWorks += len(w)
	}
	logger.Printf("session %s: %d work(s) in %d step(s)", sessionID, totalWorks, len(steps))
	state.initWorks(sessionID, steps)

	state.mu.Lock()
	brokerClient := state.brokerClient
	state.mu.Unlock()

	var stream brokerpb.Broker_SubscribeClient
	var streamErr error
	for i := range 20 {
		stream, streamErr = brokerClient.Subscribe(ctx, &brokerpb.SubscribeRequest{SessionId: sessionID})
		if streamErr == nil {
			break
		}
		if i < 19 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if streamErr != nil {
		logger.Printf("session %s: subscribe failed: %v", sessionID, streamErr)
		state.setSessionStatus(sessionID, "failed")
		return
	}
	logger.Printf("session %s: subscribed to broker", sessionID)

	var barStream brokerpb.Broker_SubscribeProgressBarsClient
	var barErr error
	for i := range 20 {
		barStream, barErr = brokerClient.SubscribeProgressBars(ctx, &brokerpb.SubscribeRequest{SessionId: sessionID})
		if barErr == nil {
			break
		}
		if i < 19 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if barErr != nil {
		logger.Printf("session %s: subscribe progress bars failed: %v", sessionID, barErr)
		state.setSessionStatus(sessionID, "failed")
		return
	}

	var runnerWG sync.WaitGroup
	runnerWG.Add(2)
	go func() {
		defer runnerWG.Done()
		for {
			event, err := stream.Recv()
			if err != nil {
				return
			}
			apiLogger.Printf("%s  %s", event.WorkContext, event.Text)
		}
	}()
	go func() {
		defer runnerWG.Done()
		for {
			bar, err := barStream.Recv()
			if err != nil {
				return
			}
			buf, _ := json.Marshal(struct {
				Work  string  `json:"work"`
				Name  string  `json:"name"`
				Value float64 `json:"value"`
				Min   float64 `json:"min"`
				Max   float64 `json:"max"`
				IsInt bool    `json:"is_int"`
			}{
				Work: bar.WorkContext, Name: bar.Name,
				Value: bar.Value, Min: bar.Min, Max: bar.Max,
				IsInt: bar.IsInt,
			})
			apiLogger.Printf("__bar__ %s", buf)
		}
	}()
	defer func() {
		done := make(chan struct{})
		go func() { runnerWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			logger.Printf("session %s: runner drain timed out", sessionID)
		}
	}()

	tmpDir, err := os.MkdirTemp("", "manta-run-*")
	if err != nil {
		logger.Printf("session %s: failed to create work dir: %v", sessionID, err)
		state.setSessionStatus(sessionID, "failed")
		return
	}
	defer os.RemoveAll(tmpDir)

	srcDir := filepath.Join(pipeline.Root, pipeline.SrcDir)
	if err := exec.CommandContext(ctx, "cp", "-r", srcDir+"/.", tmpDir).Run(); err != nil {
		logger.Printf("session %s: failed to copy src: %v", sessionID, err)
		state.setSessionStatus(sessionID, "failed")
		return
	}

	if pipeline.Dependencies != "" {
		depsPath := filepath.Join(pipeline.Root, pipeline.Dependencies)
		destPath := filepath.Join(tmpDir, filepath.Base(depsPath))
		if err := exec.CommandContext(ctx, "cp", depsPath, destPath).Run(); err != nil {
			logger.Printf("session %s: failed to copy deps: %v", sessionID, err)
			state.setSessionStatus(sessionID, "failed")
			return
		}
	}

	if err := writeEmbeddedPackage(tmpDir); err != nil {
		logger.Printf("session %s: failed to write mantapipeline package: %v", sessionID, err)
		state.setSessionStatus(sessionID, "failed")
		return
	}

	submitWork := func(rw ResolvedWork) error {
		params := rw.Work.Params
		if params == nil {
			params = []ParamEntry{}
		}
		paramsJSON, _ := json.Marshal(params)
		if _, err := brokerClient.RegisterParams(ctx, &brokerpb.WorkParams{
			SessionId:   sessionID,
			WorkContext: rw.Context(),
			ParamsJson:  string(paramsJSON),
		}); err != nil {
			logger.Printf("work %s: failed to register params: %v", rw.Work.Name, err)
		}

		if rw.Work.LocalMode && !state.localOnly {
			logger.Printf("running locally %s — %s", rw.Work.Name, rw.Work.Entrypoint)
			return runWorkLocally(ctx, rw, tmpDir, sessionID, logger)
		}

		runtimeEnv, _ := json.Marshal(map[string]any{
			"env_vars": map[string]string{
				"MANTA_WORK_CONTEXT": rw.Context(),
				"MANTA_SESSION_ID":   sessionID,
			},
		})

		submissionID := sessionID + "-" + randHex(2)
		state.addRayJob(sessionID, submissionID)
		logger.Printf("submitting %s — %s", rw.Work.Name, rw.Work.Entrypoint)
		err := streamToLogger(ctx, logger, "uv", "run", "ray", "job", "submit",
			"--address", "http://localhost:8265",
			"--submission-id", submissionID,
			"--working-dir", tmpDir,
			"--runtime-env-json", string(runtimeEnv),
			"--", "uv", "run", "python", rw.Work.Entrypoint)
		state.removeRayJob(sessionID, submissionID)
		return err
	}

	runOne := func(rw ResolvedWork) error {
		state.markWorkRunning(sessionID, rw.Context())
		err := submitWork(rw)
		state.markWorkDone(sessionID, rw.Context(), err == nil)
		return err
	}

	for stepIdx, step := range steps {
		select {
		case <-ctx.Done():
			logger.Printf("session %s: cancelled", sessionID)
			state.setSessionStatus(sessionID, "failed")
			return
		default:
		}

		state.setCurrentStep(sessionID, stepIdx)
		logger.Printf("step %d: %d work(s)", stepIdx+1, len(step))

		if len(step) == 1 {
			rw := step[0]
			if err := runOne(rw); err != nil {
				logger.Printf("work %s failed: %v", rw.Work.Name, err)
				state.setSessionStatus(sessionID, "failed")
				return
			}
			logger.Printf("work %s done", rw.Work.Name)
		} else {
			type workResult struct {
				name string
				err  error
			}
			results := make(chan workResult, len(step))
			var wg sync.WaitGroup
			for _, rw := range step {
				wg.Add(1)
				rw := rw
				go func() {
					defer wg.Done()
					results <- workResult{rw.Work.Name, runOne(rw)}
				}()
			}
			wg.Wait()
			close(results)

			stepFailed := false
			for r := range results {
				if r.err != nil {
					logger.Printf("work %s failed: %v", r.name, r.err)
					stepFailed = true
				} else {
					logger.Printf("work %s done", r.name)
				}
			}
			if stepFailed {
				state.setSessionStatus(sessionID, "failed")
				return
			}
		}
	}

	logger.Printf("session %s: done", sessionID)
	state.setSessionStatus(sessionID, "done")
}

func handleDaemonConn(conn net.Conn, state *daemonStatus, ctx context.Context, cancel context.CancelFunc) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return
	}
	line := scanner.Text()

	state.mu.Lock()
	stopping := state.Phase == "stopping"
	state.mu.Unlock()
	if stopping && line != "status" && !strings.HasPrefix(line, "step ") {
		fmt.Fprintf(conn, "error: shutting down\n")
		return
	}

	if strings.HasPrefix(line, "run ") {
		target := strings.TrimPrefix(line, "run ")
		sessionID, fileStem, err := startSession(ctx, state, target)
		if err != nil {
			fmt.Fprintf(conn, "error: %v\n", err)
			return
		}
		fmt.Fprintf(conn, "%s %s\n", sessionID, fileStem)
		return
	}

	if strings.HasPrefix(line, "stop ") {
		id := strings.TrimPrefix(line, "stop ")
		if err := state.cancelSession(id); err != nil {
			fmt.Fprintf(conn, "error: %v\n", err)
			return
		}
		fmt.Fprintf(conn, "ok\n")
		return
	}

	if strings.HasPrefix(line, "step ") {
		id := strings.TrimPrefix(line, "step ")
		wv, err := state.stepSnapshot(id)
		if err != nil {
			fmt.Fprintf(conn, "error: %v\n", err)
			return
		}
		buf, _ := json.Marshal(wv)
		fmt.Fprintf(conn, "%s\n", buf)
		return
	}

	switch line {
	case "stopall":
		n := state.cancelAllSessions()
		fmt.Fprintf(conn, "%d\n", n)
	case "status":
		resp, _ := json.Marshal(state.snapshot())
		fmt.Fprintf(conn, "%s\n", resp)
	case "shutdown":
		fmt.Fprintf(conn, "ok\n")
		state.mu.Lock()
		state.disconnectOnly = false
		state.mu.Unlock()
		cancel()
	case "disconnect":
		fmt.Fprintf(conn, "ok\n")
		state.mu.Lock()
		state.disconnectOnly = true
		state.mu.Unlock()
		cancel()
	default:
		fmt.Fprintf(conn, "unknown command\n")
	}
}
