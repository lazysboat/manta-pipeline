package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const pipelineFile = ".manta/pipeline.json"

const (
	lifecycleUpTimeout   = 10 * time.Minute
	lifecycleDownTimeout = 5 * time.Minute
)

func pipelineUp(detach, plain bool) error {
	if isDaemonRunning() {
		fmt.Println("Pipeline already running. Use 'manta-pipeline status' to check.")
		return nil
	}
	if _, err := os.Stat(pipelineFile); os.IsNotExist(err) {
		return fmt.Errorf("no built pipeline found — run 'manta-pipeline build' first")
	}
	if _, err := loadMantafile(); err != nil {
		return err
	}

	fmt.Println("Starting pipeline...")
	logPath, err := spawnDaemon(false)
	if err != nil {
		return err
	}
	if detach {
		fmt.Printf("Pipeline starting — log: %s\n", logPath)
		return nil
	}
	if plain {
		return tailDaemonLogUntilReady(logPath)
	}
	return runLifecycle(flowUp, lifecycleUpTimeout, logPath)
}

func pipelineDown(detach, plain bool) error {
	logPath, _ := latestDaemonLog()
	resp, err := sendClientCommand("shutdown")
	if err != nil {
		return fmt.Errorf("pipeline not running")
	}
	if resp != "ok" {
		return nil
	}
	if detach {
		fmt.Println("Stopping pipeline — run 'manta-pipeline log' to follow progress.")
		return nil
	}
	if plain {
		fmt.Println("Stopping pipeline...")
		if logPath == "" {
			for isDaemonRunning() {
				time.Sleep(200 * time.Millisecond)
			}
		} else {
			tailLogUntilDone(logPath)
		}
		fmt.Println("Pipeline stopped.")
		return nil
	}
	return runLifecycle(flowDown, lifecycleDownTimeout, logPath)
}

func pipelineConnect(plain bool) error {
	if isDaemonRunning() {
		fmt.Println("Already connected. Use 'manta-pipeline status' to check.")
		return nil
	}
	if _, err := os.Stat(pipelineFile); os.IsNotExist(err) {
		return fmt.Errorf("no built pipeline found — run 'manta-pipeline build' first")
	}
	if _, err := loadMantafile(); err != nil {
		return err
	}
	fmt.Println("Connecting to cluster...")
	logPath, err := spawnDaemon(true)
	if err != nil {
		return err
	}
	if plain {
		return tailDaemonLogUntilReady(logPath)
	}
	return runLifecycle(flowUp, lifecycleUpTimeout, logPath)
}

func pipelineDisconnect(detach, plain bool) error {
	logPath, _ := latestDaemonLog()
	resp, err := sendClientCommand("disconnect")
	if err != nil {
		return fmt.Errorf("not connected")
	}
	if resp != "ok" {
		return nil
	}
	if detach {
		fmt.Println("Disconnecting — cluster remains running.")
		return nil
	}
	if plain {
		fmt.Println("Disconnecting...")
		if logPath == "" {
			for isDaemonRunning() {
				time.Sleep(200 * time.Millisecond)
			}
		} else {
			tailLogUntilDone(logPath)
		}
		fmt.Println("Disconnected. Cluster still running.")
		return nil
	}
	return runLifecycle(flowDown, lifecycleDownTimeout, logPath)
}

// tailDaemonLogUntilReady tails the daemon log live and returns once the
// daemon reaches phase "running" (success) or "stopping" (failure).
func tailDaemonLogUntilReady(logPath string) error {
	cmd := exec.Command("tail", "-n", "0", "-f", logPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
	}()
	for {
		resp, err := sendClientCommand("status")
		if err != nil {
			return fmt.Errorf("daemon exited during startup — check %s", logPath)
		}
		var data map[string]any
		if json.Unmarshal([]byte(resp), &data) == nil {
			switch data["phase"] {
			case "running":
				time.Sleep(200 * time.Millisecond) // let final log lines flush
				return nil
			case "stopping":
				time.Sleep(200 * time.Millisecond)
				return fmt.Errorf("daemon failed to start — check %s", logPath)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func pipelineStatus() error {
	resp, err := sendClientCommand("status")
	if err != nil {
		fmt.Println("Pipeline is not running.")
		return nil
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(resp), &data); err != nil {
		fmt.Println(resp)
		return nil
	}

	phase, _ := data["phase"].(string)
	localOnly, _ := data["local_only"].(bool)
	tunnel, _ := data["tunnel"].(string)
	pid, _ := data["ssh_pid"].(float64)

	fmt.Printf("phase:   %s\n", phase)
	if localOnly {
		fmt.Printf("mode:    local\n")
	} else if pid > 0 {
		fmt.Printf("tunnel:  %s (ssh pid %d)\n", tunnel, int(pid))
	} else {
		fmt.Printf("tunnel:  %s\n", tunnel)
	}
	return nil
}

func pipelineSteps(sessionID string) error {
	resp, err := sendClientCommand("status")
	if err != nil {
		fmt.Println("Pipeline is not running.")
		return nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(resp), &data); err != nil {
		return err
	}

	rows := fetchSessions(data)
	var running []sessionRow
	for _, r := range rows {
		if r.status == "running" {
			running = append(running, r)
		}
	}

	var targetID string
	if sessionID != "" {
		var match *sessionRow
		for i, r := range rows {
			if r.id == sessionID || strings.HasPrefix(r.id, sessionID) {
				match = &rows[i]
				break
			}
		}
		if match == nil {
			return fmt.Errorf("session %s not found", sessionID)
		}
		if match.status != "running" {
			return fmt.Errorf("session %s is not running (status: %s)", match.id, match.status)
		}
		targetID = match.id
	} else {
		switch len(running) {
		case 1:
			targetID = running[0].id
		case 0:
			fmt.Println("No running sessions.")
			if len(rows) > 0 {
				printSessionsTable(rows)
			}
			return nil
		default:
			fmt.Println("Multiple running sessions. Specify one:")
			printSessionsTable(running)
			return nil
		}
	}

	return printSessionDetail(data, targetID)
}

func printSessionDetail(data map[string]any, sessionID string) error {
	raw, _ := data["sessions"].([]any)
	var match map[string]any
	for _, s := range raw {
		m, _ := s.(map[string]any)
		id, _ := m["id"].(string)
		if id == sessionID || strings.HasPrefix(id, sessionID) {
			match = m
			break
		}
	}
	if match == nil {
		return fmt.Errorf("session %s not found", sessionID)
	}

	id, _ := match["id"].(string)
	target, _ := match["target"].(string)
	status, _ := match["status"].(string)
	stepCount, _ := match["step_count"].(float64)
	currentStep, _ := match["current_step"].(float64)

	stepPos := "—"
	if stepCount > 0 {
		if currentStep < 0 {
			stepPos = fmt.Sprintf("0/%d", int(stepCount))
		} else {
			stepPos = fmt.Sprintf("%d/%d", int(currentStep)+1, int(stepCount))
		}
	}

	fmt.Printf("Session: %s\n", id)
	fmt.Printf("Target:  %s\n", target)
	fmt.Printf("Status:  %-20s Step: %s\n", status, stepPos)
	fmt.Println()

	works, _ := match["works"].([]any)
	if len(works) == 0 {
		fmt.Println("  (no works recorded)")
		return nil
	}

	fmt.Printf("  %-9s  %-44s  %s\n", "STATUS", "WORK", "TIMING")
	prevStep := -1
	for _, w := range works {
		m, _ := w.(map[string]any)
		pipeline, _ := m["pipeline"].(string)
		stage, _ := m["stage"].(string)
		name, _ := m["name"].(string)
		st, _ := m["status"].(string)
		stepIdx, _ := m["step_idx"].(float64)
		startedAt, _ := m["started_at"].(string)
		endedAt, _ := m["ended_at"].(string)

		if prevStep >= 0 && int(stepIdx) != prevStep {
			fmt.Println("  " + strings.Repeat("-", 70))
		}
		prevStep = int(stepIdx)

		fmt.Printf("  %-9s  %-44s  %s\n", st, pipeline+"."+stage+"."+name, formatTiming(startedAt, endedAt, st))
	}
	return nil
}

func formatTiming(startedAt, endedAt, status string) string {
	if startedAt == "" {
		return ""
	}
	startT, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return ""
	}
	startStr := startT.Local().Format("15:04:05")
	if endedAt == "" {
		if status == "running" {
			elapsed := time.Since(startT).Round(time.Second)
			return fmt.Sprintf("%s → …      (%s)", startStr, elapsed)
		}
		return startStr
	}
	endT, err := time.Parse(time.RFC3339, endedAt)
	if err != nil {
		return startStr
	}
	dur := endT.Sub(startT).Round(time.Second)
	return fmt.Sprintf("%s → %s  (%s)", startStr, endT.Local().Format("15:04:05"), dur)
}

func pipelineBuild() error {
	mf, err := parseMantafile("mantafile.toml")
	if err != nil {
		return err
	}
	for _, p := range mf.Pipelines {
		if err := validateGraph(p); err != nil {
			return err
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	for _, p := range mf.Pipelines {
		p.Root = cwd
	}
	if err := os.MkdirAll(mantaDir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(pipelineFile, data, 0o644); err != nil {
		return err
	}
	if raw, err := os.ReadFile("mantafile.toml"); err == nil {
		hash := fmt.Sprintf("%x", sha256.Sum256(raw))
		os.WriteFile(mantaDir+"/mantafile.hash", []byte(hash), 0o644)
	}
	totalStages, totalWorks := 0, 0
	for _, p := range mf.Pipelines {
		totalStages += len(p.Stages)
		for _, s := range p.Stages {
			totalWorks += len(s.Works)
		}
	}
	fmt.Printf("Pipeline built: %d pipeline(s), %d stages, %d works → %s\n",
		len(mf.Pipelines), totalStages, totalWorks, pipelineFile)
	return nil
}

func pipelineShow() error {
	mf, err := loadMantafile()
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no built pipeline — run 'manta-pipeline build' first")
		}
		return err
	}
	if mf.Cluster != "" {
		fmt.Printf("cluster: %s\n\n", mf.Cluster)
	} else {
		fmt.Printf("cluster: (local)\n\n")
	}
	for i, p := range mf.Pipelines {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("%s\n", p.Name)
		fmt.Printf("  src:     %s\n", p.SrcDir)
		if p.Dependencies != "" {
			fmt.Printf("  deps:    %s\n", p.Dependencies)
		}
		for _, stage := range p.Stages {
			fmt.Printf("\n  [%s]\n", stage.Name)
			for j, work := range stage.Works {
				suffix := ""
				if work.ManualWork {
					suffix = "  (manual)"
				}
				fmt.Printf("    %d  %-10s  %s%s\n", j, work.Name, work.Entrypoint, suffix)
			}
		}
	}
	return nil
}

func pipelineRun(target string, detach, plain bool) error {
	if !isDaemonRunning() {
		return fmt.Errorf("daemon not running — start with 'manta-pipeline up'")
	}
	if stored, err := os.ReadFile(mantaDir + "/mantafile.hash"); err == nil {
		if current, err := os.ReadFile("mantafile.toml"); err == nil {
			if string(stored) != fmt.Sprintf("%x", sha256.Sum256(current)) {
				fmt.Fprintln(os.Stderr, "warning: mantafile.toml has changed since last build — run 'manta-pipeline build'")
				fmt.Fprintln(os.Stderr, "continuing with current build in 5s... (Ctrl+C to abort)")
				time.Sleep(5 * time.Second)
			}
		}
	}
	resp, err := sendClientCommand("run " + target)
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "error:") {
		return fmt.Errorf("%s", strings.TrimPrefix(resp, "error: "))
	}
	parts := strings.SplitN(resp, " ", 2)
	sessionID := parts[0]
	fileStem := parts[0]
	if len(parts) == 2 {
		fileStem = parts[1]
	}
	sessionLogFile := runsDir + "/" + fileStem + ".log"
	apiLogFile := apiLogsDir + "/" + fileStem + ".log"
	doneFile := runsDir + "/" + fileStem + ".done"
	fmt.Printf("Run started — session %s\n", sessionID)
	if detach {
		fmt.Printf("Session log: %s\n", sessionLogFile)
		fmt.Printf("API log:     %s\n", apiLogFile)
		return nil
	}
	if plain {
		tailRunLogUntilDone(sessionLogFile)
		return nil
	}
	tailAPILogUntilDone(apiLogFile, doneFile, sessionID)
	return nil
}

func pipelineProgress(sessionID string) error {
	resp, err := sendClientCommand("status")
	if err != nil {
		fmt.Println("Pipeline is not running.")
		return nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(resp), &data); err != nil {
		return err
	}

	rows := fetchSessions(data)
	var running []sessionRow
	for _, r := range rows {
		if r.status == "running" {
			running = append(running, r)
		}
	}

	var target *sessionRow

	if sessionID != "" {
		for i, r := range rows {
			if r.id == sessionID {
				target = &rows[i]
				break
			}
		}
		if target == nil {
			return fmt.Errorf("session %s not found", sessionID)
		}
		if target.status != "running" {
			return fmt.Errorf("session %s is not running (status: %s)", sessionID, target.status)
		}
	} else if len(running) == 1 {
		target = &running[0]
	} else if len(running) == 0 {
		fmt.Println("No running sessions.")
		if len(rows) > 0 {
			printSessionsTable(rows)
		}
		return nil
	} else {
		fmt.Println("Multiple running sessions — use --session <id>:")
		printSessionsTable(rows)
		return nil
	}

	apiLogFile := apiLogsDir + "/" + target.fileStem + ".log"
	doneFile := runsDir + "/" + target.fileStem + ".done"
	fmt.Printf("Following session %s\n", target.id)
	tailAPILogUntilDone(apiLogFile, doneFile, target.id)
	return nil
}

// tailRunLogUntilDone tails a session log until the .done marker appears.
func tailRunLogUntilDone(logFile string) {
	doneFile := strings.TrimSuffix(logFile, ".log") + ".done"
	cmd := exec.Command("tail", "-n", "20", "-f", logFile)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return
	}
	for {
		if _, err := os.Stat(doneFile); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	cmd.Process.Kill()
	cmd.Wait()
}

// tailLog tails a log file until Ctrl+C.
func tailLog(logFile string) {
	cmd := exec.Command("tail", "-n", "20", "-f", logFile)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}

// tailLogUntilDone tails a log file until the daemon socket disappears.
func tailLogUntilDone(logFile string) {
	cmd := exec.Command("tail", "-n", "20", "-f", logFile)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return
	}
	for isDaemonRunning() {
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	cmd.Process.Kill()
	cmd.Wait()
}

// latestDaemonLog returns the path of the most recently created daemon log.
func latestDaemonLog() (string, error) {
	entries, err := os.ReadDir(daemonLogsDir)
	if err != nil {
		return "", fmt.Errorf("no daemon logs found in %s", daemonLogsDir)
	}
	var logs []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
			logs = append(logs, e.Name())
		}
	}
	if len(logs) == 0 {
		return "", fmt.Errorf("no daemon logs found in %s", daemonLogsDir)
	}
	sort.Strings(logs)
	return filepath.Join(daemonLogsDir, logs[len(logs)-1]), nil
}

type sessionRow struct {
	fileStem    string
	id          string
	target      string
	status      string
	stepCount   int
	currentStep int
}

func fetchSessions(data map[string]any) []sessionRow {
	raw, _ := data["sessions"].([]any)
	rows := make([]sessionRow, 0, len(raw))
	for _, s := range raw {
		m, _ := s.(map[string]any)
		wc, _ := m["step_count"].(float64)
		cw, _ := m["current_step"].(float64)
		rows = append(rows, sessionRow{
			fileStem:    m["file_stem"].(string),
			id:          m["id"].(string),
			target:      m["target"].(string),
			status:      m["status"].(string),
			stepCount:   int(wc),
			currentStep: int(cw),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].fileStem > rows[j].fileStem
	})
	return rows
}

func printSessionsTable(rows []sessionRow) {
	fmt.Printf("%-9s %-24s %-10s %-6s %s\n", "STATUS", "ID", "STARTED", "STEP", "TARGET")
	for _, r := range rows {
		started := ""
		if len(r.fileStem) >= 15 {
			if t, err := time.Parse("20060102-150405", r.fileStem[:15]); err == nil {
				started = t.Format("15:04:05")
			}
		}
		step := "—"
		if r.stepCount > 0 {
			cw := r.currentStep + 1
			if cw < 1 {
				cw = 0
			}
			step = fmt.Sprintf("%d/%d", cw, r.stepCount)
		}
		fmt.Printf("%-9s %-24s %-10s %-6s %s\n", r.status, r.id, started, step, r.target)
	}
}

func pipelineStop(sessionID string, all bool) error {
	resp, err := sendClientCommand("status")
	if err != nil {
		fmt.Println("Pipeline is not running.")
		return nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(resp), &data); err != nil {
		return err
	}

	rows := fetchSessions(data)
	var running []sessionRow
	for _, r := range rows {
		if r.status == "running" {
			running = append(running, r)
		}
	}

	if all {
		resp, err := sendClientCommand("stopall")
		if err != nil {
			return err
		}
		n := strings.TrimSpace(resp)
		if n == "0" {
			fmt.Println("No running sessions.")
		} else {
			fmt.Printf("Stopped %s session(s).\n", n)
		}
		return nil
	}

	if sessionID != "" {
		resp, err := sendClientCommand("stop " + sessionID)
		if err != nil {
			return err
		}
		if strings.HasPrefix(resp, "error:") {
			return fmt.Errorf("%s", strings.TrimPrefix(resp, "error: "))
		}
		fmt.Printf("Stopped session %s.\n", sessionID)
		return nil
	}

	if len(running) == 0 {
		fmt.Println("No running sessions.")
		if len(rows) > 0 {
			printSessionsTable(rows)
		}
		return nil
	}

	fmt.Println("Use --session <id> or --all:")
	printSessionsTable(rows)
	return nil
}

func pipelineSessions() error {
	resp, err := sendClientCommand("status")
	if err != nil {
		fmt.Println("Pipeline is not running.")
		return nil
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(resp), &data); err != nil {
		return err
	}

	rows := fetchSessions(data)
	if len(rows) > 10 {
		rows = rows[:10]
	}
	if len(rows) == 0 {
		fmt.Println("No sessions.")
		return nil
	}
	printSessionsTable(rows)
	return nil
}

func pipelineTeardown() error {
	kill := func(pattern string) error {
		cmd := exec.Command("pkill", "-TERM", "-f", pattern)
		if err := cmd.Run(); err != nil {
			if e, ok := err.(*exec.ExitError); ok && e.ExitCode() == 1 {
				return nil
			}
			return err
		}
		return nil
	}

	step := func(label string, fn func() error) {
		fmt.Printf("%-44s", label)
		if err := fn(); err != nil {
			fmt.Printf("warning: %v\n", err)
		} else {
			fmt.Println("ok")
		}
	}

	step("killing daemon...", func() error { return kill("manta-daemon-run") })
	step("killing manta-ssh...", func() error { return kill("manta-ssh") })
	step("removing socket...", func() error { return os.Remove(daemonSocketPath) })

	fmt.Println("ray stop (local)...")
	if err := streamCommand("uv", "run", "ray", "stop"); err != nil {
		fmt.Printf("  warning: %v\n", err)
	}

	mf, err := loadMantafile()
	if err == nil && mf.Cluster != "" {
		fmt.Printf("ray down %s...\n", mf.Cluster)
		if err := streamCommand("uv", "run", "ray", "down", "-y", mf.Cluster); err != nil {
			fmt.Printf("  warning: %v\n", err)
		}
	}

	step("cleaning /tmp/manta-broker-*...", func() error {
		matches, _ := filepath.Glob("/tmp/manta-broker-*")
		for _, m := range matches {
			os.RemoveAll(m)
		}
		return nil
	})

	fmt.Println("teardown complete.")
	return nil
}

func streamCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)

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
		fmt.Println(scanner.Text())
	}
	pr.Close()

	return cmd.Wait()
}
