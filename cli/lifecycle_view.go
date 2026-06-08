package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type phaseRow struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at"`
	Error     string `json:"error"`
}

type statusSnapshot struct {
	Phase string     `json:"phase"`
	Phases []phaseRow  `json:"phases"`
}

type lifecycleFlow int

const (
	flowUp lifecycleFlow = iota
	flowDown
)

type lifecycleView struct {
	lastDrawn int
}

func (v *lifecycleView) redraw(phases []phaseRow) {
	if v.lastDrawn > 0 {
		fmt.Print("\033[" + strconv.Itoa(v.lastDrawn) + "A\033[J")
	}
	for _, s := range phases {
		fmt.Println(formatPhaseRow(s))
	}
	v.lastDrawn = len(phases)
}

func formatPhaseRow(s phaseRow) string {
	timing := ""
	switch s.Status {
	case "running":
		if t, err := time.Parse(time.RFC3339, s.StartedAt); err == nil {
			timing = time.Since(t).Round(time.Second).String()
		}
	case "done", "failed":
		startT, err1 := time.Parse(time.RFC3339, s.StartedAt)
		endT, err2 := time.Parse(time.RFC3339, s.EndedAt)
		if err1 == nil && err2 == nil {
			timing = endT.Sub(startT).Round(time.Second).String()
		}
	}
	row := fmt.Sprintf("  %-9s  %-44s  %s", s.Status, truncate(s.Name, 44), timing)
	if s.Status == "failed" && s.Error != "" {
		row += "  — " + truncate(s.Error, 80)
	}
	return row
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

// runLifecycle polls the daemon's status and renders a checklist until the
// flow reaches a terminal state.
//
// Terminal conditions:
//   - flowUp:   all phases done AND phase == "running"  → print "Pipeline ready."
//   - flowDown: socket dial fails (daemon exited)      → print "Pipeline stopped."
//   - any:      a phase is "failed"                     → print failure + return error
//   - SIGINT during the wait                           → print "Detached" + return nil
//   - timeout                                          → return timeout error
func runLifecycle(flow lifecycleFlow, timeout time.Duration, logPath string) error {
	view := &lifecycleView{}
	deadline := time.Now().Add(timeout)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastGood statusSnapshot
	var haveLastGood bool

	for {
		snap, dialErr := fetchStatus()

		if dialErr != nil {
			if flow == flowDown {
				// Daemon is gone. Mark any pending/running phase as done from our last snapshot.
				if haveLastGood {
					for i := range lastGood.Phases {
						if lastGood.Phases[i].Status == "pending" || lastGood.Phases[i].Status == "running" {
							lastGood.Phases[i].Status = "done"
							if lastGood.Phases[i].EndedAt == "" {
								lastGood.Phases[i].EndedAt = time.Now().UTC().Format(time.RFC3339)
							}
						}
					}
					view.redraw(lastGood.Phases)
					for _, s := range lastGood.Phases {
						if s.Status == "failed" {
							fmt.Printf("Failed at phase %q: %s\nSee %s for details.\n", s.Name, s.Error, logPath)
							return fmt.Errorf("shutdown failed at %s", s.Name)
						}
					}
				}
				fmt.Println("Pipeline stopped.")
				return nil
			}
			return fmt.Errorf("daemon exited during startup — check %s", logPath)
		}

		view.redraw(snap.Phases)
		lastGood = snap
		haveLastGood = true

		// For up flow, bail as soon as a phase fails — later phases won't run.
		// For down flow, keep watching so ray down (or ray stop) can complete;
		// the final tally is reported when the socket closes.
		if flow == flowUp {
			for _, s := range snap.Phases {
				if s.Status == "failed" {
					fmt.Printf("Failed at phase %q: %s\nSee %s for details.\n", s.Name, s.Error, logPath)
					return fmt.Errorf("%s failed: %s", s.Name, s.Error)
				}
			}
			if snap.Phase == "running" && allDone(snap.Phases) && len(snap.Phases) > 0 {
				fmt.Println("Pipeline ready.")
				return nil
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s — check %s", timeout, logPath)
		}

		select {
		case <-ticker.C:
		case <-sigCh:
			fmt.Println("Detached — daemon still running. Use 'manta-pipeline status' to check progress.")
			return nil
		}
	}
}

func fetchStatus() (statusSnapshot, error) {
	var snap statusSnapshot
	resp, err := sendClientCommand("status")
	if err != nil {
		return snap, err
	}
	if strings.HasPrefix(resp, "error:") {
		return snap, fmt.Errorf("%s", strings.TrimPrefix(resp, "error: "))
	}
	if err := json.Unmarshal([]byte(resp), &snap); err != nil {
		return snap, err
	}
	return snap, nil
}

func allDone(phases []phaseRow) bool {
	for _, s := range phases {
		if s.Status != "done" {
			return false
		}
	}
	return true
}
