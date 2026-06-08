package main

import (
	"fmt"
	"os"
	"strings"
)

// version is the release version, shared by the CLI binary and (in lockstep)
// the mantapipeline wheel. Overridable at build time via:
//   go build -ldflags "-X main.version=<v>"
var version = "0.1.0.dev1"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "version", "--version", "-v":
		fmt.Printf("manta-pipeline %s\n", version)
	case "manta-daemon-run":
		runDaemon()
	case "up":
		if err := pipelineUp(hasFlag("--detach"), hasFlag("--plain")); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "down":
		if err := pipelineDown(hasFlag("--detach"), hasFlag("--plain")); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "connect":
		if err := pipelineConnect(hasFlag("--plain")); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "disconnect":
		if err := pipelineDisconnect(hasFlag("--detach"), hasFlag("--plain")); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "status":
		if err := pipelineStatus(); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "steps":
		if err := pipelineSteps(positionalArg()); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "log":
		logPath, err := latestDaemonLog()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		tailLog(logPath)
	case "run":
		if len(os.Args) < 3 {
			fmt.Fprintf(os.Stderr, "error: missing target\nUsage: manta-pipeline run <pipeline>[.<stage>[.<work>]] [--detach|--plain]\n")
			os.Exit(1)
		}
		if err := pipelineRun(os.Args[2], hasFlag("--detach"), hasFlag("--plain")); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "progress":
		if err := pipelineProgress(flagValue("--session")); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "build":
		if err := pipelineBuild(); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "show":
		if err := pipelineShow(); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "sessions":
		if err := pipelineSessions(); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "stop":
		if err := pipelineStop(flagValue("--session"), hasFlag("--all")); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "teardown":
		if err := pipelineTeardown(); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func positionalArg() string {
	for _, arg := range os.Args[2:] {
		if !strings.HasPrefix(arg, "--") {
			return arg
		}
	}
	return ""
}

func hasFlag(flag string) bool {
	for _, arg := range os.Args[2:] {
		if arg == flag {
			return true
		}
	}
	return false
}

func flagValue(flag string) string {
	args := os.Args[2:]
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  manta-pipeline version")
	fmt.Println("  manta-pipeline up [--detach] [--plain]")
	fmt.Println("  manta-pipeline down [--detach] [--plain]")
	fmt.Println("  manta-pipeline connect [--plain]")
	fmt.Println("  manta-pipeline disconnect [--detach] [--plain]")
	fmt.Println("  manta-pipeline status")
	fmt.Println("  manta-pipeline steps [<session-id>]")
	fmt.Println("  manta-pipeline log")
	fmt.Println("  manta-pipeline build")
	fmt.Println("  manta-pipeline show")
	fmt.Println("  manta-pipeline run <pipeline>[.<stage>[.<work>]] [--detach|--plain]")
	fmt.Println("  manta-pipeline progress [--session <id>]")
	fmt.Println("  manta-pipeline sessions")
	fmt.Println("  manta-pipeline stop [--session <id>] [--all]")
	fmt.Println("  manta-pipeline teardown")
}
