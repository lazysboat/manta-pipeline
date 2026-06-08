package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

var tunnelPorts = []string{"8265", "5000", "5001", "10001"}

func runTunnelLoop(ctx context.Context, state *daemonStatus, cfg *ClusterConfig) {
	args := []string{
		"-N", "-T",
		"-i", cfg.Auth.SSHPrivateKey,
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=10",
		"-o", "ServerAliveCountMax=3",
		"-o", "LogLevel=ERROR",
	}
	for _, port := range tunnelPorts {
		args = append(args, "-L", port+":localhost:"+port)
	}
	args = append(args, cfg.Auth.SSHUser+"@"+cfg.Provider.HeadIP)

	log.Printf("tunnel: connecting to %s@%s ports %s", cfg.Auth.SSHUser, cfg.Provider.HeadIP, strings.Join(tunnelPorts, ","))

	for {
		state.setTunnel("connecting", 0)
		cmd := exec.CommandContext(ctx, "ssh", args...)
		cmd.Args[0] = "manta-ssh"
		cmd.Stdin = nil
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			if ctx.Err() != nil {
				state.setTunnel("stopped", 0)
				return
			}
			log.Printf("tunnel: failed to start ssh: %v, retrying in 5s", err)
		} else {
			state.setTunnel("up", cmd.Process.Pid)
			if err := cmd.Wait(); err != nil {
				if ctx.Err() != nil {
					state.setTunnel("stopped", 0)
					log.Printf("tunnel: shutting down")
					return
				}
				log.Printf("tunnel: ssh exited: %v, reconnecting in 5s", err)
			} else {
				log.Printf("tunnel: ssh exited cleanly, reconnecting in 5s")
			}
		}

		state.setTunnel("reconnecting", 0)
		select {
		case <-ctx.Done():
			state.setTunnel("stopped", 0)
			log.Printf("tunnel: shutting down")
			return
		case <-time.After(5 * time.Second):
		}
	}
}
