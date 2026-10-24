// lab-probe is a bounded, source-built Linux workload; it is never run as a host stressor.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func emit(value any)  { _ = json.NewEncoder(os.Stdout).Encode(value) }
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }

func main() {
	runtime.GOMAXPROCS(1)
	if len(os.Args) < 2 {
		fatal(errors.New("usage: lab-probe noop|echo|fs|serve|network|memory|processes|sleep|tree|flood"))
	}
	switch os.Args[1] {
	case "noop":
		emit(map[string]bool{"ready": true})
	case "echo":
		fmt.Fprintln(os.Stdout, "stdout-marker")
		fmt.Fprintln(os.Stderr, "stderr-marker")
		code := 0
		if len(os.Args) > 2 {
			var err error
			code, err = strconv.Atoi(os.Args[2])
			if err != nil {
				fatal(err)
			}
		}
		os.Exit(code)
	case "fs":
		if len(os.Args) != 3 {
			fatal(errors.New("fs requires host canary path"))
		}
		if os.Getuid() == 0 {
			fatal(errors.New("unexpected root UID"))
		}
		for _, path := range []string{os.Args[2], "/var/run/docker.sock", "/run/docker.sock"} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				fatal(fmt.Errorf("host path visible or unexpected error: %s: %v", path, err))
			}
		}
		if err := os.WriteFile("/root-write-test", []byte("unexpected"), 0600); err == nil {
			fatal(errors.New("root filesystem writable"))
		}
		if err := os.WriteFile("/tmp/ephemeral-marker", []byte("temporary"), 0600); err != nil {
			fatal(err)
		}
		status, err := os.ReadFile("/proc/self/status")
		if err != nil {
			fatal(err)
		}
		if !strings.Contains(string(status), "CapEff:\t0000000000000000") {
			fatal(errors.New("effective capabilities are not empty"))
		}
		emit(map[string]any{"host_canary_hidden": true, "root_write_denied": true, "tmp_writable": true, "uid": os.Getuid(), "capabilities_empty": true})
	case "serve":
		listener, err := net.Listen("tcp4", ":8080")
		if err != nil {
			fatal(err)
		}
		defer listener.Close()
		emit(map[string]bool{"listening": true})
		for {
			conn, err := listener.Accept()
			if err != nil {
				fatal(err)
			}
			_, _ = conn.Write([]byte("lab-peer\n"))
			_ = conn.Close()
		}
	case "network":
		if len(os.Args) != 4 {
			fatal(errors.New("network requires endpoint and allow|deny"))
		}
		expectAllow := os.Args[3] == "allow"
		if !expectAllow && os.Args[3] != "deny" {
			fatal(errors.New("invalid expectation"))
		}
		connected := false
		var last error
		for i := 0; i < 20; i++ {
			conn, err := net.DialTimeout("tcp4", os.Args[2], 150*time.Millisecond)
			last = err
			if err == nil {
				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				buf := make([]byte, 32)
				n, readErr := conn.Read(buf)
				_ = conn.Close()
				connected = readErr == nil && strings.TrimSpace(string(buf[:n])) == "lab-peer"
				if connected {
					break
				}
			}
			if !expectAllow {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		emit(map[string]any{"connected": connected, "expected_allow": expectAllow, "last_error": fmt.Sprint(last)})
		if connected != expectAllow {
			os.Exit(1)
		}
	case "memory":
		// Hold a baseline long enough for an optional sample, then touch 256 MiB.
		time.Sleep(2 * time.Second)
		blocks := make([][]byte, 0, 256)
		for i := 0; i < 256; i++ {
			b := make([]byte, 1<<20)
			for j := 0; j < len(b); j += 4096 {
				b[j] = 1
			}
			blocks = append(blocks, b)
			time.Sleep(5 * time.Millisecond)
		}
		runtime.KeepAlive(blocks)
		emit(map[string]bool{"unexpected_allocation_success": true})
	case "processes":
		if len(os.Args) != 4 || (os.Args[3] != "allow" && os.Args[3] != "reject") {
			fatal(errors.New("processes requires count and allow|reject"))
		}
		bound, err := strconv.Atoi(os.Args[2])
		if err != nil || bound < 1 || bound > 96 {
			fatal(errors.New("process count must be 1..96"))
		}
		children := make([]*exec.Cmd, 0, bound)
		defer func() {
			for _, child := range children {
				_ = child.Process.Kill()
			}
			for _, child := range children {
				_ = child.Wait()
			}
		}()
		rejected := false
		diagnostic := ""
		for i := 0; i < bound; i++ {
			child := exec.Command("/lab-probe", "sleep")
			child.Env = os.Environ()
			child.Stdout, child.Stderr = nil, nil
			if err := child.Start(); err != nil {
				rejected = errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.ENOMEM)
				diagnostic = err.Error()
				break
			}
			children = append(children, child)
			time.Sleep(20 * time.Millisecond)
		}
		emit(map[string]any{"spawned": len(children), "attempt_bound": bound, "creation_rejected": rejected, "error": diagnostic, "limit_unit": "host tasks, including threads"})
		if rejected != (os.Args[3] == "reject") {
			os.Exit(1)
		}
	case "sleep":
		time.Sleep(30 * time.Second)
	case "tree":
		if err := os.WriteFile("/tmp/termination-marker", []byte("ephemeral"), 0600); err != nil {
			fatal(err)
		}
		child := exec.Command("/lab-probe", "sleep")
		if err := child.Start(); err != nil {
			fatal(err)
		}
		emit(map[string]any{"child_pid": child.Process.Pid, "marker_written": true})
		time.Sleep(30 * time.Second)
	case "clean":
		if _, err := os.Stat("/tmp/termination-marker"); !errors.Is(err, os.ErrNotExist) {
			fatal(errors.New("stale termination marker or unexpected filesystem error"))
		}
		emit(map[string]bool{"fresh_tmp": true})
	case "flood":
		block := strings.Repeat("x", 1024)
		for i := 0; i < 1200; i++ {
			fmt.Fprint(os.Stdout, block)
			fmt.Fprint(os.Stderr, block)
		}
	default:
		fatal(errors.New("unknown probe mode"))
	}
}
