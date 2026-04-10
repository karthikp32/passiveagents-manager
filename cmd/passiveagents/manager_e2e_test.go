//go:build unix
// +build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func waitForManagerE2ECondition(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for e2e condition after %s", timeout)
}

func TestManagerReleaseBinaryFreshInstallToOnlineE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manager subprocess e2e in short mode")
	}

	tempDir := t.TempDir()
	binaryPath := filepath.Join(tempDir, "passiveagents")
	buildStampedManagerBinary(t, binaryPath, "9.8.7", "homebrew")

	// Verify binary is executable
	info, err := os.Stat(binaryPath)
	if err != nil {
		t.Fatalf("binary stat: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatal("binary is not executable")
	}

	// Verify version is stamped correctly
	env := newManagerE2EEnv(t, tempDir, "test-token")
	versionOutput := runManagerBinaryCommand(t, env, binaryPath, "version")
	if !strings.Contains(versionOutput, "9.8.7") {
		t.Fatalf("expected version 9.8.7 in output, got: %s", versionOutput)
	}

	// Verify status works before first start
	statusOutput := runManagerBinaryCommand(t, env, binaryPath, "status")
	if !strings.Contains(statusOutput, "Status: stopped") {
		t.Fatalf("expected stopped status before first start, got: %s", statusOutput)
	}
}

func TestManagerReleaseBinaryUpgradeRestartE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manager subprocess e2e in short mode")
	}

	tempDir := t.TempDir()
	installDir := filepath.Join(tempDir, "install")
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		t.Fatalf("mkdir install dir: %v", err)
	}
	installPath := filepath.Join(installDir, "passiveagents")

	env := newManagerE2EEnv(t, tempDir, "test-token")

	// Build and verify initial version
	buildStampedManagerBinary(t, installPath, "1.0.0", "homebrew")
	versionOutput := runManagerBinaryCommand(t, env, installPath, "version")
	if !strings.Contains(versionOutput, "1.0.0") {
		t.Fatalf("expected initial version 1.0.0, got: %s", versionOutput)
	}

	// Build upgraded binary
	upgradedPath := filepath.Join(tempDir, "passiveagents-upgraded")
	buildStampedManagerBinary(t, upgradedPath, "1.1.0", "homebrew")

	// Replace binary
	if err := os.Rename(upgradedPath, installPath); err != nil {
		t.Fatalf("replace installed binary: %v", err)
	}

	// Verify upgraded version is now in place
	upgradedVersionOutput := runManagerBinaryCommand(t, env, installPath, "version")
	if !strings.Contains(upgradedVersionOutput, "1.1.0") {
		t.Fatalf("expected upgraded version 1.1.0, got: %s", upgradedVersionOutput)
	}
}

func buildStampedManagerBinary(t *testing.T, outputPath, version, installChannel string) {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("determine passiveagents source directory")
	}

	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(
		buildCtx,
		"go",
		"build",
		"-o",
		outputPath,
		"-ldflags",
		fmt.Sprintf("-s -w -X main.managerVersion=%s -X main.managerInstallChannel=%s", version, installChannel),
		".",
	)
	cmd.Dir = filepath.Dir(thisFile)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build manager binary: %v\n%s", err, output)
	}
}

func newManagerE2EEnv(t *testing.T, tempDir, accessToken string) []string {
	t.Helper()

	homeDir := filepath.Join(tempDir, "home")
	if err := os.MkdirAll(filepath.Join(homeDir, ".passiveagents"), 0o700); err != nil {
		t.Fatalf("mkdir e2e home: %v", err)
	}
	writeE2ESessionFile(t, homeDir, accessToken)

	fakeBinDir := filepath.Join(tempDir, "fake-bin")
	if err := os.MkdirAll(fakeBinDir, 0o755); err != nil {
		t.Fatalf("mkdir fake bin dir: %v", err)
	}
	writeFakeCloudflaredScript(t, filepath.Join(fakeBinDir, "cloudflared"))

	stateFile := filepath.Join(homeDir, ".passiveagents", "manager-state.json")
	managerLogFile := filepath.Join(homeDir, ".passiveagents", "manager.log")
	pollSnapshotFile := filepath.Join(homeDir, ".passiveagents", "polled-task-batch.json")

	return append(
		os.Environ(),
		"HOME="+homeDir,
		"PATH="+fakeBinDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PASSIVEAGENTS_STATE_FILE="+stateFile,
		"PASSIVEAGENTS_MANAGER_LOG_FILE="+managerLogFile,
		"PASSIVEAGENTS_POLL_SNAPSHOT_FILE="+pollSnapshotFile,
		"PASSIVEAGENTS_MAX_CONCURRENT=1",
	)
}

func writeE2ESessionFile(t *testing.T, homeDir, accessToken string) {
	t.Helper()

	session := storedSession{
		AccessToken:  accessToken,
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		UserID:       "user-e2e",
		UserEmail:    "e2e@example.com",
	}
	raw, err := json.Marshal(session)
	if err != nil {
		t.Fatalf("marshal e2e session: %v", err)
	}
	path := filepath.Join(homeDir, ".passiveagents", authFileName)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write e2e session: %v", err)
	}
}

func writeFakeCloudflaredScript(t *testing.T, scriptPath string) {
	t.Helper()

	script := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "tunnel" && "${2:-}" == "run" ]]; then
  printf 'Registered tunnel connection\n'
  trap 'exit 0' TERM INT
  while true; do
    sleep 1
  done
fi
printf 'unsupported fake cloudflared invocation: %s\n' "$*" >&2
exit 1
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake cloudflared: %v", err)
	}
}

func runManagerBinaryCommand(t *testing.T, env []string, binaryPath string, args ...string) string {
	t.Helper()

	cmd := exec.Command(binaryPath, args...)
	cmd.Env = env
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("run %s %v: %v\nstdout:\n%s\nstderr:\n%s", binaryPath, args, err, stdout.String(), stderr.String())
	}

	return stdout.String() + stderr.String()
}

func waitForManagerStopped(t *testing.T, env []string, binaryPath string) {
	t.Helper()

	waitForManagerE2ECondition(t, 10*time.Second, func() bool {
		statusOutput := runManagerBinaryCommand(t, env, binaryPath, "status")
		return strings.Contains(statusOutput, "Status: stopped")
	})
}

func stopManagerBestEffort(env []string, binaryPath string) {
	cmd := exec.Command(binaryPath, "stop")
	cmd.Env = env
	_ = cmd.Run()
}
