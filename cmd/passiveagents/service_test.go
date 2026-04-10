//go:build unix
// +build unix

package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestBuildManagerServiceArtifactsLinux(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	serviceCurrentGOOS = func() string { return "linux" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) { return "/usr/local/bin/passiveagents", nil }
	serviceLookupPath = func(name string) (string, error) { return "/usr/bin/cloudflared", nil }
	servicePathEnv = func() string { return "/usr/local/bin:/usr/bin" }

	artifacts, err := buildManagerServiceArtifacts(config{
		WebBaseURL:        "https://passiveagents.com",
		APIBaseURL:        "https://api.passiveagents.com",
		ManagerLogFile:    filepath.Join(tempHome, ".passiveagents", "manager.log"),
		CloudflaredBinary: "",
	})
	if err != nil {
		t.Fatalf("build artifacts: %v", err)
	}

	if artifacts.supervisor != "systemd --user" {
		t.Fatalf("expected systemd supervisor, got %q", artifacts.supervisor)
	}
	if !strings.Contains(artifacts.launcherContents, "PASSIVEAGENTS_CLOUDFLARED_PATH") {
		t.Fatalf("launcher missing cloudflared env: %s", artifacts.launcherContents)
	}
	if !strings.Contains(artifacts.launcherContents, "run-manager --web-url") {
		t.Fatalf("launcher missing run-manager command: %s", artifacts.launcherContents)
	}
	if !strings.Contains(artifacts.serviceContents, "Restart=always") {
		t.Fatalf("unit missing restart policy: %s", artifacts.serviceContents)
	}
	if len(artifacts.installCommands) != 3 {
		t.Fatalf("expected 3 install commands, got %d", len(artifacts.installCommands))
	}
}

func TestBuildManagerServiceArtifactsDarwin(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	serviceCurrentGOOS = func() string { return "darwin" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) { return "/opt/homebrew/bin/passiveagents", nil }
	serviceLookupPath = func(name string) (string, error) { return "/opt/homebrew/bin/cloudflared", nil }
	servicePathEnv = func() string { return "/opt/homebrew/bin:/usr/bin" }
	serviceCurrentUser = func() (*user.User, error) {
		return &user.User{Uid: "501", Username: "karthik"}, nil
	}

	artifacts, err := buildManagerServiceArtifacts(config{
		WebBaseURL:     "https://passiveagents.com",
		APIBaseURL:     "https://api.passiveagents.com",
		ManagerLogFile: filepath.Join(tempHome, ".passiveagents", "manager.log"),
	})
	if err != nil {
		t.Fatalf("build artifacts: %v", err)
	}

	if !strings.Contains(artifacts.serviceContents, managerLaunchdLabel) {
		t.Fatalf("plist missing label: %s", artifacts.serviceContents)
	}
	if !strings.Contains(artifacts.serviceContents, "/bin/bash") {
		t.Fatalf("plist missing launcher shell: %s", artifacts.serviceContents)
	}
	if len(artifacts.installCommands) != 3 {
		t.Fatalf("expected launchctl install commands, got %d", len(artifacts.installCommands))
	}
	if artifacts.installCommands[1].name != "launchctl" || artifacts.installCommands[1].args[0] != "bootstrap" {
		t.Fatalf("unexpected launchctl bootstrap command: %#v", artifacts.installCommands[1])
	}
	if got, want := artifacts.launchdTargets, []string{"gui/501", "user/501"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected launchd targets: got %v want %v", got, want)
	}
}

func TestBuildManagerServiceArtifactsWindows(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	serviceCurrentGOOS = func() string { return "windows" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) { return `C:\PassiveAgents\passiveagents.exe`, nil }
	serviceLookupPath = func(name string) (string, error) { return `C:\Program Files\Cloudflare\cloudflared.exe`, nil }
	servicePathEnv = func() string { return `C:\PassiveAgents;C:\Windows\System32` }
	serviceCurrentUser = func() (*user.User, error) {
		return &user.User{Username: `DESKTOP\karthik`}, nil
	}

	artifacts, err := buildManagerServiceArtifacts(config{
		WebBaseURL:     "https://passiveagents.com",
		APIBaseURL:     "https://api.passiveagents.com",
		ManagerLogFile: filepath.Join(tempHome, ".passiveagents", "manager.log"),
	})
	if err != nil {
		t.Fatalf("build artifacts: %v", err)
	}

	if !strings.Contains(artifacts.launcherContents, "$env:PASSIVEAGENTS_CLOUDFLARED_PATH") {
		t.Fatalf("launcher missing cloudflared env: %s", artifacts.launcherContents)
	}
	if !strings.Contains(artifacts.serviceContents, "Power-Troubleshooter") {
		t.Fatalf("task XML missing resume trigger: %s", artifacts.serviceContents)
	}
	if artifacts.installCommands[0].name != "schtasks" {
		t.Fatalf("unexpected install command: %#v", artifacts.installCommands[0])
	}
}

func TestInstalledManagerSupervisorUsesServicePath(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	serviceCurrentGOOS = func() string { return "linux" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }

	servicePath, _, err := managerServicePath()
	if err != nil {
		t.Fatalf("service path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(servicePath), 0o700); err != nil {
		t.Fatalf("mkdir service dir: %v", err)
	}
	if err := os.WriteFile(servicePath, []byte("[Unit]\n"), 0o600); err != nil {
		t.Fatalf("write service file: %v", err)
	}

	if got := installedManagerSupervisor(config{}); got != "systemd --user" {
		t.Fatalf("expected installed systemd supervisor, got %q", got)
	}
}

func TestBuildManagerServiceArtifactsRejectsTemporaryGoRunBinary(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	tempRoot := filepath.Join(tempHome, "tmp")
	serviceCurrentGOOS = func() string { return "linux" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) {
		return filepath.Join(tempRoot, "go-build1234", "b001", "exe", "passiveagents"), nil
	}
	serviceLookupPath = func(name string) (string, error) { return "/usr/bin/cloudflared", nil }
	t.Setenv("TMPDIR", tempRoot)

	_, err := buildManagerServiceArtifacts(config{
		WebBaseURL:     "https://passiveagents.com",
		APIBaseURL:     "https://api.passiveagents.com",
		ManagerLogFile: filepath.Join(tempHome, ".passiveagents", "manager.log"),
	})
	if err == nil {
		t.Fatal("expected go run service executable to be rejected")
	}
	if !strings.Contains(err.Error(), "temporary go run binary") {
		t.Fatalf("expected temporary binary error, got %v", err)
	}
	if !strings.Contains(err.Error(), "go build -o ~/.local/bin/passiveagents") {
		t.Fatalf("expected local build guidance, got %v", err)
	}
}

func TestStartManagerProcessReturnsBeforePersistingStateWhenManagerAlreadyRunning(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	serviceReadManagerPIDFromState = func(string) (int, error) { return 4321, nil }
	serviceIsProcessRunning = func(pid int) bool { return pid == 4321 }
	err := startManagerProcess(config{
		StateFile:  filepath.Join(t.TempDir(), "state.json"),
		WebBaseURL: "https://passiveagents.com",
		APIBaseURL: "https://api.passiveagents.com",
	})
	if err == nil {
		t.Fatal("expected already-running error")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Fatalf("expected already-running error, got %v", err)
	}
}

func TestStartManagerProcessDoesNotPersistStateWhenServiceArtifactsFail(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	tempRoot := filepath.Join(tempHome, "tmp")
	serviceCurrentGOOS = func() string { return "linux" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) {
		return filepath.Join(tempRoot, "go-build1234", "b001", "exe", "passiveagents"), nil
	}
	serviceLookupPath = func(name string) (string, error) { return "/usr/bin/cloudflared", nil }
	serviceReadManagerPIDFromState = func(string) (int, error) { return 0, os.ErrNotExist }
	t.Setenv("TMPDIR", tempRoot)

	err := startManagerProcess(config{
		StateFile:       filepath.Join(tempHome, "state.json"),
		WebBaseURL:      "https://passiveagents.com",
		APIBaseURL:      "https://api.passiveagents.com",
		ManagerLogFile:  filepath.Join(tempHome, ".passiveagents", "manager.log"),
		LessonsBaseDir:  filepath.Join(tempHome, ".passiveagents", "lessons"),
		SupabaseURL:     "https://supabase.passiveagents.com",
		SupabaseAnonKey: "anon",
	})
	if err == nil {
		t.Fatal("expected service artifact build to fail")
	}
	if !strings.Contains(err.Error(), "temporary go run binary") {
		t.Fatalf("expected temporary binary error, got %v", err)
	}
	if !strings.Contains(err.Error(), "go build -o ~/.local/bin/passiveagents") {
		t.Fatalf("expected local build guidance, got %v", err)
	}
}

func TestValidateTemporaryGoRunBinaryErrorUsesHomebrewPathOnDarwin(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	serviceCurrentGOOS = func() string { return "darwin" }
	servicePathEnv = func() string { return "/opt/homebrew/bin:/usr/bin" }

	err := temporaryGoRunBinaryError("/tmp/go-build1234/b001/exe/passiveagents")
	if err == nil {
		t.Fatal("expected temporary binary error")
	}
	if !strings.Contains(err.Error(), "/opt/homebrew/bin/passiveagents") {
		t.Fatalf("expected darwin Homebrew path guidance, got %v", err)
	}
}

func TestStopManagerProcessKillsManagedPID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sleep helper process setup is unix-specific")
	}

	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	statePath := filepath.Join(tempHome, "state.json")
	serviceCurrentGOOS = func() string { return "linux" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) { return "/usr/local/bin/passiveagents", nil }
	serviceLookupPath = func(name string) (string, error) { return "/usr/bin/cloudflared", nil }
	runServiceCommand = func(name string, args ...string) error { return nil }

	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_, _ = cmd.Process.Wait()
	})

	if err := writeState(statePath, persistedState{
		ManagerPID: cmd.Process.Pid,
		Instances:  map[string]persistedWorker{},
	}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	if err := stopManagerProcess(config{
		StateFile:       statePath,
		WebBaseURL:      "https://passiveagents.com",
		APIBaseURL:      "https://api.passiveagents.com",
		ManagerLogFile:  filepath.Join(tempHome, ".passiveagents", "manager.log"),
		LessonsBaseDir:  filepath.Join(tempHome, ".passiveagents", "lessons"),
		SupabaseURL:     "https://supabase.passiveagents.com",
		SupabaseAnonKey: "anon",
	}); err != nil {
		t.Fatalf("stop manager process: %v", err)
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case <-time.After(3 * time.Second):
		t.Fatal("expected helper process to exit after stopManagerProcess")
	case <-waitDone:
	}
}

func TestStopManagerProcessDoesNotRequireInstallArtifacts(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	serviceCurrentGOOS = func() string { return "linux" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) {
		return filepath.Join(tempHome, "tmp", "go-build1234", "b001", "exe", "passiveagents"), nil
	}
	serviceLookupPath = func(name string) (string, error) {
		return "", exec.ErrNotFound
	}
	serviceReadManagerPIDFromState = func(string) (int, error) { return 0, os.ErrNotExist }
	var commands []string
	runServiceCommand = func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := stopManagerProcess(config{
		StateFile: filepath.Join(tempHome, "state.json"),
	}); err != nil {
		t.Fatalf("stop manager process: %v", err)
	}

	if len(commands) != 3 {
		t.Fatalf("expected linux stop path to run 3 commands, got %d (%v)", len(commands), commands)
	}
	if commands[0] != "systemctl --user disable --now passiveagents.service" {
		t.Fatalf("unexpected first stop command: %q", commands[0])
	}
	if commands[2] != "systemctl --user daemon-reload" {
		t.Fatalf("unexpected daemon-reload command: %q", commands[2])
	}
}

func TestStartManagerProcessFallsBackToUserLaunchdDomain(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	statePath := filepath.Join(tempHome, "state.json")
	serviceCurrentGOOS = func() string { return "darwin" }
	serviceUserHomeDir = func() (string, error) { return tempHome, nil }
	serviceExecutable = func() (string, error) { return "/opt/homebrew/bin/passiveagents", nil }
	serviceLookupPath = func(name string) (string, error) { return "/opt/homebrew/bin/cloudflared", nil }
	servicePathEnv = func() string { return "/opt/homebrew/bin:/usr/bin" }
	serviceCurrentUser = func() (*user.User, error) {
		return &user.User{Uid: "501", Username: "karthik"}, nil
	}
	serviceReadManagerPIDFromState = func(string) (int, error) { return 0, os.ErrNotExist }

	var commands []string
	runServiceCommand = func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		if len(args) >= 2 && args[0] == "bootstrap" && args[1] == "gui/501" {
			return fmt.Errorf("bootstrap gui/501: input/output error")
		}
		return nil
	}

	if err := startManagerProcess(config{
		StateFile:       statePath,
		WebBaseURL:      "https://passiveagents.com",
		APIBaseURL:      "https://api.passiveagents.com",
		ManagerLogFile:  filepath.Join(tempHome, ".passiveagents", "manager.log"),
		LessonsBaseDir:  filepath.Join(tempHome, ".passiveagents", "lessons"),
		SupabaseURL:     "https://supabase.passiveagents.com",
		SupabaseAnonKey: "anon",
	}); err != nil {
		t.Fatalf("start manager process: %v", err)
	}

	want := []string{
		"launchctl bootout gui/501 " + filepath.Join(tempHome, "Library", "LaunchAgents", managerLaunchdLabel+".plist"),
		"launchctl bootstrap gui/501 " + filepath.Join(tempHome, "Library", "LaunchAgents", managerLaunchdLabel+".plist"),
		"launchctl bootout user/501 " + filepath.Join(tempHome, "Library", "LaunchAgents", managerLaunchdLabel+".plist"),
		"launchctl bootstrap user/501 " + filepath.Join(tempHome, "Library", "LaunchAgents", managerLaunchdLabel+".plist"),
		"launchctl kickstart -k user/501/" + managerLaunchdLabel,
		"launchctl bootout gui/501 " + filepath.Join(tempHome, "Library", "LaunchAgents", managerLaunchdLabel+".plist"),
	}
	if len(commands) != len(want) {
		t.Fatalf("unexpected command count: got %d want %d (%v)", len(commands), len(want), commands)
	}
	for i := range want {
		if commands[i] != want[i] {
			t.Fatalf("unexpected command %d: got %q want %q", i, commands[i], want[i])
		}
	}
}

func TestInstallLaunchdServiceBootsOutGuiTargetBeforeUserFallbackAfterKickstartFailure(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	servicePath := filepath.Join(tempHome, "Library", "LaunchAgents", managerLaunchdLabel+".plist")
	artifacts := managerServiceArtifacts{
		supervisor:      "launchd",
		servicePath:     servicePath,
		launchdTargets:  []string{"gui/501", "user/501"},
	}

	var commands []string
	runServiceCommand = func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		if len(args) >= 3 && args[0] == "kickstart" && args[2] == "gui/501/"+managerLaunchdLabel {
			return fmt.Errorf("kickstart gui/501 failed")
		}
		return nil
	}

	if err := installLaunchdService(artifacts); err != nil {
		t.Fatalf("install launchd service: %v", err)
	}

	want := []string{
		"launchctl bootout gui/501 " + servicePath,
		"launchctl bootstrap gui/501 " + servicePath,
		"launchctl kickstart -k gui/501/" + managerLaunchdLabel,
		"launchctl bootout gui/501 " + servicePath,
		"launchctl bootout user/501 " + servicePath,
		"launchctl bootstrap user/501 " + servicePath,
		"launchctl kickstart -k user/501/" + managerLaunchdLabel,
		"launchctl bootout gui/501 " + servicePath,
	}
	if len(commands) != len(want) {
		t.Fatalf("unexpected command count: got %d want %d (%v)", len(commands), len(want), commands)
	}
	for i := range want {
		if commands[i] != want[i] {
			t.Fatalf("unexpected command %d: got %q want %q", i, commands[i], want[i])
		}
	}
}

func TestInstallLaunchdServiceBootsOutStaleUserTargetWhenGuiTargetSucceeds(t *testing.T) {
	restore := stubServiceGlobals(t)
	defer restore()

	tempHome := t.TempDir()
	servicePath := filepath.Join(tempHome, "Library", "LaunchAgents", managerLaunchdLabel+".plist")
	artifacts := managerServiceArtifacts{
		supervisor:     "launchd",
		servicePath:    servicePath,
		launchdTargets: []string{"gui/501", "user/501"},
	}

	var commands []string
	runServiceCommand = func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := installLaunchdService(artifacts); err != nil {
		t.Fatalf("install launchd service: %v", err)
	}

	want := []string{
		"launchctl bootout gui/501 " + servicePath,
		"launchctl bootstrap gui/501 " + servicePath,
		"launchctl kickstart -k gui/501/" + managerLaunchdLabel,
		"launchctl bootout user/501 " + servicePath,
	}
	if len(commands) != len(want) {
		t.Fatalf("unexpected command count: got %d want %d (%v)", len(commands), len(want), commands)
	}
	for i := range want {
		if commands[i] != want[i] {
			t.Fatalf("unexpected command %d: got %q want %q", i, commands[i], want[i])
		}
	}
}

func stubServiceGlobals(t *testing.T) func() {
	t.Helper()
	prevGOOS := serviceCurrentGOOS
	prevExecutable := serviceExecutable
	prevHomeDir := serviceUserHomeDir
	prevLookupPath := serviceLookupPath
	prevCurrentUser := serviceCurrentUser
	prevPathEnv := servicePathEnv
	prevReadPID := serviceReadManagerPIDFromState
	prevIsProcessRunning := serviceIsProcessRunning
	prevFindProcess := serviceFindProcess
	prevRunServiceCommand := runServiceCommand
	return func() {
		serviceCurrentGOOS = prevGOOS
		serviceExecutable = prevExecutable
		serviceUserHomeDir = prevHomeDir
		serviceLookupPath = prevLookupPath
		serviceCurrentUser = prevCurrentUser
		servicePathEnv = prevPathEnv
		serviceReadManagerPIDFromState = prevReadPID
		serviceIsProcessRunning = prevIsProcessRunning
		serviceFindProcess = prevFindProcess
		runServiceCommand = prevRunServiceCommand
	}
}
