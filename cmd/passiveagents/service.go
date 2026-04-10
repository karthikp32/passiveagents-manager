//go:build unix
// +build unix

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	managerLaunchdLabel = "com.passiveagents.manager"
	managerSystemdUnit  = "passiveagents.service"
	managerWindowsTask  = "PassiveAgents Manager"
)

var (
	serviceCurrentGOOS             = func() string { return runtime.GOOS }
	serviceExecutable              = os.Executable
	serviceUserHomeDir             = os.UserHomeDir
	serviceLookupPath              = exec.LookPath
	serviceCurrentUser             = user.Current
	servicePathEnv                 = func() string { return os.Getenv("PATH") }
	serviceReadManagerPIDFromState = readManagerPIDFromState
	serviceIsProcessRunning        = isProcessRunning
	serviceFindProcess             = os.FindProcess
	runServiceCommand              = func(name string, args ...string) error {
		cmd := exec.Command(name, args...)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		return cmd.Run()
	}
)

type serviceCommand struct {
	name        string
	args        []string
	ignoreError bool
}

type managerServiceArtifacts struct {
	supervisor       string
	servicePath      string
	launcherPath     string
	launcherContents string
	serviceContents  string
	installCommands  []serviceCommand
	stopCommands     []serviceCommand
	launchdTargets   []string
}

func startManagerProcess(cfg config) error {
	if pid, err := serviceReadManagerPIDFromState(cfg.StateFile); err == nil && serviceIsProcessRunning(pid) {
		return fmt.Errorf("passiveagents is already running (PID %d). Run 'passiveagents status' for details", pid)
	}
	if _, err := resolveConfiguredCloudflaredBinary(cfg); err != nil {
		return err
	}
	artifacts, err := buildManagerServiceArtifacts(cfg)
	if err != nil {
		return err
	}
	if err := writeManagerServiceArtifacts(artifacts); err != nil {
		return err
	}
	if len(artifacts.launchdTargets) > 0 {
		if err := installLaunchdService(artifacts); err != nil {
			return err
		}
		fmt.Printf("Manager is now managed by %s and will restart automatically when available.\n", artifacts.supervisor)
		return nil
	}
	for _, command := range artifacts.installCommands {
		if err := runServiceCommand(command.name, command.args...); err != nil && !command.ignoreError {
			return fmt.Errorf("%s setup failed: %w", artifacts.supervisor, err)
		}
	}
	fmt.Printf("Manager is now managed by %s and will restart automatically when available.\n", artifacts.supervisor)
	return nil
}

func stopManagerProcess(cfg config) error {
	artifacts, err := buildManagerStopArtifacts()
	if err != nil {
		return err
	}
	if len(artifacts.launchdTargets) > 0 {
		if err := stopLaunchdService(artifacts); err != nil {
			return err
		}
	} else {
		for _, command := range artifacts.stopCommands {
			if err := runServiceCommand(command.name, command.args...); err != nil && !command.ignoreError {
				return fmt.Errorf("%s stop failed: %w", artifacts.supervisor, err)
			}
		}
	}
	_ = os.Remove(artifacts.servicePath)
	_ = os.Remove(artifacts.launcherPath)
	if serviceCurrentGOOS() == "linux" {
		_ = runServiceCommand("systemctl", "--user", "daemon-reload")
	}
	pid, err := serviceReadManagerPIDFromState(cfg.StateFile)
	if err == nil && serviceIsProcessRunning(pid) {
		proc, findErr := serviceFindProcess(pid)
		if findErr != nil {
			return findErr
		}
		if killErr := proc.Kill(); killErr != nil {
			return killErr
		}
	}
	fmt.Printf("Stopped automatic manager startup via %s.\n", artifacts.supervisor)
	return nil
}

func installedManagerSupervisor(cfg config) string {
	servicePath, supervisor, err := managerServicePath()
	if err != nil {
		return ""
	}
	if _, err := os.Stat(servicePath); err == nil {
		return supervisor
	}
	return ""
}

func managerServicePath() (string, string, error) {
	home, err := serviceUserHomeDir()
	if err != nil {
		return "", "", err
	}
	switch serviceCurrentGOOS() {
	case "darwin":
		return filepath.Join(home, "Library", "LaunchAgents", managerLaunchdLabel+".plist"), "launchd", nil
	case "linux":
		return filepath.Join(home, ".config", "systemd", "user", managerSystemdUnit), "systemd --user", nil
	case "windows":
		return filepath.Join(home, ".passiveagents", "service", "task.xml"), "Task Scheduler", nil
	default:
		return "", "", fmt.Errorf("unsupported platform")
	}
}

func buildManagerServiceArtifacts(cfg config) (managerServiceArtifacts, error) {
	home, err := serviceUserHomeDir()
	if err != nil {
		return managerServiceArtifacts{}, fmt.Errorf("resolve home dir: %w", err)
	}
	executablePath, err := serviceExecutable()
	if err != nil {
		return managerServiceArtifacts{}, fmt.Errorf("resolve passiveagents binary: %w", err)
	}
	executablePath, err = filepath.Abs(executablePath)
	if err != nil {
		return managerServiceArtifacts{}, fmt.Errorf("resolve passiveagents binary: %w", err)
	}
	if err := validatePersistentServiceExecutable(executablePath); err != nil {
		return managerServiceArtifacts{}, err
	}
	cloudflaredPath, _ := resolveConfiguredCloudflaredBinary(cfg)
	return buildManagerServiceArtifactsForPlatform(cfg, home, executablePath, cloudflaredPath)
}

func buildManagerStopArtifacts() (managerServiceArtifacts, error) {
	home, err := serviceUserHomeDir()
	if err != nil {
		return managerServiceArtifacts{}, fmt.Errorf("resolve home dir: %w", err)
	}
	return buildManagerServiceArtifactsForPlatform(config{}, home, "", "")
}

func buildManagerServiceArtifactsForPlatform(cfg config, home, executablePath, cloudflaredPath string) (managerServiceArtifacts, error) {
	baseDir := filepath.Join(home, ".passiveagents", "service")
	switch serviceCurrentGOOS() {
	case "darwin":
		userInfo, userErr := serviceCurrentUser()
		if userErr != nil {
			return managerServiceArtifacts{}, fmt.Errorf("resolve current user: %w", userErr)
		}
		launcherPath := filepath.Join(baseDir, "run-manager.sh")
		servicePath := filepath.Join(home, "Library", "LaunchAgents", managerLaunchdLabel+".plist")
		uid := strings.TrimSpace(userInfo.Uid)
		if uid == "" {
			return managerServiceArtifacts{}, fmt.Errorf("resolve current user uid")
		}
		targets := []string{"gui/" + uid, "user/" + uid}
		return managerServiceArtifacts{
			supervisor:       "launchd",
			servicePath:      servicePath,
			launcherPath:     launcherPath,
			launcherContents: buildPOSIXLauncher(cfg, executablePath, cloudflaredPath),
			serviceContents:  buildLaunchdPlist(launcherPath),
			installCommands: []serviceCommand{
				{name: "launchctl", args: []string{"bootout", targets[0], servicePath}, ignoreError: true},
				{name: "launchctl", args: []string{"bootstrap", targets[0], servicePath}},
				{name: "launchctl", args: []string{"kickstart", "-k", targets[0] + "/" + managerLaunchdLabel}},
			},
			stopCommands: []serviceCommand{
				{name: "launchctl", args: []string{"bootout", targets[0], servicePath}, ignoreError: true},
			},
			launchdTargets: targets,
		}, nil
	case "linux":
		launcherPath := filepath.Join(baseDir, "run-manager.sh")
		servicePath := filepath.Join(home, ".config", "systemd", "user", managerSystemdUnit)
		return managerServiceArtifacts{
			supervisor:       "systemd --user",
			servicePath:      servicePath,
			launcherPath:     launcherPath,
			launcherContents: buildPOSIXLauncher(cfg, executablePath, cloudflaredPath),
			serviceContents:  buildSystemdUnit(launcherPath, home),
			installCommands: []serviceCommand{
				{name: "systemctl", args: []string{"--user", "daemon-reload"}},
				{name: "systemctl", args: []string{"--user", "enable", "--now", managerSystemdUnit}},
				{name: "systemctl", args: []string{"--user", "restart", managerSystemdUnit}},
			},
			stopCommands: []serviceCommand{
				{name: "systemctl", args: []string{"--user", "disable", "--now", managerSystemdUnit}, ignoreError: true},
				{name: "systemctl", args: []string{"--user", "daemon-reload"}, ignoreError: true},
			},
		}, nil
	case "windows":
		userInfo, userErr := serviceCurrentUser()
		if userErr != nil {
			return managerServiceArtifacts{}, fmt.Errorf("resolve current user: %w", userErr)
		}
		userName := strings.TrimSpace(userInfo.Username)
		if userName == "" {
			return managerServiceArtifacts{}, fmt.Errorf("resolve current username")
		}
		launcherPath := filepath.Join(baseDir, "run-manager.ps1")
		servicePath := filepath.Join(baseDir, "task.xml")
		return managerServiceArtifacts{
			supervisor:       "Task Scheduler",
			servicePath:      servicePath,
			launcherPath:     launcherPath,
			launcherContents: buildWindowsLauncher(cfg, executablePath, cloudflaredPath),
			serviceContents:  buildWindowsTaskXML(userName, launcherPath),
			installCommands: []serviceCommand{
				{name: "schtasks", args: []string{"/Create", "/TN", managerWindowsTask, "/XML", servicePath, "/F"}},
				{name: "schtasks", args: []string{"/Run", "/TN", managerWindowsTask}},
			},
			stopCommands: []serviceCommand{
				{name: "schtasks", args: []string{"/End", "/TN", managerWindowsTask}, ignoreError: true},
				{name: "schtasks", args: []string{"/Delete", "/TN", managerWindowsTask, "/F"}, ignoreError: true},
			},
		}, nil
	default:
		return managerServiceArtifacts{}, fmt.Errorf("automatic manager restart is not supported on %s", serviceCurrentGOOS())
	}
}

func installLaunchdService(artifacts managerServiceArtifacts) error {
	var lastErr error
	for i, target := range artifacts.launchdTargets {
		_ = runServiceCommand("launchctl", "bootout", target, artifacts.servicePath)
		if err := runServiceCommand("launchctl", "bootstrap", target, artifacts.servicePath); err != nil {
			lastErr = err
			continue
		}
		if err := runServiceCommand("launchctl", "kickstart", "-k", target+"/"+managerLaunchdLabel); err != nil {
			_ = runServiceCommand("launchctl", "bootout", target, artifacts.servicePath)
			lastErr = err
			continue
		}
		for j, otherTarget := range artifacts.launchdTargets {
			if j == i {
				continue
			}
			_ = runServiceCommand("launchctl", "bootout", otherTarget, artifacts.servicePath)
		}
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("%s setup failed: %w", artifacts.supervisor, lastErr)
	}
	return fmt.Errorf("%s setup failed", artifacts.supervisor)
}

func stopLaunchdService(artifacts managerServiceArtifacts) error {
	for _, target := range artifacts.launchdTargets {
		_ = runServiceCommand("launchctl", "bootout", target, artifacts.servicePath)
	}
	return nil
}

func validatePersistentServiceExecutable(executablePath string) error {
	cleanPath := filepath.Clean(strings.TrimSpace(executablePath))
	if cleanPath == "" {
		return fmt.Errorf("resolve passiveagents binary: empty path")
	}
	tempDir := filepath.Clean(os.TempDir())
	lowerPath := strings.ToLower(cleanPath)
	lowerTempDir := strings.ToLower(tempDir)
	if strings.HasPrefix(lowerPath, lowerTempDir+string(os.PathSeparator)) && strings.Contains(lowerPath, "go-build") {
		return temporaryGoRunBinaryError(cleanPath)
	}
	return nil
}

func temporaryGoRunBinaryError(executablePath string) error {
	installedPath := "/usr/local/bin/passiveagents"
	switch serviceCurrentGOOS() {
	case "darwin":
		if strings.Contains(strings.TrimSpace(servicePathEnv()), "/opt/homebrew/bin") {
			installedPath = "/opt/homebrew/bin/passiveagents"
		}
	case "linux":
		if strings.Contains(strings.TrimSpace(servicePathEnv()), "/home/linuxbrew/.linuxbrew/bin") {
			installedPath = "/home/linuxbrew/.linuxbrew/bin/passiveagents"
		}
	}
	return fmt.Errorf(
		"passiveagents start cannot install a persistent service from temporary go run binary %q; use an installed passiveagents binary from %s or build one locally first, for example: go build -o ~/.local/bin/passiveagents ./apps/local-agent-manager/cmd/passiveagents",
		executablePath,
		installedPath,
	)
}

func writeManagerServiceArtifacts(artifacts managerServiceArtifacts) error {
	if err := os.MkdirAll(filepath.Dir(artifacts.launcherPath), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(artifacts.servicePath), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(artifacts.launcherPath, []byte(artifacts.launcherContents), launcherFileMode()); err != nil {
		return err
	}
	if err := os.WriteFile(artifacts.servicePath, []byte(artifacts.serviceContents), 0o600); err != nil {
		return err
	}
	return nil
}

func launcherFileMode() os.FileMode {
	if serviceCurrentGOOS() == "windows" {
		return 0o600
	}
	return 0o700
}

func buildPOSIXLauncher(cfg config, executablePath, cloudflaredPath string) string {
	lines := []string{
		"#!/usr/bin/env bash",
		"set -euo pipefail",
		"mkdir -p " + shellQuote(filepath.Dir(cfg.ManagerLogFile)),
	}
	if pathValue := strings.TrimSpace(servicePathEnv()); pathValue != "" {
		lines = append(lines, "export PATH="+shellQuote(pathValue))
	}
	if strings.TrimSpace(cloudflaredPath) != "" {
		lines = append(lines, "export PASSIVEAGENTS_CLOUDFLARED_PATH="+shellQuote(cloudflaredPath))
	}
	lines = append(
		lines,
		"exec "+shellQuote(executablePath)+
			" run-manager --web-url "+shellQuote(cfg.WebBaseURL)+
			" --api-url "+shellQuote(cfg.APIBaseURL)+
			" >>"+shellQuote(cfg.ManagerLogFile)+" 2>&1",
	)
	return strings.Join(lines, "\n") + "\n"
}

func buildWindowsLauncher(cfg config, executablePath, cloudflaredPath string) string {
	lines := []string{
		"$ErrorActionPreference = 'Stop'",
		"$logDir = " + powershellQuote(filepath.Dir(cfg.ManagerLogFile)),
		"New-Item -ItemType Directory -Force -Path $logDir | Out-Null",
	}
	if pathValue := strings.TrimSpace(servicePathEnv()); pathValue != "" {
		lines = append(lines, "$env:PATH = "+powershellQuote(pathValue))
	}
	if strings.TrimSpace(cloudflaredPath) != "" {
		lines = append(lines, "$env:PASSIVEAGENTS_CLOUDFLARED_PATH = "+powershellQuote(cloudflaredPath))
	}
	lines = append(
		lines,
		"& "+powershellQuote(executablePath)+
			" 'run-manager' '--web-url' "+powershellQuote(cfg.WebBaseURL)+
			" '--api-url' "+powershellQuote(cfg.APIBaseURL)+
			" *>> "+powershellQuote(cfg.ManagerLogFile),
		"exit $LASTEXITCODE",
	)
	return strings.Join(lines, "\n") + "\n"
}

func buildLaunchdPlist(launcherPath string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>%s</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
</dict>
</plist>
`, managerLaunchdLabel, xmlEscape(launcherPath))
}

func buildSystemdUnit(launcherPath, home string) string {
	return fmt.Sprintf(`[Unit]
Description=PassiveAgents Manager
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/bin/bash %s
WorkingDirectory=%s
Restart=always
RestartSec=5
KillMode=process

[Install]
WantedBy=default.target
`, systemdEscape(launcherPath), systemdEscape(home))
}

func buildWindowsTaskXML(userName, launcherPath string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
    <EventTrigger>
      <Enabled>true</Enabled>
      <Subscription>&lt;QueryList&gt;&lt;Query Id="0" Path="System"&gt;&lt;Select Path="System"&gt;*[System[Provider[@Name='Microsoft-Windows-Power-Troubleshooter'] and EventID=1]]&lt;/Select&gt;&lt;/Query&gt;&lt;/QueryList&gt;</Subscription>
    </EventTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>%s</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>powershell.exe</Command>
      <Arguments>-NoProfile -ExecutionPolicy Bypass -File "%s"</Arguments>
    </Exec>
  </Actions>
</Task>
`, xmlEscape(userName), xmlEscape(launcherPath))
}

func resolveConfiguredCloudflaredBinary(cfg config) (string, error) {
	if strings.TrimSpace(cfg.CloudflaredBinary) != "" {
		return strings.TrimSpace(cfg.CloudflaredBinary), nil
	}
	path, err := serviceLookupPath("cloudflared")
	if err != nil {
		return "", fmt.Errorf("cloudflared not found: %w", err)
	}
	return path, nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func powershellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func xmlEscape(value string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return replacer.Replace(value)
}

func systemdEscape(value string) string {
	return strings.ReplaceAll(value, " ", `\x20`)
}
