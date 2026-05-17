//go:build unix
// +build unix

package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/golang-jwt/jwt/v4"
	"github.com/gorilla/websocket"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}

func TestChoosePersonaForTask_NoEligibleListReturnsFirstPersona(t *testing.T) {
	personas := []apiAgentPersona{
		{ID: "agent-1", Name: "A", RuntimeID: "runtime-1"},
		{ID: "agent-2", Name: "B", RuntimeID: "runtime-2"},
	}
	task := apiTask{ID: "task-1"}

	chosen := choosePersonaForTask(personas, task)
	if chosen == nil {
		t.Fatalf("expected persona, got nil")
	}
	if chosen.ID != "agent-1" {
		t.Fatalf("expected agent-1, got %s", chosen.ID)
	}
}

func TestChoosePersonaForTask_SelectsFirstMatchingEligiblePersona(t *testing.T) {
	personas := []apiAgentPersona{
		{ID: "agent-1", Name: "A", RuntimeID: "runtime-1"},
		{ID: "agent-2", Name: "B", RuntimeID: "runtime-2"},
	}
	task := apiTask{
		ID:               "task-1",
		EligibleAgentIDs: []string{"agent-3", "agent-2"},
	}

	chosen := choosePersonaForTask(personas, task)
	if chosen == nil {
		t.Fatalf("expected persona, got nil")
	}
	if chosen.ID != "agent-2" {
		t.Fatalf("expected agent-2, got %s", chosen.ID)
	}
}

func TestChoosePersonaForTask_NoMatchReturnsNil(t *testing.T) {
	personas := []apiAgentPersona{
		{ID: "agent-1", Name: "A"},
	}
	task := apiTask{
		ID:               "task-1",
		EligibleAgentIDs: []string{"agent-x"},
	}

	chosen := choosePersonaForTask(personas, task)
	if chosen != nil {
		t.Fatalf("expected nil persona, got %s", chosen.ID)
	}
}

func TestBuildPersonaPromptIncludesPersonaAndTaskFields(t *testing.T) {
	persona := apiAgentPersona{
		Name:         "Gemini Worker",
		Role:         "Engineer",
		Personality:  "Concise",
		Instructions: "Do the task carefully",
		Guardrails:   "Do: verify output. Do not: fabricate results.",
		Examples:     "Input: 3. Output: triangle with 3 rows.",
		Lessons: []string{
			"Always run tests before marking done.",
			"Prefer small diffs over broad refactors.",
		},
	}
	task := apiTask{
		Name:        "Implement tests",
		Description: "Add unit coverage",
	}

	prompt := buildPersonaPrompt(
		persona,
		task,
		nil,
		"~/.passiveagents/agents/agent-1/lessons.jsonl",
		"~/.passiveagents/agents/agent-1/AGENT_PERSONA.md",
		"~/.passiveagents/agents/agent-1/TASK_CONTEXT.md",
	)
	expectedParts := []string{
		"# Session Start",
		"Read AGENT_PERSONA.md, lessons.jsonl, and TASK_CONTEXT.md before doing anything else.",
		"~/.passiveagents/agents/agent-1/AGENT_PERSONA.md",
		"~/.passiveagents/agents/agent-1/TASK_CONTEXT.md",
		"# Task Brief",
		"### Task Name",
		"Implement tests",
		"### Task Description",
		"Add unit coverage",
		"# Task Continuation Context",
		"### Recent Task Checkpoints",
		"Periodically append progress checkpoints to task_checkpoints.jsonl while you work",
		"before marking the task complete append at least one progress checkpoint",
		"Write checkpoints as JSON lines using {\"timestamp\":\"RFC3339\",\"checkpoint_text\":\"...\"}",
		"Start work now. Keep responses concise and actionable.",
	}
	for _, part := range expectedParts {
		if !strings.Contains(prompt, part) {
			t.Fatalf("prompt missing %q: %q", part, prompt)
		}
	}
	legacyParts := []string{"You are Gemini Worker.", "Task: Implement tests", "Details: Add unit coverage", " | "}
	for _, part := range legacyParts {
		if strings.Contains(prompt, part) {
			t.Fatalf("prompt still contains legacy line %q: %q", part, prompt)
		}
	}
}

func TestWriteTaskWorkspaceFilesCreatesBootstrapFilesInWorkingDir(t *testing.T) {
	workingDir := t.TempDir()
	persona := apiAgentPersona{
		ID:           "550e8400-e29b-41d4-a716-446655440000",
		Name:         "Billy",
		Role:         "Backend Engineer",
		Personality:  "Concise",
		Instructions: "Implement backend changes carefully",
	}
	task := apiTask{
		ID:          "task-1",
		Name:        "Fix manager pagination",
		Description: "Remove duplicate terminals across pages",
	}

	personaPath, lessonsPath, taskContextPath, err := writeTaskWorkspaceFiles(
		workingDir,
		persona,
		task,
		nil,
		"{\"lesson\":\"Keep fixes small.\"}\n",
	)
	if err != nil {
		t.Fatalf("writeTaskWorkspaceFiles error: %v", err)
	}

	if got := personaPath; got != filepath.Join(workingDir, "AGENT_PERSONA.md") {
		t.Fatalf("unexpected persona path: %s", got)
	}
	if got := lessonsPath; got != filepath.Join(workingDir, "lessons.jsonl") {
		t.Fatalf("unexpected lessons path: %s", got)
	}
	if got := taskContextPath; got != filepath.Join(workingDir, "TASK_CONTEXT.md") {
		t.Fatalf("unexpected task context path: %s", got)
	}

	for _, path := range []string{personaPath, lessonsPath, taskContextPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected workspace bootstrap file %s to exist: %v", path, err)
		}
	}
	taskCheckpointPath := filepath.Join(workingDir, "task_checkpoints.jsonl")
	if _, err := os.Stat(taskCheckpointPath); err != nil {
		t.Fatalf("expected task checkpoint file %s to exist: %v", taskCheckpointPath, err)
	}

	rawTaskContext, err := os.ReadFile(taskContextPath)
	if err != nil {
		t.Fatalf("read task context: %v", err)
	}
	if !strings.Contains(string(rawTaskContext), "Fix manager pagination") {
		t.Fatalf("expected task context file to include task name, got %q", string(rawTaskContext))
	}

	rawTaskCheckpoints, err := os.ReadFile(taskCheckpointPath)
	if err != nil {
		t.Fatalf("read task checkpoints: %v", err)
	}
	if strings.TrimSpace(string(rawTaskCheckpoints)) != "" {
		t.Fatalf("expected empty task checkpoint file for empty checkpoints, got %q", string(rawTaskCheckpoints))
	}
}

func TestBuildTaskCheckpointJSONLWritesChronologicalJSONL(t *testing.T) {
	jsonl := buildTaskCheckpointJSONL([]apiTaskCheckpoint{
		{
			CreatedAt:      "2026-04-17T15:03:00.000Z",
			CheckpointText: "Middle checkpoint",
		},
		{
			CreatedAt:      "2026-04-17T15:05:00.000Z",
			CheckpointText: "Second checkpoint",
		},
		{
			CreatedAt:      "2026-04-17T15:00:00.000Z",
			CheckpointText: "First checkpoint",
		},
	})

	lines := strings.Split(strings.TrimSpace(jsonl), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 jsonl lines, got %d (%q)", len(lines), jsonl)
	}
	if !strings.Contains(lines[0], `"timestamp":"2026-04-17T15:00:00.000Z"`) {
		t.Fatalf("expected first line to be chronological, got %q", lines[0])
	}
	if !strings.Contains(lines[1], `"checkpoint_text":"Middle checkpoint"`) {
		t.Fatalf("expected second line to include middle checkpoint, got %q", lines[1])
	}
	if !strings.Contains(lines[2], `"checkpoint_text":"Second checkpoint"`) {
		t.Fatalf("expected third line to include second checkpoint, got %q", lines[2])
	}
}

func TestWriteTaskWorkspaceFilesPreservesUnsyncedLocalTaskCheckpoints(t *testing.T) {
	workingDir := t.TempDir()
	taskCheckpointPath := filepath.Join(workingDir, "task_checkpoints.jsonl")
	if err := os.WriteFile(
		taskCheckpointPath,
		[]byte(`{"timestamp":"2026-04-17T15:03:00.000Z","checkpoint_text":"Local checkpoint not uploaded yet"}`+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write existing task checkpoints: %v", err)
	}

	if _, _, _, err := writeTaskWorkspaceFiles(
		workingDir,
		apiAgentPersona{Name: "Billy"},
		apiTask{ID: "task-1", Name: "Fix checkpoint sync"},
		[]apiTaskCheckpoint{
			{
				CreatedAt:      "2026-04-17T15:00:00.000Z",
				CheckpointText: "Server checkpoint",
			},
		},
		"",
	); err != nil {
		t.Fatalf("writeTaskWorkspaceFiles error: %v", err)
	}

	records, err := loadTaskCheckpointJSONL(taskCheckpointPath)
	if err != nil {
		t.Fatalf("loadTaskCheckpointJSONL error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected server and local checkpoints to be preserved, got %d: %#v", len(records), records)
	}
	if records[0].CheckpointText != "Server checkpoint" {
		t.Fatalf("expected server checkpoint first, got %#v", records[0])
	}
	if records[1].CheckpointText != "Local checkpoint not uploaded yet" {
		t.Fatalf("expected unsynced local checkpoint to be preserved, got %#v", records[1])
	}
}

func TestWriteTaskWorkspaceFilesIncludesUnsyncedLocalTaskCheckpointsInTaskContext(t *testing.T) {
	workingDir := t.TempDir()
	taskCheckpointPath := filepath.Join(workingDir, "task_checkpoints.jsonl")
	if err := os.WriteFile(
		taskCheckpointPath,
		[]byte(`{"timestamp":"2026-05-09T23:20:00.000Z","checkpoint_text":"Local wake context checkpoint"}`+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write existing task checkpoints: %v", err)
	}

	_, _, taskContextPath, err := writeTaskWorkspaceFiles(
		workingDir,
		apiAgentPersona{Name: "Billy"},
		apiTask{ID: "task-1", Name: "Resume checkpoint task"},
		nil,
		"",
	)
	if err != nil {
		t.Fatalf("writeTaskWorkspaceFiles error: %v", err)
	}

	rawTaskContext, err := os.ReadFile(taskContextPath)
	if err != nil {
		t.Fatalf("read task context: %v", err)
	}
	taskContext := string(rawTaskContext)
	if !strings.Contains(taskContext, "Local wake context checkpoint") {
		t.Fatalf("expected local JSONL checkpoint in task context, got %q", taskContext)
	}
	if strings.Contains(taskContext, "(none yet)") {
		t.Fatalf("expected task context not to claim no checkpoints, got %q", taskContext)
	}
}

func TestLoadTaskCheckpointJSONLDeduplicatesRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "task_checkpoints.jsonl")
	content := strings.Join([]string{
		`{"timestamp":"2026-04-17T15:00:00.000Z","checkpoint_text":"First checkpoint"}`,
		`{"timestamp":"2026-04-17T15:00:00.000Z","checkpoint_text":"First checkpoint"}`,
		`{"timestamp":"2026-04-17T15:02:00.000Z","checkpoint_text":"Agent was shut down after being asked to persist progress to TASK_CONTEXT.md and lessons to lessons.jsonl. Last output: ⬝⬝⬝"}`,
		`{"timestamp":"2026-04-17T15:05:00.000Z","checkpoint_text":"Second checkpoint"}`,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write task checkpoints: %v", err)
	}

	records, err := loadTaskCheckpointJSONL(path)
	if err != nil {
		t.Fatalf("loadTaskCheckpointJSONL error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 unique task checkpoints, got %d", len(records))
	}
}

func TestNormalizeTaskCheckpointJSONLTextDropsTerminalNoise(t *testing.T) {
	if got := normalizeTaskCheckpointJSONLText("  \x1b[32mAdded focused Task Detail tests.\x1b[0m\n"); got != "Added focused Task Detail tests." {
		t.Fatalf("expected clean checkpoint text, got %q", got)
	}
	if got := normalizeTaskCheckpointJSONLText("<one-paragraph summa"); got != "" {
		t.Fatalf("expected truncated lifecycle placeholder to be dropped, got %q", got)
	}
	if got := normalizeTaskCheckpointJSONLText("Before you shut down, read TASK_CONTEXT.md and append a new JSON object to lessons.jsonl."); got != "" {
		t.Fatalf("expected shutdown instructions to be dropped, got %q", got)
	}
	if got := normalizeTaskCheckpointJSONLText("Thinking: I should probably check git status next."); got != "" {
		t.Fatalf("expected thinking text to be dropped, got %q", got)
	}
}

func TestSyncTaskCheckpointsUploadsJSONLRecords(t *testing.T) {
	workingDir := t.TempDir()
	path := filepath.Join(workingDir, "task_checkpoints.jsonl")
	content := strings.Join([]string{
		`{"timestamp":"2026-04-17T15:00:00.000Z","checkpoint_text":"First checkpoint"}`,
		`{"timestamp":"2026-04-17T15:05:00.000Z","checkpoint_text":"Second checkpoint"}`,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write task checkpoints: %v", err)
	}

	var received []map[string]any
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPut || r.URL.Path != "/tasks/task-1/task_checkpoints" {
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatalf("decode task checkpoint payload: %v", err)
		}
		return jsonHTTPResponse(200, `[]`), nil
	})

	mgr, err := newManager(config{
		WebBaseURL: "http://localhost:3000",
		APIBaseURL: "http://local.test",
		UserJWT:    "user-jwt",
		StateFile:  filepath.Join(t.TempDir(), "manager-state.json"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.client = &http.Client{Transport: transport}

	if err := mgr.syncTaskCheckpoints(context.Background(), "task-1", workingDir, "agent-1", "instance-1"); err != nil {
		t.Fatalf("syncTaskCheckpoints error: %v", err)
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 uploaded checkpoints, got %d", len(received))
	}
	if received[0]["checkpoint_text"] != "First checkpoint" {
		t.Fatalf("unexpected first uploaded checkpoint: %+v", received[0])
	}
	if received[0]["agent_id"] != "agent-1" || received[0]["agent_instance_id"] != "instance-1" {
		t.Fatalf("expected uploaded checkpoint identity, got %+v", received[0])
	}
}

func TestSyncAndBroadcastTaskCheckpointsUploadsChangedLocalRows(t *testing.T) {
	workingDir := t.TempDir()
	path := filepath.Join(workingDir, "task_checkpoints.jsonl")
	content := strings.Join([]string{
		`{"timestamp":"2026-04-17T15:00:00.000Z","checkpoint_text":"First checkpoint"}`,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write task checkpoints: %v", err)
	}

	uploads := 0
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPut || r.URL.Path != "/tasks/task-1/task_checkpoints" {
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
		uploads++
		return jsonHTTPResponse(200, `[]`), nil
	})

	mgr, err := newManager(config{
		WebBaseURL: "http://localhost:3000",
		APIBaseURL: "http://local.test",
		UserJWT:    "user-jwt",
		StateFile:  filepath.Join(t.TempDir(), "manager-state.json"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.client = &http.Client{Transport: transport}
	w := &worker{
		instanceID: "instance-1",
		taskID:     "task-1",
		workingDir: workingDir,
	}

	synced, broadcast := mgr.syncAndBroadcastTaskCheckpoints(context.Background(), w, "", "")
	if synced == "" || broadcast == "" {
		t.Fatalf("expected sync and broadcast fingerprints, got synced=%q broadcast=%q", synced, broadcast)
	}
	if uploads != 1 {
		t.Fatalf("expected one checkpoint upload, got %d", uploads)
	}

	nextSynced, nextBroadcast := mgr.syncAndBroadcastTaskCheckpoints(context.Background(), w, synced, broadcast)
	if nextSynced != "" || nextBroadcast != "" {
		t.Fatalf("expected unchanged checkpoint file to no-op, got synced=%q broadcast=%q", nextSynced, nextBroadcast)
	}
	if uploads != 1 {
		t.Fatalf("expected no second checkpoint upload, got %d", uploads)
	}

	if err := os.WriteFile(path, []byte(""), 0o600); err != nil {
		t.Fatalf("clear task checkpoints: %v", err)
	}
	clearedSynced, clearedBroadcast := mgr.syncAndBroadcastTaskCheckpoints(context.Background(), w, synced, broadcast)
	if clearedSynced != emptyTaskCheckpointFingerprint || clearedBroadcast != emptyTaskCheckpointFingerprint {
		t.Fatalf("expected empty checkpoint fingerprints after clearing file, got synced=%q broadcast=%q", clearedSynced, clearedBroadcast)
	}
	if uploads != 1 {
		t.Fatalf("expected clearing checkpoints not to upload empty JSONL, got %d uploads", uploads)
	}
}

func TestBuildPersonaDocumentIncludesLessonsInstructions(t *testing.T) {
	persona := apiAgentPersona{
		Name:         "Billy",
		Role:         "Backend Engineer",
		Personality:  "Concise",
		Instructions: "Implement backend changes carefully",
	}

	doc := buildPersonaDocument(
		persona,
		"/home/karthik/.passiveagents/agents/agent-1/lessons.jsonl",
	)

	expectedParts := []string{
		"# Agent Persona: Billy",
		"## Role\nBackend Engineer",
		"## Operating Principles",
		"Prefer correctness over speed",
		"Never read or print env vars, tokens, or secrets.",
		"Work only in the assigned working directory unless the task asks otherwise.",
		"## Self-Improvement Loop",
		"Read this file at the start of every session.",
		"Lessons file: /home/karthik/.passiveagents/agents/agent-1/lessons.jsonl",
		"Write each new lesson as one JSON object per line",
	}
	for _, part := range expectedParts {
		if !strings.Contains(doc, part) {
			t.Fatalf("persona document missing %q: %q", part, doc)
		}
	}
}

func TestWrapRuntimeCommandPrintsWorkingDirForGemini(t *testing.T) {
	wrapped := wrapRuntimeCommand("gemini --model auto")
	if runtime.GOOS == "windows" {
		expected := "cd && gemini --model auto"
		if wrapped != expected {
			t.Fatalf("expected %q, got %q", expected, wrapped)
		}
		return
	}
	expected := "pwd; exec gemini --model auto"
	if wrapped != expected {
		t.Fatalf("expected %q, got %q", expected, wrapped)
	}
}

func TestWrapRuntimeCommandLeavesNonGeminiCommandsUnchanged(t *testing.T) {
	command := "codex --help"
	if got := wrapRuntimeCommand(command); got != command {
		t.Fatalf("expected %q, got %q", command, got)
	}
}

func TestEnsureCloudflareTunnelRunningRestartsWhenProcessIsNotReady(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sleep helper process setup is unix-specific")
	}

	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = killManagedPID(cmd.Process.Pid)
			_, _ = cmd.Process.Wait()
		}
	})

	m := &manager{
		tunnelCmd: cmd,
		cfg: config{
			StreamPort:        4317,
			CloudflaredBinary: "/bin/true",
		},
	}
	m.setTunnelReady(false)

	err := m.ensureCloudflareTunnelRunning(context.Background())
	if err == nil {
		t.Fatal("expected restart attempt to fail without tunnel token")
	}
	if !strings.Contains(err.Error(), "missing tunnel token") {
		t.Fatalf("expected missing tunnel token error, got %v", err)
	}
	if m.tunnelCmd != nil {
		t.Fatal("expected stuck tunnel command to be cleared before restart")
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case <-time.After(3 * time.Second):
		t.Fatal("expected stale tunnel process to exit during restart")
	case <-waitDone:
	}
}

func TestComputeLivestreamStateReturnsOfflineWhenNoWorker(t *testing.T) {
	mgr := &manager{
		workers: map[string]*worker{},
	}

	state := mgr.computeLivestreamState("instance-1")
	if state != "offline" {
		t.Fatalf("expected offline when no worker, got %q", state)
	}
}

func TestComputeLivestreamStateReturnsOfflineForUnreadyWorker(t *testing.T) {
	mgr := &manager{
		workers: map[string]*worker{
			"instance-1": {
				instanceID: "instance-1",
				// workers that have not been explicitly marked livestream-ready stay offline.
			},
		},
	}

	state := mgr.computeLivestreamState("instance-1")
	if state != "offline" {
		t.Fatalf("expected offline for unready worker, got %q", state)
	}
}

func TestComputeLivestreamStateReturnsOfflineForWorkerWithOutputBeforeLivestreamReady(t *testing.T) {
	w := &worker{
		instanceID:           "instance-1",
		usesPTY:              true,
		runtimeCommand:       "gemini --model auto",
		outputBuffer:         &outputRingBuffer{maxSize: 4096},
		terminalState:        newVTScreenState(80, 24),
		terminalReplayBuffer: &terminalReplayBuffer{maxBytes: 4096},
		done:                 make(chan struct{}),
	}
	w.outputBuffer.Write("Loaded cached credentials.\n")
	w.terminalState.Write("Loaded cached credentials.\n")

	mgr := &manager{
		workers: map[string]*worker{
			"instance-1": w,
		},
	}

	state := mgr.computeLivestreamState("instance-1")
	if state != "offline" {
		t.Fatalf("expected offline before livestream is marked ready, got %q", state)
	}
	if w.livestreamReady.Load() {
		t.Fatal("expected computeLivestreamState to avoid self-healing livestream readiness from output alone")
	}
}

func TestComputeLivestreamStateReturnsOnlineForReadyWorker(t *testing.T) {
	w := &worker{
		instanceID:           "instance-1",
		usesPTY:              true,
		runtimeCommand:       "gemini --model auto",
		outputBuffer:         &outputRingBuffer{maxSize: 4096},
		terminalState:        newVTScreenState(80, 24),
		terminalReplayBuffer: &terminalReplayBuffer{maxBytes: 4096},
		done:                 make(chan struct{}),
	}
	w.livestreamReady.Store(true)

	mgr := &manager{
		workers: map[string]*worker{
			"instance-1": w,
		},
	}

	state := mgr.computeLivestreamState("instance-1")
	if state != "online" {
		t.Fatalf("expected online for ready worker, got %q", state)
	}
}

func TestComputeAgentStateReturnsWakingUpForUnreadyWorker(t *testing.T) {
	mgr := &manager{
		workers: map[string]*worker{
			"instance-1": {
				instanceID: "instance-1",
			},
		},
	}

	state, message := mgr.computeAgentState("instance-1")
	if state != "waking_up" {
		t.Fatalf("expected waking_up for unready worker, got %q", state)
	}
	if message == "" {
		t.Fatal("expected non-empty message for waking_up state")
	}
}

func TestComputeAgentStateReturnsWakingUpForLivestreamReadyWorkerBeforeBootstrapObserved(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
	}
	w.livestreamReady.Store(true)

	mgr := &manager{
		workers: map[string]*worker{"instance-1": w},
	}

	state, message := mgr.computeAgentState("instance-1")
	if state != "waking_up" {
		t.Fatalf("expected waking_up before bootstrap is observed, got %q", state)
	}
	if message == "" {
		t.Fatal("expected non-empty waking_up message")
	}
}

func TestComputeAgentStateReturnsWorkingForGeminiTrustPromptUserTakeover(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
	}
	w.livestreamReady.Store(true)
	w.outputBuffer.Write(strings.Join([]string{
		"> You are in /home/karthik/.passiveagents/tasks/1cc5e843",
		"Do you trust the contents of this directory? Working",
		"with untrusted contents comes with higher risk of",
		"prompt injection.",
		"› 1. Yes, continue",
		"2. No, quit",
		"Press enter to continue",
	}, "\n"))

	mgr := &manager{
		workers: map[string]*worker{"instance-1": w},
	}

	state, message := mgr.computeAgentState("instance-1")
	if state != "working" {
		t.Fatalf("expected working so the user can take over the Gemini trust prompt, got %q", state)
	}
	if !strings.Contains(strings.ToLower(message), "trust") {
		t.Fatalf("expected Gemini trust prompt message to stay visible for user takeover, got %q", message)
	}
}

func TestComputeAgentStateReturnsWorkingWithDeferredOpenCodeResumeMessage(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "opencode",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
	}
	w.livestreamReady.Store(true)
	w.outputBuffer.Write(strings.Join([]string{
		"OpenCode",
		"Resume your previous session with:",
		"$ opencode -s ses_123abc",
		"Press enter to continue",
	}, "\n"))

	mgr := &manager{
		workers: map[string]*worker{"instance-1": w},
	}

	state, message := mgr.computeAgentState("instance-1")
	if state != "working" {
		t.Fatalf("expected working while OpenCode waits on resume chooser, got %q", state)
	}
	if !strings.Contains(strings.ToLower(message), "waiting in the terminal") {
		t.Fatalf("expected terminal-action message, got %q", message)
	}
}

func TestMarkWorkerLivestreamReadyLogsMilestoneAndStates(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
	}

	mgr := &manager{logger: logger}
	mgr.markWorkerLivestreamReady(w, "Agent connected in livestream.")

	logOutput := logBuf.String()
	for _, expected := range []string{
		"milestone=pty_live",
		"livestream_state=online",
		"agent_state=waking_up",
	} {
		if !strings.Contains(logOutput, expected) {
			t.Fatalf("expected log output to contain %q, got %q", expected, logOutput)
		}
	}
}

func TestLogPTYStartFailureIncludesRuntimeAndWorkingDir(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	mgr := &manager{logger: logger}

	mgr.logPTYStartFailure("instance-1", "gemini --model auto", "/tmp/task-dir", 2, errors.New("open /dev/ptmx: resource temporarily unavailable"))

	logOutput := logBuf.String()
	for _, expected := range []string{
		"pty_start_failed",
		"instance_id=instance-1",
		"runtime=gemini --model auto",
		"working_dir=/tmp/task-dir",
		"attempt=2",
		"resource temporarily unavailable",
	} {
		if !strings.Contains(logOutput, expected) {
			t.Fatalf("expected log output to contain %q, got %q", expected, logOutput)
		}
	}
}

func setWorkerPromptSnapshotForTest(w *worker, snapshot string) {
	if w == nil {
		return
	}
	w.outputBuffer = &outputRingBuffer{maxSize: 4096}
	w.outputBuffer.Write(snapshot)
	w.terminalStateMu.Lock()
	w.terminalState = newVTScreenState(80, 24)
	if strings.TrimSpace(snapshot) != "" {
		w.terminalState.Write(snapshot)
	}
	w.terminalStateMu.Unlock()
}

func TestAwaitWorkerPromptSubmittedReturnsTrueWhenBusy(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
	}
	if !w.markShutdownRequested(buildGracefulShutdownPrompt(), shutdownModeUser) {
		t.Fatal("expected shutdown request to be recorded")
	}
	setWorkerPromptSnapshotForTest(w, "Loaded cached credentials.\n")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if !awaitWorkerPromptSubmitted(ctx, w) {
		t.Fatal("expected busy prompt to be detected")
	}
}

func TestAwaitWorkerPromptSubmittedReturnsFalseOnTimeout(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
	}
	if !w.markShutdownRequested(buildGracefulShutdownPrompt(), shutdownModeUser) {
		t.Fatal("expected shutdown request to be recorded")
	}
	setWorkerPromptSnapshotForTest(w, strings.Join([]string{
		"Gemini CLI",
		"› Before you shut down, read TASK_CONTEXT.md and task_checkpoints.jsonl",
		"Append 1-5 new progress checkpoints to task_checkpoints.jsonl",
	}, "\n"))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	if awaitWorkerPromptSubmitted(ctx, w) {
		t.Fatal("expected visible draft prompt to time out while waiting for submitted prompt")
	}
}

func TestAwaitWorkerPromptSubmittedReturnsFalseWhenRuntimeStaysReadyEmpty(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
	}
	if !w.markShutdownRequested(buildGracefulShutdownPrompt(), shutdownModeUser) {
		t.Fatal("expected shutdown request to be recorded")
	}
	setWorkerPromptSnapshotForTest(w, "Gemini CLI\nType your message or @path\n› ")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if awaitWorkerPromptSubmitted(ctx, w) {
		t.Fatal("expected pre-existing ready-empty prompt to stay unconfirmed when the shutdown draft never appears")
	}
}

func TestAwaitWorkerPromptReadyEmptyReturnsTrueWhenReadyEmpty(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
	}
	setWorkerPromptSnapshotForTest(w, "Gemini CLI\nType your message or @path\n› ")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if !awaitWorkerPromptReadyEmpty(ctx, w) {
		t.Fatal("expected ready-empty prompt to be detected")
	}
}

func TestAwaitWorkerPromptReadyEmptyReturnsFalseOnTimeout(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
	}
	setWorkerPromptSnapshotForTest(w, "Loaded cached credentials.\n")

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	if awaitWorkerPromptReadyEmpty(ctx, w) {
		t.Fatal("expected busy prompt to time out while waiting for ready-empty")
	}
}

func TestAwaitWorkerPromptSubmittedReturnsFalseForNonPTY(t *testing.T) {
	w := &worker{
		usesPTY:        false,
		runtimeCommand: "gemini --model auto",
		done:           make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if awaitWorkerPromptSubmitted(ctx, w) {
		t.Fatal("expected non-PTY worker to skip submission wait")
	}
}

func TestRequestGracefulWorkerShutdownLogsPromptAcceptanceAndFinish(t *testing.T) {
	var input bytes.Buffer
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		input:          nopWriteCloser{Writer: &input},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		logs:           make(chan logEntry, 1),
	}
	setWorkerPromptSnapshotForTest(w, "Loaded cached credentials.\n")

	mgr := &manager{
		logger:  logger,
		workers: map[string]*worker{"instance-1": w},
	}

	go func() {
		time.Sleep(250 * time.Millisecond)
		close(w.done)
	}()

	if err := mgr.requestGracefulWorkerShutdown(w); err != nil {
		t.Fatalf("requestGracefulWorkerShutdown error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		logOutput := logBuf.String()
		if strings.Contains(logOutput, "graceful_shutdown_prompt_written") &&
			strings.Contains(logOutput, "TASK_CONTEXT.md") &&
			strings.Contains(logOutput, "graceful_shutdown_agent_finished") {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("expected shutdown flow logs, got %q", logBuf.String())
}

func TestRequestWorkerShutdownKillsWorkerAfterGracePeriod(t *testing.T) {
	cmd := exec.Command("sh", "-lc", "sleep 30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep process: %v", err)
	}
	pid := cmd.Process.Pid
	exitCh := make(chan error, 1)
	doneClosed := make(chan struct{})
	go func() {
		exitCh <- cmd.Wait()
		close(doneClosed)
	}()
	t.Cleanup(func() {
		if isProcessRunning(pid) {
			_ = killManagedPID(pid)
		}
		select {
		case <-doneClosed:
		case <-time.After(2 * time.Second):
		}
	})

	var input bytes.Buffer
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		cmd:            cmd,
		input:          nopWriteCloser{Writer: &input},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		logs:           make(chan logEntry, 1),
	}
	setWorkerPromptSnapshotForTest(w, strings.Join([]string{
		"Gemini CLI",
		"› Before you shut down, read TASK_CONTEXT.md and task_checkpoints.jsonl",
		"Append 1-5 new progress checkpoints to task_checkpoints.jsonl",
	}, "\n"))

	mgr := &manager{
		logger:  logger,
		workers: map[string]*worker{"instance-1": w},
	}

	if err := mgr.requestWorkerShutdown(w, shutdownModeUser, "Shutdown requested.", 150*time.Millisecond); err != nil {
		t.Fatalf("requestWorkerShutdown error: %v", err)
	}

	select {
	case <-doneClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected worker process to be terminated after grace period")
	}

	if exitErr := <-exitCh; exitErr == nil {
		t.Fatal("expected killed worker process to return a non-nil wait error")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if strings.Contains(logBuf.String(), "graceful_shutdown_kill_sent") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected kill log after grace period, got %q", logBuf.String())
}

func TestRequestWorkerShutdownRetriesKillWithBackoff(t *testing.T) {
	oldKill := killManagedPIDFunc
	oldBaseDelay := shutdownKillRetryBaseDelay
	oldMaxDelay := shutdownKillRetryMaxDelay
	t.Cleanup(func() {
		killManagedPIDFunc = oldKill
		shutdownKillRetryBaseDelay = oldBaseDelay
		shutdownKillRetryMaxDelay = oldMaxDelay
	})

	shutdownKillRetryBaseDelay = 10 * time.Millisecond
	shutdownKillRetryMaxDelay = 20 * time.Millisecond

	cmd := exec.Command("sh", "-lc", "sleep 30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep process: %v", err)
	}
	pid := cmd.Process.Pid
	exitCh := make(chan error, 1)
	doneClosed := make(chan struct{})
	go func() {
		exitCh <- cmd.Wait()
		close(doneClosed)
	}()
	t.Cleanup(func() {
		if isProcessRunning(pid) {
			_ = killManagedPID(pid)
		}
		select {
		case <-doneClosed:
		case <-time.After(2 * time.Second):
		}
	})

	var attempts atomic.Int32
	killManagedPIDFunc = func(targetPID int) error {
		if targetPID != pid {
			return fmt.Errorf("unexpected pid %d", targetPID)
		}
		attempt := attempts.Add(1)
		if attempt < 6 {
			return nil
		}
		return killManagedPID(pid)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		cmd:            cmd,
		input:          nopWriteCloser{Writer: &bytes.Buffer{}},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		logs:           make(chan logEntry, 1),
	}
	setWorkerPromptSnapshotForTest(w, "Gemini CLI\n› shutting down")

	mgr := &manager{
		logger:  logger,
		workers: map[string]*worker{"instance-1": w},
	}

	if err := mgr.requestWorkerShutdown(w, shutdownModeUser, "Shutdown requested.", 20*time.Millisecond); err != nil {
		t.Fatalf("requestWorkerShutdown error: %v", err)
	}

	select {
	case <-doneClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected worker process to be terminated after kill retries")
	}

	if got := attempts.Load(); got != 6 {
		t.Fatalf("expected initial kill plus 5 retries before success, got %d attempts", got)
	}
	if !strings.Contains(logBuf.String(), "graceful_shutdown_kill_retrying") {
		t.Fatalf("expected retry log after kill failure, got %q", logBuf.String())
	}
}

func TestIdleShutdownRollbackClearsReadyTaskReservation(t *testing.T) {
	oldKill := killManagedPIDFunc
	oldBaseDelay := shutdownKillRetryBaseDelay
	oldMaxDelay := shutdownKillRetryMaxDelay
	t.Cleanup(func() {
		killManagedPIDFunc = oldKill
		shutdownKillRetryBaseDelay = oldBaseDelay
		shutdownKillRetryMaxDelay = oldMaxDelay
	})

	shutdownKillRetryBaseDelay = time.Millisecond
	shutdownKillRetryMaxDelay = time.Millisecond

	cmd := exec.Command("sh", "-lc", "sleep 30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep process: %v", err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		if isProcessRunning(pid) {
			_ = killManagedPID(pid)
		}
		_, _ = cmd.Process.Wait()
	})

	killManagedPIDFunc = func(targetPID int) error {
		if targetPID != pid {
			return fmt.Errorf("unexpected pid %d", targetPID)
		}
		return fmt.Errorf("kill failed")
	}

	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		cmd:            cmd,
		input:          nopWriteCloser{Writer: &bytes.Buffer{}},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		logs:           make(chan logEntry, 1),
	}
	setWorkerPromptSnapshotForTest(w, "Gemini CLI\n› shutting down")

	mgr := &manager{
		logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		workers:                  map[string]*worker{"instance-1": w},
		reservedTaskIDs:          map[string]struct{}{"ready-task": {}},
		idleShutdownReservations: map[string]string{"instance-1": "ready-task"},
	}

	if err := mgr.requestWorkerShutdown(w, shutdownModeIdleTimeout, "Shutdown requested.", time.Millisecond); err != nil {
		t.Fatalf("requestWorkerShutdown error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		requested, _, _ := w.shutdownRequestDetails()
		mgr.mu.Lock()
		_, reserved := mgr.reservedTaskIDs["ready-task"]
		_, mapped := mgr.idleShutdownReservations["instance-1"]
		mgr.mu.Unlock()
		if !requested && !reserved && !mapped {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	requested, _, _ := w.shutdownRequestDetails()
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	t.Fatalf("expected rollback to clear shutdown and reservation, requested=%t reserved=%v mapped=%v", requested, mgr.reservedTaskIDs, mgr.idleShutdownReservations)
}

func TestStartPTYProcessRetriesWithoutSetcttyWhenNativePTYNotPermitted(t *testing.T) {
	oldStartWithSize := ptyStartWithSize
	oldOpen := ptyOpen
	oldSetSize := ptySetSize
	oldCmdStart := cmdStart
	t.Cleanup(func() {
		ptyStartWithSize = oldStartWithSize
		ptyOpen = oldOpen
		ptySetSize = oldSetSize
		cmdStart = oldCmdStart
	})

	var attempts []string
	var logBuf bytes.Buffer
	mgr := &manager{logger: slog.New(slog.NewTextHandler(&logBuf, nil))}
	ptyStartWithSize = func(cmd *exec.Cmd, ws *pty.Winsize) (*os.File, error) {
		attempts = append(attempts, "native")
		return nil, syscall.EPERM
	}
	ptyOpen = func() (*os.File, *os.File, error) {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe: %v", err)
		}
		return reader, writer, nil
	}
	ptySetSize = func(*os.File, *pty.Winsize) error { return nil }
	cmdStart = func(cmd *exec.Cmd) error {
		attempts = append(attempts, "no_setctty")
		if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid || cmd.SysProcAttr.Setctty {
			t.Fatalf("expected Setsid-only PTY fallback, got %#v", cmd.SysProcAttr)
		}
		if cmd.Stdin == nil || cmd.Stdout == nil || cmd.Stderr == nil {
			t.Fatal("expected PTY fallback to wire slave to stdio")
		}
		return nil
	}

	cmd, ptmx, err := mgr.startPTYProcess("instance-1", "gemini --model auto", "gemini --model auto", t.TempDir(), &pty.Winsize{Rows: 24, Cols: 80})
	if err != nil {
		t.Fatalf("startPTYProcess: %v", err)
	}
	if cmd == nil || ptmx == nil {
		t.Fatalf("expected PTY fallback to return cmd and ptmx, got cmd=%v ptmx=%v", cmd, ptmx)
	}
	_ = ptmx.Close()
	if got := strings.Join(attempts, ","); got != "native,no_setctty" {
		t.Fatalf("expected native PTY then no-Setctty PTY, got %q", got)
	}
	for _, expected := range []string{
		"pty_start_failed",
		"attempt=1",
		"pty_setctty_blocked_retrying",
		"pty_started_without_setctty",
		"attempt=2",
	} {
		if !strings.Contains(logBuf.String(), expected) {
			t.Fatalf("expected log output to contain %q, got %q", expected, logBuf.String())
		}
	}
}

func TestStartWorkerProcessFailsForPTYRequiredRuntimeWhenPTYUnavailable(t *testing.T) {
	oldStartWithSize := ptyStartWithSize
	oldOpen := ptyOpen
	oldSetSize := ptySetSize
	oldCmdStart := cmdStart
	t.Cleanup(func() {
		ptyStartWithSize = oldStartWithSize
		ptyOpen = oldOpen
		ptySetSize = oldSetSize
		cmdStart = oldCmdStart
	})

	ptyStartWithSize = func(cmd *exec.Cmd, ws *pty.Winsize) (*os.File, error) {
		return nil, syscall.EPERM
	}
	ptyOpen = func() (*os.File, *os.File, error) {
		return nil, nil, syscall.EPERM
	}
	ptySetSize = func(*os.File, *pty.Winsize) error { return nil }
	cmdStart = func(cmd *exec.Cmd) error {
		t.Fatal("cmdStart should not be called when PTY open fails")
		return nil
	}

	mgr := &manager{
		cfg: config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
	}
	worker, err := mgr.startWorkerProcess(
		context.Background(),
		"instance-1",
		"task-1",
		"Test task",
		"IN_PROGRESS",
		"agent-1",
		"session-1",
		t.TempDir(),
		"gemini --model auto",
		"gemini --model auto",
	)
	if err == nil {
		if worker != nil {
			t.Fatalf("expected hard failure for PTY-required runtime, got worker=%v", worker)
		}
		t.Fatal("expected PTY-required runtime startup to fail when both PTY attempts fail")
	}
	if !strings.Contains(err.Error(), "pty required for gemini") {
		t.Fatalf("expected PTY-required runtime error, got %v", err)
	}
}

func TestWorkerAgentStatusReturnsWorkingForBusyPromptAfterInitialReadyEmpty(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
	}
	w.bootstrapObserved.Store(true)
	w.outputBuffer.Write("Gemini CLI\nType your message or @path\n› ")
	w.terminalState.Write("Gemini CLI\nType your message or @path\n› ")

	if got := workerAgentStatusForWorker(w); got != "working" {
		t.Fatalf("expected ready-empty bootstrap-complete prompt to be working, got %q", got)
	}

	w.outputBuffer = &outputRingBuffer{maxSize: 4096}
	w.outputBuffer.Write("Gemini CLI\nWorking...\n")
	w.terminalState = newVTScreenState(80, 24)
	w.terminalState.Write("Gemini CLI\nWorking...\n")

	if got := workerAgentStatusForWorker(w); got != "working" {
		t.Fatalf("expected busy prompt after initial ready-empty to stay working, got %q", got)
	}
}

func TestComputeAgentStateReturnsIdleAfterNoActivitySinceStart(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		startedAt:      time.Now().Add(-workerIdleDisplayThreshold - time.Second),
	}
	w.livestreamReady.Store(true)
	w.bootstrapObserved.Store(true)
	w.bootstrapReadyEmptySeen.Store(true)
	w.outputBuffer.Write("Gemini CLI\nWorking...\n")
	w.terminalState.Write("Gemini CLI\nWorking...\n")

	mgr := &manager{workers: map[string]*worker{"instance-1": w}}
	state, _ := mgr.computeAgentState("instance-1")
	if state != "idle" {
		t.Fatalf("expected worker with no activity since start to be idle after display threshold, got %q", state)
	}
}

func TestComputeAgentStateReturnsWorkingForRecentActivity(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		startedAt:      time.Now().Add(-workerIdleDisplayThreshold - time.Second),
	}
	w.livestreamReady.Store(true)
	w.bootstrapObserved.Store(true)
	w.bootstrapReadyEmptySeen.Store(true)
	w.markActivityAt(time.Now())
	w.outputBuffer.Write("Gemini CLI\nWorking...\n")
	w.terminalState.Write("Gemini CLI\nWorking...\n")

	mgr := &manager{workers: map[string]*worker{"instance-1": w}}
	state, _ := mgr.computeAgentState("instance-1")
	if state != "working" {
		t.Fatalf("expected recent activity to keep worker working, got %q", state)
	}
}

func TestCheckWorkerIdleTimeoutDoesNotShutdownWhenCapacityAvailable(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        false,
		runtimeCommand: "gemini --model auto",
		input:          nopWriteCloser{Writer: io.Discard},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		startedAt:      time.Now().Add(-workerIdleShutdownThreshold - time.Second),
	}
	w.bootstrapObserved.Store(true)
	w.bootstrapReadyEmptySeen.Store(true)
	w.outputBuffer.Write("Gemini CLI\nType your message or @path\n› ")
	w.terminalState.Write("Gemini CLI\nType your message or @path\n› ")

	mgr := &manager{
		cfg:     config{MaxConcurrent: 2, MBPerAgent: 1024},
		workers: map[string]*worker{},
		freeMB:  4096,
		totalMB: 8192,
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			t.Fatalf("did not expect READY task lookup while capacity is available: %s %s", r.Method, r.URL.Path)
			return nil, nil
		})},
	}
	mgr.checkWorkerIdleTimeout(w)

	if shutdownRequested, _, shutdownMode := w.shutdownRequestDetails(); shutdownRequested {
		t.Fatalf("did not expect idle timeout shutdown without capacity pressure, mode=%q", shutdownMode)
	}
}

func TestCheckWorkerIdleTimeoutDoesNotShutdownWhenNoReadyTaskNeedsCapacity(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		taskID:         "running-task",
		agentID:        "agent-1",
		usesPTY:        false,
		runtimeCommand: "gemini --model auto",
		input:          nopWriteCloser{Writer: io.Discard},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		startedAt:      time.Now().Add(-workerIdleShutdownThreshold - time.Second),
	}
	w.bootstrapObserved.Store(true)
	w.bootstrapReadyEmptySeen.Store(true)

	mgr := &manager{
		cfg:     config{UserJWT: "user-jwt", MaxConcurrent: 1, MBPerAgent: 1024},
		workers: map[string]*worker{"instance-1": w},
		state:   persistedState{ManagerID: "manager-1", Instances: map[string]persistedWorker{"instance-1": {Status: "idle"}}},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
				return jsonHTTPResponse(200, `[{"id":"agent-1","name":"Agent","runtime_id":"runtime-1"}]`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/managers/tasks/eligible":
				return jsonHTTPResponse(200, `[]`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}
		})},
	}
	mgr.checkWorkerIdleTimeout(w)

	if shutdownRequested, _, shutdownMode := w.shutdownRequestDetails(); shutdownRequested {
		t.Fatalf("did not expect idle timeout shutdown without READY work, mode=%q", shutdownMode)
	}
}

func TestCheckWorkerIdleTimeoutRequestsShutdownWhenReadyTaskNeedsCapacity(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		taskID:         "running-task",
		agentID:        "agent-1",
		usesPTY:        false,
		runtimeCommand: "gemini --model auto",
		input:          nopWriteCloser{Writer: io.Discard},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		startedAt:      time.Now().Add(-workerIdleShutdownThreshold - time.Second),
	}
	w.bootstrapObserved.Store(true)
	w.bootstrapReadyEmptySeen.Store(true)

	mgr := &manager{
		cfg:     config{UserJWT: "user-jwt", MaxConcurrent: 1, MBPerAgent: 1024},
		workers: map[string]*worker{"instance-1": w},
		state:   persistedState{ManagerID: "manager-1", Instances: map[string]persistedWorker{"instance-1": {Status: "idle"}}},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
				return jsonHTTPResponse(200, `[{"id":"agent-1","name":"Agent","runtime_id":"runtime-1"}]`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/managers/tasks/eligible":
				return jsonHTTPResponse(200, `[{"id":"ready-task","name":"Ready task","status":"READY","eligible_agent_ids":["agent-1"]}]`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}
		})},
	}
	mgr.checkWorkerIdleTimeout(w)

	if shutdownRequested, _, shutdownMode := w.shutdownRequestDetails(); !shutdownRequested || shutdownMode != shutdownModeIdleTimeout {
		t.Fatalf("expected idle timeout shutdown when READY work needs capacity, requested=%t mode=%q", shutdownRequested, shutdownMode)
	}
}

func TestCheckWorkerIdleTimeoutDefersShutdownWhenActivityChangesDuringReadyCheck(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		taskID:         "running-task",
		agentID:        "agent-1",
		usesPTY:        false,
		runtimeCommand: "gemini --model auto",
		input:          nopWriteCloser{Writer: io.Discard},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		startedAt:      time.Now().Add(-workerIdleShutdownThreshold - time.Second),
	}
	w.bootstrapObserved.Store(true)
	w.bootstrapReadyEmptySeen.Store(true)

	mgr := &manager{
		cfg:     config{UserJWT: "user-jwt", MaxConcurrent: 1, MBPerAgent: 1024},
		workers: map[string]*worker{"instance-1": w},
		state:   persistedState{ManagerID: "manager-1", Instances: map[string]persistedWorker{"instance-1": {Status: "idle"}}},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
				return jsonHTTPResponse(200, `[{"id":"agent-1","name":"Agent","runtime_id":"runtime-1"}]`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/managers/tasks/eligible":
				w.markActivityAt(time.Now())
				return jsonHTTPResponse(200, `[{"id":"ready-task","name":"Ready task","status":"READY","eligible_agent_ids":["agent-1"]}]`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}
		})},
	}
	mgr.checkWorkerIdleTimeout(w)

	if shutdownRequested, _, shutdownMode := w.shutdownRequestDetails(); shutdownRequested {
		t.Fatalf("did not expect idle timeout shutdown after fresh activity, mode=%q", shutdownMode)
	}
	mgr.mu.Lock()
	_, reserved := mgr.reservedTaskIDs["ready-task"]
	_, mapped := mgr.idleShutdownReservations["instance-1"]
	mgr.mu.Unlock()
	if reserved || mapped {
		t.Fatalf("expected deferred shutdown to clear reservation, reserved=%v mapped=%v", reserved, mapped)
	}
}

func TestShouldShutdownIdleWorkerReservesReadyTaskOnce(t *testing.T) {
	w1 := &worker{
		instanceID: "instance-1",
		taskID:     "running-task-1",
		done:       make(chan struct{}),
	}
	w2 := &worker{
		instanceID: "instance-2",
		taskID:     "running-task-2",
		done:       make(chan struct{}),
	}

	mgr := &manager{
		cfg:             config{UserJWT: "user-jwt", MaxConcurrent: 2, MBPerAgent: 1024},
		workers:         map[string]*worker{"instance-1": w1, "instance-2": w2},
		reservedTaskIDs: map[string]struct{}{},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
				return jsonHTTPResponse(200, `[{"id":"agent-1","name":"Agent","runtime_id":"runtime-1"}]`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/managers/tasks/eligible":
				return jsonHTTPResponse(200, `[{"id":"ready-task","name":"Ready task","status":"READY","eligible_agent_ids":["agent-1"]}]`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}
		})},
	}

	first, err := mgr.shouldShutdownIdleWorkerForReadyTask(context.Background(), w1)
	if err != nil {
		t.Fatalf("shouldShutdownIdleWorkerForReadyTask first error: %v", err)
	}
	if !first {
		t.Fatal("expected first idle worker to reserve READY task capacity")
	}
	second, err := mgr.shouldShutdownIdleWorkerForReadyTask(context.Background(), w2)
	if err != nil {
		t.Fatalf("shouldShutdownIdleWorkerForReadyTask second error: %v", err)
	}
	if second {
		t.Fatal("expected second idle worker not to evict for the same reserved READY task")
	}
}

func TestWriteAgentInputResetsActivityIdleClock(t *testing.T) {
	newIdleWorker := func(instanceID string) *worker {
		w := &worker{
			instanceID:     instanceID,
			taskID:         "running-task",
			agentID:        "agent-1",
			usesPTY:        true,
			runtimeCommand: "gemini --model auto",
			input:          nopWriteCloser{Writer: &bytes.Buffer{}},
			outputBuffer:   &outputRingBuffer{maxSize: 4096},
			terminalState:  newVTScreenState(80, 24),
			startedAt:      time.Now().Add(-workerIdleShutdownThreshold - time.Second),
			done:           make(chan struct{}),
		}
		w.livestreamReady.Store(true)
		w.bootstrapObserved.Store(true)
		w.bootstrapReadyEmptySeen.Store(true)
		return w
	}
	newReadyWorkManager := func(w *worker) *manager {
		return &manager{
			cfg:             config{UserJWT: "user-jwt", MaxConcurrent: 1, MBPerAgent: 1024},
			workers:         map[string]*worker{w.instanceID: w},
			reservedTaskIDs: map[string]struct{}{},
			state:           persistedState{ManagerID: "manager-1", Instances: map[string]persistedWorker{w.instanceID: {Status: "idle"}}},
			client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
					return jsonHTTPResponse(200, `[{"id":"agent-1","name":"Agent","runtime_id":"runtime-1"}]`), nil
				case r.Method == http.MethodPost && r.URL.Path == "/managers/tasks/eligible":
					return jsonHTTPResponse(200, `[{"id":"ready-task","name":"Ready task","status":"READY","eligible_agent_ids":["agent-1"]}]`), nil
				default:
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
					return nil, nil
				}
			})},
		}
	}

	controlWorker := newIdleWorker("control-instance")
	controlManager := newReadyWorkManager(controlWorker)
	controlManager.checkWorkerIdleTimeout(controlWorker)
	if shutdownRequested, _, shutdownMode := controlWorker.shutdownRequestDetails(); !shutdownRequested || shutdownMode != shutdownModeIdleTimeout {
		t.Fatalf("expected stale worker to shut down before user input, requested=%t mode=%q", shutdownRequested, shutdownMode)
	}

	var input bytes.Buffer
	w := newIdleWorker("instance-1")
	w.input = nopWriteCloser{Writer: &input}

	mgr := newReadyWorkManager(w)
	if err := mgr.writeAgentInputWithOptions(w, "hello", false, false); err != nil {
		t.Fatalf("writeAgentInputWithOptions error: %v", err)
	}
	mgr.checkWorkerIdleTimeout(w)

	if shutdownRequested, _, shutdownMode := w.shutdownRequestDetails(); shutdownRequested {
		t.Fatalf("expected user input to reset idle timeout, requested=%t mode=%q", shutdownRequested, shutdownMode)
	}
}

func TestComputeAgentStateReturnsOfflineWhenNoWorker(t *testing.T) {
	mgr := &manager{
		workers: map[string]*worker{},
		state: persistedState{
			Instances: map[string]persistedWorker{
				"instance-1": {Status: "working"},
			},
		},
	}

	// Persisted state must NOT be surfaced; offline is always returned when no live worker.
	state, _ := mgr.computeAgentState("instance-1")
	if state != "offline" {
		t.Fatalf("expected offline when no live worker (persisted state must not be sent), got %q", state)
	}
}

func TestComputeAgentStateReturnsShuttingDownForRequestedWorker(t *testing.T) {
	w := &worker{
		instanceID: "instance-1",
	}
	w.livestreamReady.Store(true)
	if !w.markShutdownRequested(buildGracefulShutdownPrompt(), shutdownModeManagerRestart) {
		t.Fatal("expected shutdown request to be recorded")
	}

	mgr := &manager{
		workers: map[string]*worker{"instance-1": w},
	}

	state, message := mgr.computeAgentState("instance-1")
	if state != "shutting_down" {
		t.Fatalf("expected shutting_down, got %q", state)
	}
	if message != "Manager is stopping. Waiting for the agent to persist progress." {
		t.Fatalf("unexpected shutting_down message: %q", message)
	}
}

func TestComputeAgentStateIncludesWorkingDirViaWorkerWorkingDir(t *testing.T) {
	w := &worker{
		instanceID: "instance-1",
		workingDir: "/tmp/stripe/.passiveagents-worktrees/task-1",
	}
	w.livestreamReady.Store(true)

	mgr := &manager{
		workers: map[string]*worker{"instance-1": w},
	}

	// working_dir is surfaced by broadcastAgentState via workerWorkingDir, not computeAgentState.
	// Verify workerWorkingDir returns the correct value.
	if got := mgr.workerWorkingDir("instance-1"); got != "/tmp/stripe/.passiveagents-worktrees/task-1" {
		t.Fatalf("expected working_dir from live worker, got %#v", got)
	}
}

func TestWriteSpawnBootstrapPromptReturnsErrorWhenSubmissionUnconfirmed(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		instanceID:     "instance-1",
		input:          nopWriteCloser{Writer: &input},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
	}
	w.outputBuffer.Write("Gemini CLI\nType your message or @path\n› ")

	mgr := &manager{}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	err := mgr.writeSpawnBootstrapPrompt(ctx, w, "Read TASK_CONTEXT.md")
	if err == nil {
		t.Fatal("expected bootstrap submission to remain unconfirmed")
	}
	if w.bootstrapObserved.Load() {
		t.Fatal("expected bootstrapObserved to remain false after unconfirmed submission")
	}
	if w.bootstrapPromptPending {
		t.Fatal("expected bootstrap prompt to clear after the single bootstrap attempt times out")
	}
}

func TestMaybeForceBootstrapReadyAfterObservationTimeoutIgnoresNonTimeoutErrors(t *testing.T) {
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
	}
	w.livestreamReady.Store(true)
	w.markBootstrapPromptPending("Read TASK_CONTEXT.md")

	mgr := &manager{
		workers: map[string]*worker{"instance-1": w},
	}

	if mgr.maybeForceBootstrapReadyAfterObservationTimeout(w, io.EOF) {
		t.Fatal("expected non-timeout error not to force bootstrap ready")
	}
	if w.bootstrapObserved.Load() {
		t.Fatal("expected bootstrapObserved to remain false for non-timeout error")
	}
	if !w.bootstrapPromptPending {
		t.Fatal("expected bootstrap prompt to remain pending for non-timeout error")
	}
}

func TestWriteSpawnBootstrapPromptAttemptsBootstrapOnceWhenSubmissionStaysUnconfirmed(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		instanceID:     "instance-1",
		input:          nopWriteCloser{Writer: &input},
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		done:           make(chan struct{}),
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
	}
	setWorkerPromptSnapshotForTest(w, readySnapshotForRuntime(w.runtimeCommand))

	mgr := &manager{}
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()

	prompt := "Read TASK_CONTEXT.md"
	err := mgr.writeSpawnBootstrapPrompt(ctx, w, prompt)
	if err == nil {
		t.Fatal("expected bootstrap submission to remain unconfirmed")
	}
	if got := strings.Count(input.String(), prompt+"\r"); got != 1 {
		t.Fatalf("expected one bootstrap prompt attempt, got %d writes in %q", got, input.String())
	}
}

func TestWorkerTerminalVisibleToBrowserRequiresLivestreamReadyForPTY(t *testing.T) {
	w := &worker{usesPTY: true}
	if workerTerminalVisibleToBrowser(w) {
		t.Fatal("expected PTY worker without livestream readiness to stay hidden")
	}
	w.livestreamReady.Store(true)
	if !workerTerminalVisibleToBrowser(w) {
		t.Fatal("expected livestream-ready PTY worker to be visible")
	}
}

func TestReadOutputLoopDoesNotMarkWorkerLivestreamReadyFromPromptOutputAlone(t *testing.T) {
	output := io.NopCloser(strings.NewReader("Gemini CLI\nType your message or @path\n› "))
	w := &worker{
		instanceID:           "instance-1",
		output:               output,
		outputBuffer:         &outputRingBuffer{maxSize: 4096},
		done:                 make(chan struct{}),
		logs:                 make(chan logEntry, 8),
		usesPTY:              true,
		runtimeCommand:       "gemini --model auto",
		terminalState:        newVTScreenState(80, 24),
		terminalReplayBuffer: &terminalReplayBuffer{maxBytes: 4096},
	}
	mgr := &manager{}

	mgr.readOutputLoop(context.Background(), w)

	if w.livestreamReady.Load() {
		t.Fatal("expected prompt output alone to avoid marking the worker livestream-ready")
	}
}

func TestExtractLatestLifecycleEventPrefersNeedsUserInputAndSanitizesComment(t *testing.T) {
	recent := strings.Join([]string{
		"[TASK_COMPLETED] [38;5;153m│[39m",
		"[NEEDS_USER_INPUT]   Need approval to continue.   ",
	}, "\n")

	event, ok := extractLatestLifecycleEvent(recent)
	if !ok {
		t.Fatalf("expected lifecycle event")
	}
	if event.status != "WAITING_FOR_USER_INPUT" {
		t.Fatalf("expected WAITING_FOR_USER_INPUT, got %s", event.status)
	}
	if event.comment != "Need approval to continue." {
		t.Fatalf("unexpected lifecycle comment: %q", event.comment)
	}
}

func TestExtractLatestLifecycleEventIgnoresPlaceholderNeedsInputTemplate(t *testing.T) {
	recent := strings.Join([]string{
		"When you need the user's input to proceed, output EXACTLY this line on its own:",
		"[NEEDS_USER_INPUT] <your question for the user>",
		"Start work now. Keep responses concise and actionable.",
	}, "\n")

	_, ok := extractLatestLifecycleEvent(recent)
	if ok {
		t.Fatalf("expected placeholder lifecycle template to be ignored")
	}
}

func TestExtractLatestLifecycleEventIgnoresCompactPromptPlaceholder(t *testing.T) {
	recent := "# Lifecycle Signals | When you need the user's input to proceed, output EXACTLY this line on its own: | [NEEDS_USER_INPUT] <your question for the user> | Start work now. Keep responses concise and actionable."

	_, ok := extractLatestLifecycleEvent(recent)
	if ok {
		t.Fatalf("expected compact prompt placeholder to be ignored")
	}
}

func TestExtractLatestLifecycleEventIgnoresCorruptedPromptPlaceholder(t *testing.T) {
	recent := "Session Manager handles pre-created task worktree issue\n<yourquesionfor the user> Start work now. Keep responses concise and actionable. Thinking: Considering worktree solutions [48;2;1"

	_, ok := extractLatestLifecycleEvent(recent)
	if ok {
		t.Fatalf("expected corrupted prompt placeholder to be ignored")
	}
}

func TestExtractLatestLifecycleEventIgnoresTruncatedNeedsInputPlaceholder(t *testing.T) {
	recent := "[NEEDS_USER_INPUT] <your question for t"

	_, ok := extractLatestLifecycleEvent(recent)
	if ok {
		t.Fatalf("expected truncated needs-input placeholder to be ignored")
	}
}

func TestExtractLatestLifecycleEventIgnoresShortNeedsInputPlaceholder(t *testing.T) {
	recent := "[NEEDS_USER_INPUT] <y"

	_, ok := extractLatestLifecycleEvent(recent)
	if ok {
		t.Fatalf("expected short needs-input placeholder to be ignored")
	}
}

func TestExtractLatestLifecycleEventPrefersRealNeedsInputOverPlaceholder(t *testing.T) {
	recent := strings.Join([]string{
		"[NEEDS_USER_INPUT] <your question for the user>",
		"[NEEDS_USER_INPUT] What API key should I use for Stripe?",
	}, "\n")

	event, ok := extractLatestLifecycleEvent(recent)
	if !ok {
		t.Fatalf("expected real lifecycle event")
	}
	if event.status != "WAITING_FOR_USER_INPUT" {
		t.Fatalf("expected WAITING_FOR_USER_INPUT, got %s", event.status)
	}
	if event.comment != "What API key should I use for Stripe?" {
		t.Fatalf("unexpected lifecycle comment: %q", event.comment)
	}
}

func TestScanLifecycleMarkersDefersCompletedAndLetsNeedsInputWin(t *testing.T) {
	var updates []map[string]any
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost || req.URL.Path != "/agent-instances/instance-1/update-task" {
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
				}
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Fatalf("read update payload: %v", err)
				}
				var payload map[string]any
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatalf("decode update payload: %v", err)
				}
				updates = append(updates, payload)
				return testJSONResponse(t, http.StatusOK, map[string]any{"ok": true}), nil
			}),
		},
	}
	w := &worker{
		instanceID:   "instance-1",
		agentID:      "agent-1",
		taskID:       "task-1",
		outputBuffer: &outputRingBuffer{maxSize: 4096},
	}

	firstChunk := "[TASK_COMPLETED] [38;5;153m│[39m\n"
	w.outputBuffer.Write(firstChunk)
	mgr.scanLifecycleMarkers(context.Background(), w, firstChunk)
	if len(updates) != 0 {
		t.Fatalf("expected no immediate update for pending completed marker, got %d", len(updates))
	}

	secondChunk := "[NEEDS_USER_INPUT] Need approval to continue.\n"
	w.outputBuffer.Write(secondChunk)
	mgr.scanLifecycleMarkers(context.Background(), w, secondChunk)
	if len(updates) != 1 {
		t.Fatalf("expected exactly one lifecycle update, got %d", len(updates))
	}
	if got := updates[0]["status"]; got != "WAITING_FOR_USER_INPUT" {
		t.Fatalf("expected WAITING_FOR_USER_INPUT update, got %#v", got)
	}
	if got := updates[0]["comment"]; got != "Need approval to continue." {
		t.Fatalf("unexpected waiting comment: %#v", got)
	}
	if got := updates[0]["taskId"]; got != "task-1" {
		t.Fatalf("expected lifecycle update task id %#v, got %#v", "task-1", got)
	}
	if strings.TrimSpace(w.pendingTaskCompleted) != "" {
		t.Fatalf("expected pending completed marker to be cleared, got %q", w.pendingTaskCompleted)
	}
}

func TestFlushPendingTaskCompletedPostsSanitizedSummaryAfterGracePeriod(t *testing.T) {
	var updates []map[string]any
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost || req.URL.Path != "/agent-instances/instance-1/update-task" {
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
				}
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Fatalf("read update payload: %v", err)
				}
				var payload map[string]any
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatalf("decode update payload: %v", err)
				}
				updates = append(updates, payload)
				return testJSONResponse(t, http.StatusOK, map[string]any{"ok": true}), nil
			}),
		},
	}
	w := &worker{
		instanceID:             "instance-1",
		agentID:                "",
		taskID:                 "task-1",
		pendingTaskCompleted:   cleanLifecycleComment("[38;5;153m│[39m", "Task completed (no summary)."),
		pendingTaskCompletedAt: time.Now().Add(-taskCompletedGrace - 50*time.Millisecond),
	}

	mgr.flushPendingTaskCompleted(context.Background(), w, false)
	if len(updates) != 1 {
		t.Fatalf("expected one completed update, got %d", len(updates))
	}
	if got := updates[0]["status"]; got != "READY_FOR_REVIEW" {
		t.Fatalf("expected READY_FOR_REVIEW, got %#v", got)
	}
	if got := updates[0]["comment"]; got != "Task completed (no summary)." {
		t.Fatalf("unexpected completed comment: %#v", got)
	}
	if got := updates[0]["taskId"]; got != "task-1" {
		t.Fatalf("expected completed update task id %#v, got %#v", "task-1", got)
	}
}

func TestFlushPendingTaskCompletedPersistsWorkerLessonsBeforeSync(t *testing.T) {
	tempDir := t.TempDir()
	personaID := "550e8400-e29b-41d4-a716-446655440000"
	workingDir := filepath.Join(tempDir, "worktree")
	if err := writeFileAtomic(
		filepath.Join(workingDir, "lessons.jsonl"),
		[]byte("{\"lesson\":\"Copy worktree lessons back before sync.\"}\n"),
		0o600,
		0o700,
	); err != nil {
		t.Fatalf("write workspace lessons: %v", err)
	}

	mgr := &manager{
		cfg: config{
			APIBaseURL:       "http://example.test",
			UserJWT:          "token",
			LessonsBaseDir:   filepath.Join(tempDir, "agents"),
			LessonsJSONLFile: filepath.Join(tempDir, "lessons.jsonl"),
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body := `{"ok":true}`
				if req.Method != http.MethodPost || req.URL.Path != "/agent-instances/instance-1/update-task" {
					body = `[]`
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(body)),
				}, nil
			}),
		},
	}
	w := &worker{
		instanceID:             "instance-1",
		agentID:                personaID,
		taskID:                 "task-1",
		workingDir:             workingDir,
		pendingTaskCompleted:   "Task completed.",
		pendingTaskCompletedAt: time.Now().Add(-taskCompletedGrace - 50*time.Millisecond),
	}

	mgr.flushPendingTaskCompleted(context.Background(), w, false)

	records, err := mgr.loadGlobalLessonRecords()
	if err != nil {
		t.Fatalf("load global lessons: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one persisted lesson, got %d: %#v", len(records), records)
	}
	if records[0].AgentPersonaID != personaID || records[0].Lesson != "Copy worktree lessons back before sync." {
		t.Fatalf("unexpected persisted lesson: %#v", records[0])
	}
}

func TestFlushPendingTaskCompletedBacksOffAfterRateLimit(t *testing.T) {
	requests := 0
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost || req.URL.Path != "/agent-instances/instance-1/update-task" {
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
				}
				requests++
				if requests == 1 {
					return testJSONResponse(t, http.StatusTooManyRequests, map[string]any{
						"error":             "rate_limited",
						"retryAfterSeconds": 11,
					}), nil
				}
				return testJSONResponse(t, http.StatusOK, map[string]any{"ok": true}), nil
			}),
		},
	}
	w := &worker{
		instanceID:             "instance-1",
		taskID:                 "task-1",
		pendingTaskCompleted:   "Task completed.",
		pendingTaskCompletedAt: time.Now().Add(-taskCompletedGrace - 50*time.Millisecond),
	}

	mgr.flushPendingTaskCompleted(context.Background(), w, false)
	if requests != 1 {
		t.Fatalf("expected first completed update request, got %d", requests)
	}
	if w.pendingTaskCompletedRetryAt.IsZero() {
		t.Fatal("expected retry timestamp after rate limit")
	}

	mgr.flushPendingTaskCompleted(context.Background(), w, false)
	if requests != 1 {
		t.Fatalf("expected no retry before retry timestamp, got %d requests", requests)
	}

	w.pendingTaskCompletedRetryAt = time.Now().Add(-time.Second)
	mgr.flushPendingTaskCompleted(context.Background(), w, false)
	if requests != 2 {
		t.Fatalf("expected retry after retry timestamp, got %d requests", requests)
	}
	if !w.sawTaskCompleted {
		t.Fatal("expected task completion after retry succeeds")
	}
}

func TestFlushPendingTaskCompletedSkipsWhenStatusAlreadyKnown(t *testing.T) {
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				t.Fatalf("unexpected no-op lifecycle update request: %s %s", req.Method, req.URL.String())
				return nil, nil
			}),
		},
	}
	w := &worker{
		instanceID:             "instance-1",
		taskID:                 "task-1",
		taskStatus:             "READY_FOR_REVIEW",
		pendingTaskCompleted:   "Task completed.",
		pendingTaskCompletedAt: time.Now().Add(-taskCompletedGrace - 50*time.Millisecond),
	}

	mgr.flushPendingTaskCompleted(context.Background(), w, false)
	if !w.sawTaskCompleted {
		t.Fatal("expected task completion marker to be considered handled")
	}
	if strings.TrimSpace(w.pendingTaskCompleted) != "" {
		t.Fatalf("expected pending completed marker to clear, got %q", w.pendingTaskCompleted)
	}
}

func TestFlushPendingTaskCompletedSkipsWhenMetadataStatusAlreadyKnown(t *testing.T) {
	tasksMetadataFile := filepath.Join(t.TempDir(), "tasks.json")
	mgr := &manager{
		cfg: config{
			APIBaseURL:        "http://example.test",
			UserJWT:           "token",
			TasksMetadataFile: tasksMetadataFile,
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				t.Fatalf("unexpected no-op lifecycle update request: %s %s", req.Method, req.URL.String())
				return nil, nil
			}),
		},
	}
	if err := mgr.writeTaskMetadataStore(taskMetadataStore{
		Tasks: map[string]taskMetadata{
			"task-1": {
				ID:     "task-1",
				Status: "READY_FOR_REVIEW",
			},
		},
	}); err != nil {
		t.Fatalf("write task metadata: %v", err)
	}
	w := &worker{
		instanceID:             "instance-1",
		taskID:                 "task-1",
		pendingTaskCompleted:   "Task completed.",
		pendingTaskCompletedAt: time.Now().Add(-taskCompletedGrace - 50*time.Millisecond),
	}

	mgr.flushPendingTaskCompleted(context.Background(), w, false)
	if !w.sawTaskCompleted {
		t.Fatal("expected task completion marker to be considered handled")
	}
	if w.taskStatus != "READY_FOR_REVIEW" {
		t.Fatalf("expected worker task status to be restored from metadata, got %q", w.taskStatus)
	}
}

func TestFlushPendingTaskCompletedRefreshesBackendStatusBeforeNoopUpdate(t *testing.T) {
	tasksMetadataFile := filepath.Join(t.TempDir(), "tasks.json")
	getRequests := 0
	mgr := &manager{
		cfg: config{
			APIBaseURL:        "http://example.test",
			UserJWT:           "token",
			TasksMetadataFile: tasksMetadataFile,
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet && req.URL.Path == "/tasks/task-1" {
					getRequests++
					return testJSONResponse(t, http.StatusOK, map[string]any{
						"id":                         "task-1",
						"status":                     "READY_FOR_REVIEW",
						"assigned_agent_instance_id": nil,
						"working_directory":          "/tmp/passiveagents-task",
					}), nil
				}
				t.Fatalf("unexpected lifecycle update request after backend status refresh: %s %s", req.Method, req.URL.String())
				return nil, nil
			}),
		},
	}
	if err := mgr.writeTaskMetadataStore(taskMetadataStore{
		Tasks: map[string]taskMetadata{
			"task-1": {
				ID:     "task-1",
				Status: "IN_PROGRESS",
			},
		},
	}); err != nil {
		t.Fatalf("write task metadata: %v", err)
	}
	w := &worker{
		instanceID:             "instance-1",
		taskID:                 "task-1",
		pendingTaskCompleted:   "Task completed.",
		pendingTaskCompletedAt: time.Now().Add(-taskCompletedGrace - 50*time.Millisecond),
	}

	mgr.flushPendingTaskCompleted(context.Background(), w, false)
	if getRequests != 1 {
		t.Fatalf("expected one backend task refresh request, got %d", getRequests)
	}
	if !w.sawTaskCompleted {
		t.Fatal("expected task completion marker to be considered handled")
	}
	store, err := mgr.readTaskMetadataStore()
	if err != nil {
		t.Fatalf("read task metadata: %v", err)
	}
	if got := store.Tasks["task-1"].Status; got != "READY_FOR_REVIEW" {
		t.Fatalf("expected refreshed metadata status READY_FOR_REVIEW, got %q", got)
	}
}

func TestNeedsUserInputMarkerSkipsWhenStatusAlreadyKnown(t *testing.T) {
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				t.Fatalf("unexpected no-op lifecycle update request: %s %s", req.Method, req.URL.String())
				return nil, nil
			}),
		},
	}
	w := &worker{
		instanceID:   "instance-1",
		taskID:       "task-1",
		taskStatus:   "WAITING_FOR_USER_INPUT",
		outputBuffer: &outputRingBuffer{maxSize: 4096},
	}

	chunk := "[NEEDS_USER_INPUT] Need approval to continue.\n"
	w.outputBuffer.Write(chunk)
	mgr.scanLifecycleMarkers(context.Background(), w, chunk)
	if !w.sawNeedsUserInput {
		t.Fatal("expected needs-user-input marker to be considered handled")
	}
}

func TestNeedsUserInputMarkerSkipsWhenTaskAlreadyReadyForReview(t *testing.T) {
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				t.Fatalf("unexpected stale needs-user-input update request: %s %s", req.Method, req.URL.String())
				return nil, nil
			}),
		},
	}
	w := &worker{
		instanceID:   "instance-1",
		taskID:       "task-1",
		taskStatus:   "READY_FOR_REVIEW",
		outputBuffer: &outputRingBuffer{maxSize: 4096},
	}

	chunk := "[NEEDS_USER_INPUT] Need approval to continue.\n"
	w.outputBuffer.Write(chunk)
	mgr.scanLifecycleMarkers(context.Background(), w, chunk)
	if !w.sawNeedsUserInput {
		t.Fatal("expected stale needs-user-input marker to be considered handled")
	}
}

func TestUpdateTaskStatusFromLifecyclePersistsTaskMetadata(t *testing.T) {
	var requests int
	tasksMetadataFile := filepath.Join(t.TempDir(), "tasks.json")
	mgr := &manager{
		cfg: config{
			APIBaseURL:        "http://example.test",
			UserJWT:           "token",
			TasksMetadataFile: tasksMetadataFile,
		},
		state: persistedState{
			ManagerID: "manager-1",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost || req.URL.Path != "/agent-instances/instance-1/update-task" {
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
				}
				requests++
				return testJSONResponse(t, http.StatusOK, map[string]any{"ok": true}), nil
			}),
		},
	}
	w := &worker{
		instanceID: "instance-1",
		taskID:     "task-1",
		agentID:    "agent-1",
		workingDir: "/tmp/passiveagents-task",
	}

	if err := mgr.updateTaskStatusFromLifecycle(context.Background(), w, "READY_FOR_REVIEW", "done"); err != nil {
		t.Fatalf("updateTaskStatusFromLifecycle error: %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected one lifecycle update request, got %d", requests)
	}
	store, err := mgr.readTaskMetadataStore()
	if err != nil {
		t.Fatalf("read task metadata: %v", err)
	}
	metadata := store.Tasks["task-1"]
	if metadata.Status != "READY_FOR_REVIEW" {
		t.Fatalf("expected metadata status READY_FOR_REVIEW, got %#v", metadata)
	}
	if metadata.InstanceID != "instance-1" || metadata.AgentID != "agent-1" {
		t.Fatalf("expected metadata ids to persist, got %#v", metadata)
	}
}

func TestCurrentManagerBuildMetadataDefaults(t *testing.T) {
	originalVersion := managerVersion
	originalChannel := managerInstallChannel
	t.Cleanup(func() {
		managerVersion = originalVersion
		managerInstallChannel = originalChannel
	})

	managerVersion = ""
	managerInstallChannel = ""

	if got := currentManagerVersion(); got != "dev" {
		t.Fatalf("expected default version dev, got %q", got)
	}
	if got := currentInstallChannel(); got != "dev-source" {
		t.Fatalf("expected default install channel dev-source, got %q", got)
	}
}

func TestCurrentManagerBuildMetadataUsesStampedValues(t *testing.T) {
	originalVersion := managerVersion
	originalChannel := managerInstallChannel
	t.Cleanup(func() {
		managerVersion = originalVersion
		managerInstallChannel = originalChannel
	})

	managerVersion = "1.2.3"
	managerInstallChannel = "homebrew"

	if got := currentManagerVersion(); got != "1.2.3" {
		t.Fatalf("expected stamped version, got %q", got)
	}
	if got := currentInstallChannel(); got != "homebrew" {
		t.Fatalf("expected stamped install channel, got %q", got)
	}
}

func TestRegisterManagerIncludesReleaseMetadata(t *testing.T) {
	originalVersion := managerVersion
	originalChannel := managerInstallChannel
	t.Cleanup(func() {
		managerVersion = originalVersion
		managerInstallChannel = originalChannel
	})
	managerVersion = "2.3.4"
	managerInstallChannel = "homebrew"

	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:        "http://local.test",
		UserJWT:           "user-jwt",
		MachineName:       "regression-box",
		ExecutionMode:     "LOCAL",
		MaxConcurrent:     3,
		StateFile:         filepath.Join(tempDir, "manager-state.json"),
		HeartbeatInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}

	var captured map[string]any
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/local-agent-managers/register" {
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer user-jwt" {
			return nil, fmt.Errorf("unexpected authorization header %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			return nil, err
		}
		return jsonHTTPResponse(200, `{"machineId":"manager-1"}`), nil
	})}

	if err := m.registerManager(context.Background()); err != nil {
		t.Fatalf("registerManager error: %v", err)
	}

	if got := captured["managerVersion"]; got != "2.3.4" {
		t.Fatalf("expected managerVersion=2.3.4, got %#v", got)
	}
	if got := captured["platform"]; got != runtime.GOOS {
		t.Fatalf("expected platform=%s, got %#v", runtime.GOOS, got)
	}
	if got := captured["installChannel"]; got != "homebrew" {
		t.Fatalf("expected installChannel=homebrew, got %#v", got)
	}
}

func TestDetectInstalledCodingAgentProvidersReturnsInstalledProviders(t *testing.T) {
	previousVersionCheck := codingAgentVersionCheck
	codingAgentVersionCheck = func(command string) error {
		switch command {
		case "codex", "gemini":
			return nil
		default:
			return exec.ErrNotFound
		}
	}
	t.Cleanup(func() {
		codingAgentVersionCheck = previousVersionCheck
	})

	got := installedCodingAgentProviders()
	want := []string{"CODEX", "GEMINI_CLI"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("installedCodingAgentProviders() = %v, want %v", got, want)
	}
}

func TestRegisterManagerIncludesInstalledCodingAgentProviders(t *testing.T) {
	previousVersionCheck := codingAgentVersionCheck
	codingAgentVersionCheck = func(command string) error {
		switch command {
		case "claude", "opencode":
			return nil
		default:
			return exec.ErrNotFound
		}
	}
	t.Cleanup(func() {
		codingAgentVersionCheck = previousVersionCheck
	})

	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:        "http://local.test",
		UserJWT:           "user-jwt",
		MachineName:       "regression-box",
		ExecutionMode:     "LOCAL",
		MaxConcurrent:     1,
		StateFile:         filepath.Join(tempDir, "manager-state.json"),
		HeartbeatInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}

	var captured map[string]any
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/local-agent-managers/register" {
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			return nil, err
		}
		return jsonHTTPResponse(200, `{"machineId":"manager-1"}`), nil
	})}

	if err := m.registerManager(context.Background()); err != nil {
		t.Fatalf("registerManager error: %v", err)
	}

	got, ok := captured["installedCodingAgentProviders"].([]any)
	if !ok {
		t.Fatalf("expected installedCodingAgentProviders array, got %#v", captured["installedCodingAgentProviders"])
	}
	if !reflect.DeepEqual(got, []any{"CLAUDE_CODE", "OPENCODE"}) {
		t.Fatalf("installedCodingAgentProviders = %v, want %v", got, []any{"CLAUDE_CODE", "OPENCODE"})
	}
}

func TestRegisterManagerIncludesStoredRefreshToken(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	originalKeyringSet := keyringSet
	originalKeyringGet := keyringGet
	var sessionInKeyring string
	keyringSet = func(service, user, value string) error {
		if user == keyringUser {
			sessionInKeyring = value
		}
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if user == keyringUser && sessionInKeyring != "" {
			return sessionInKeyring, nil
		}
		return "", errors.New("secret not found in keyring")
	}
	t.Cleanup(func() {
		keyringSet = originalKeyringSet
		keyringGet = originalKeyringGet
	})

	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:        "http://local.test",
		MachineName:       "regression-box",
		ExecutionMode:     "LOCAL",
		MaxConcurrent:     1,
		StateFile:         filepath.Join(tempDir, "manager-state.json"),
		HeartbeatInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}

	session := storedSession{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
		UserID:       "user-1",
	}
	if err := m.saveSession(session); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	var captured map[string]any
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/local-agent-managers/register" {
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer access-token" {
			return nil, fmt.Errorf("unexpected authorization header %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			return nil, err
		}
		return jsonHTTPResponse(200, `{"machineId":"manager-1"}`), nil
	})}

	if err := m.registerManager(context.Background()); err != nil {
		t.Fatalf("registerManager error: %v", err)
	}

	if got := captured["refreshToken"]; got != "refresh-token" {
		t.Fatalf("expected refreshToken=refresh-token, got %#v", got)
	}
}

func TestRegisterManagerSkipsWhenManagerIDAndTunnelTokenAlreadyExist(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:  "http://local.test",
		UserJWT:     "user-jwt",
		MachineName: "existing-box",
		StateFile:   filepath.Join(tempDir, "manager-state.json"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.state.ManagerID = "manager-existing"
	m.state.TunnelToken = "token-existing"
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected request to %s", r.URL.Path)
		return nil, nil
	})}

	if err := m.registerManager(context.Background()); err != nil {
		t.Fatalf("registerManager error: %v", err)
	}
}

func TestRegisterManagerSyncsStoredSessionWhenAlreadyRegistered(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:  "http://local.test",
		MachineName: "existing-box",
		StateFile:   filepath.Join(tempDir, "manager-state.json"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	if err := m.saveSession(storedSession{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	m.state.ManagerID = "manager-existing"
	m.state.TunnelToken = "token-existing"
	syncCalls := 0
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/local-agent-managers/manager-existing/session" {
			t.Fatalf("unexpected request to %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer access-token" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if got := payload["refreshToken"]; got != "refresh-token" {
			t.Fatalf("unexpected refreshToken %#v", got)
		}
		syncCalls++
		return jsonHTTPResponse(200, `{"ok":true}`), nil
	})}

	if err := m.registerManager(context.Background()); err != nil {
		t.Fatalf("registerManager error: %v", err)
	}
	if syncCalls != 1 {
		t.Fatalf("expected one session sync call, got %d", syncCalls)
	}
}

func TestRegisterManagerFetchesTunnelTokenWhenMissingLocally(t *testing.T) {
	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:  "http://local.test",
		UserJWT:     "user-jwt",
		MachineName: "existing-box",
		StateFile:   filepath.Join(tempDir, "manager-state.json"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.state.ManagerID = "manager-existing"
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/local-agent-managers/register" {
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			return nil, fmt.Errorf("unexpected request method %s", r.Method)
		}
		return jsonHTTPResponse(200, `{"machineId":"manager-existing","tunnelId":"tunnel-existing","tunnelToken":"token-existing","managerSubdomain":"existing"}`), nil
	})}

	if err := m.registerManager(context.Background()); err != nil {
		t.Fatalf("registerManager error: %v", err)
	}

	if got := m.state.ManagerID; got != "manager-existing" {
		t.Fatalf("expected manager-existing, got %q", got)
	}
	if got := m.state.TunnelToken; got != "token-existing" {
		t.Fatalf("expected token-existing, got %q", got)
	}
	if got := m.tunnelID; got != "tunnel-existing" {
		t.Fatalf("expected tunnel-existing, got %q", got)
	}
}

func TestEnsureRegisteredReturnsErrorWhenTunnelTokenRecoveryFails(t *testing.T) {
	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:  "http://local.test",
		UserJWT:     "user-jwt",
		MachineName: "existing-box",
		StateFile:   filepath.Join(tempDir, "manager-state.json"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.state.ManagerID = "manager-existing"
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/local-agent-managers/register" {
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
		return jsonHTTPResponse(500, `{"message":"backend down"}`), nil
	})}

	err = m.ensureRegistered(context.Background())
	if err == nil {
		t.Fatal("expected ensureRegistered to return error")
	}
	if !strings.Contains(err.Error(), "backend down") {
		t.Fatalf("expected backend error to propagate, got %v", err)
	}
}

func TestSendHeartbeatIncludesReleaseMetadata(t *testing.T) {
	originalVersion := managerVersion
	originalChannel := managerInstallChannel
	t.Cleanup(func() {
		managerVersion = originalVersion
		managerInstallChannel = originalChannel
	})
	managerVersion = "2.3.4"
	managerInstallChannel = "windows-direct"

	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:  "http://local.test",
		UserJWT:     "manager-jwt",
		StateFile:   filepath.Join(tempDir, "manager-state.json"),
		MachineName: "heartbeat-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.state.ManagerID = "manager-123"
	m.cpu = 12.5
	m.freeMB = 2048

	var captured map[string]any
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/managers/manager-123/heartbeat" {
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
		if r.Method != http.MethodPatch {
			return nil, fmt.Errorf("unexpected request method %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			return nil, err
		}
		return jsonHTTPResponse(200, `{}`), nil
	})}

	if err := m.sendHeartbeat(context.Background()); err != nil {
		t.Fatalf("sendHeartbeat error: %v", err)
	}

	if got := captured["manager_version"]; got != "2.3.4" {
		t.Fatalf("expected manager_version=2.3.4, got %#v", got)
	}
	if got := captured["platform"]; got != runtime.GOOS {
		t.Fatalf("expected platform=%s, got %#v", runtime.GOOS, got)
	}
	if got := captured["install_channel"]; got != "windows-direct" {
		t.Fatalf("expected install_channel=windows-direct, got %#v", got)
	}
}

func TestBootstrapRegisterAndHeartbeatPersistOnlineMetadata(t *testing.T) {
	originalVersion := managerVersion
	originalChannel := managerInstallChannel
	t.Cleanup(func() {
		managerVersion = originalVersion
		managerInstallChannel = originalChannel
	})
	managerVersion = "3.4.5"
	managerInstallChannel = "homebrew"

	userJWT := mustTestJWT(t, jwt.MapClaims{
		"sub": "user-123",
		"exp": time.Now().Add(time.Hour).Unix(),
	})

	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "manager-state.json")
	m, err := newManager(config{
		APIBaseURL:  "http://local.test",
		WebBaseURL:  "https://app.passiveagents.test",
		UserJWT:     userJWT,
		MachineName: "install-smoke-box",
		StateFile:   stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}

	registerCalled := false
	heartbeatCalled := false
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/version/client-config":
			return jsonHTTPResponse(200, `{"supabaseUrl":"http://local.test","supabaseAnonKey":"anon-key"}`), nil
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"error":"jwks unavailable"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer "+userJWT {
				return nil, fmt.Errorf("unexpected authorization header %q", got)
			}
			if got := r.Header.Get("apikey"); got != "anon-key" {
				return nil, fmt.Errorf("unexpected apikey header %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"user-123"}`), nil
		case "/local-agent-managers/register":
			registerCalled = true
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				return nil, err
			}
			if got := payload["managerVersion"]; got != "3.4.5" {
				return nil, fmt.Errorf("unexpected managerVersion %#v", got)
			}
			if got := payload["installChannel"]; got != "homebrew" {
				return nil, fmt.Errorf("unexpected installChannel %#v", got)
			}
			return jsonHTTPResponse(200, `{"machineId":"manager-online-1"}`), nil
		case "/api/managers/manager-online-1/heartbeat":
			heartbeatCalled = true
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				return nil, err
			}
			if got := payload["manager_version"]; got != "3.4.5" {
				return nil, fmt.Errorf("unexpected manager_version %#v", got)
			}
			if got := payload["install_channel"]; got != "homebrew" {
				return nil, fmt.Errorf("unexpected install_channel %#v", got)
			}
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		default:
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
	})}

	if err := m.ensureSupabaseBootstrap(context.Background()); err != nil {
		t.Fatalf("ensureSupabaseBootstrap error: %v", err)
	}
	if got := m.cfg.SupabaseURL; got != "http://local.test" {
		t.Fatalf("expected SupabaseURL to be bootstrapped, got %q", got)
	}
	if got := m.cfg.SupabaseAnonKey; got != "anon-key" {
		t.Fatalf("expected SupabaseAnonKey to be bootstrapped, got %q", got)
	}

	if err := m.registerManager(context.Background()); err != nil {
		t.Fatalf("registerManager error: %v", err)
	}
	if err := m.refreshManagerUserIdentity(context.Background()); err != nil {
		t.Fatalf("refreshManagerUserIdentity error: %v", err)
	}
	if err := m.sendHeartbeat(context.Background()); err != nil {
		t.Fatalf("sendHeartbeat error: %v", err)
	}

	if !registerCalled {
		t.Fatalf("expected register endpoint to be called")
	}
	if !heartbeatCalled {
		t.Fatalf("expected heartbeat endpoint to be called")
	}
	if got := strings.TrimSpace(m.state.ManagerID); got != "manager-online-1" {
		t.Fatalf("expected manager id to be persisted, got %q", got)
	}
	if got := strings.TrimSpace(m.userID); got != "user-123" {
		t.Fatalf("expected user id to be refreshed, got %q", got)
	}

	persisted, err := readState(stateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if got := strings.TrimSpace(persisted.ManagerID); got != "manager-online-1" {
		t.Fatalf("expected persisted manager id, got %q", got)
	}
	if got := strings.TrimSpace(persisted.SupabaseURL); got != "http://local.test" {
		t.Fatalf("expected persisted SupabaseURL, got %q", got)
	}
	if got := strings.TrimSpace(persisted.SupabaseAnonKey); got != "anon-key" {
		t.Fatalf("expected persisted SupabaseAnonKey, got %q", got)
	}
}

func TestUpdateManagerAuthStateImmediatelyHeartbeatsFreshReauthSessionID(t *testing.T) {
	tempDir := t.TempDir()
	m, err := newManager(config{
		APIBaseURL:  "http://local.test",
		UserJWT:     "manager-jwt",
		StateFile:   filepath.Join(tempDir, "manager-state.json"),
		MachineName: "reauth-heartbeat-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.state.ManagerID = "manager-123"

	heartbeatSeen := make(chan map[string]any, 1)
	var captured map[string]any
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/managers/manager-123/heartbeat" {
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
		if r.Method != http.MethodPatch {
			return nil, fmt.Errorf("unexpected request method %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			return nil, err
		}
		heartbeatSeen <- captured
		return jsonHTTPResponse(200, `{}`), nil
	})}

	if err := m.updateManagerAuthState(true, "login-456", "verifier-456"); err != nil {
		t.Fatalf("updateManagerAuthState error: %v", err)
	}
	select {
	case captured = <-heartbeatSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auth-state heartbeat")
	}

	resourceMetrics, ok := captured["resource_metrics"].(map[string]any)
	if !ok {
		t.Fatalf("expected resource_metrics map, got %#v", captured["resource_metrics"])
	}
	if got := resourceMetrics["reauth_required"]; got != true {
		t.Fatalf("expected reauth_required=true, got %#v", got)
	}
	if got := resourceMetrics["reauth_session_id"]; got != "login-456" {
		t.Fatalf("expected reauth_session_id=login-456, got %#v", got)
	}
}

func TestEnsureSupabaseBootstrapFallsBackToAPIClientConfigWhenRootReturnsHTML(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "manager-state.json")

	m, err := newManager(config{
		APIBaseURL: "https://passiveagents.com",
		WebBaseURL: "https://passiveagents.com",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}

	rootRequests := 0
	apiRequests := 0
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/version/client-config":
			rootRequests++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
				Body: io.NopCloser(bytes.NewBufferString(
					`<!doctype html><html><body>PassiveAgents</body></html>`,
				)),
			}, nil
		case "/api/version/client-config":
			apiRequests++
			return jsonHTTPResponse(200, `{"supabaseUrl":"https://example.supabase.co","supabaseAnonKey":"anon-key"}`), nil
		default:
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
	})}

	if err := m.ensureSupabaseBootstrap(context.Background()); err != nil {
		t.Fatalf("ensureSupabaseBootstrap error: %v", err)
	}

	if rootRequests != 1 {
		t.Fatalf("expected one root client-config request, got %d", rootRequests)
	}
	if apiRequests != 1 {
		t.Fatalf("expected one /api client-config fallback request, got %d", apiRequests)
	}
	if got := m.cfg.SupabaseURL; got != "https://example.supabase.co" {
		t.Fatalf("expected SupabaseURL to be bootstrapped from /api fallback, got %q", got)
	}
	if got := m.cfg.SupabaseAnonKey; got != "anon-key" {
		t.Fatalf("expected SupabaseAnonKey to be bootstrapped from /api fallback, got %q", got)
	}
}

func TestLoginKeepsSessionWhenManagerRegistrationFails(t *testing.T) {
	disableBrowserOpenForTest(t)

	originalKeyringSet := keyringSet
	originalKeyringGet := keyringGet
	originalSignalManagedPIDFunc := signalManagedPIDFunc
	var storedSessionRaw string
	keyringSet = func(service, user, password string) error {
		storedSessionRaw = password
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if storedSessionRaw == "" {
			return "", errors.New("secret not found in keyring")
		}
		return storedSessionRaw, nil
	}
	t.Cleanup(func() {
		keyringSet = originalKeyringSet
		keyringGet = originalKeyringGet
		signalManagedPIDFunc = originalSignalManagedPIDFunc
	})
	signalCalls := 0
	signalManagedPIDFunc = func(pid int, sig syscall.Signal) error {
		signalCalls++
		if pid != 4321 {
			t.Fatalf("expected login to notify manager pid 4321, got %d", pid)
		}
		if sig != syscall.SIGHUP {
			t.Fatalf("expected login to notify manager with SIGHUP, got %v", sig)
		}
		return nil
	}

	originalStdin := os.Stdin
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe error: %v", err)
	}
	if _, err := writer.Write([]byte("\n")); err != nil {
		t.Fatalf("stdin write error: %v", err)
	}
	_ = writer.Close()
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = originalStdin
	})

	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "manager-state.json")
	m, err := newManager(config{
		APIBaseURL: "http://local.test",
		WebBaseURL: "https://app.passiveagents.test",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.state.ManagerPID = 4321
	if err := writeState(stateFile, m.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	accessToken := mustTestJWT(t, jwt.MapClaims{
		"sub":   "user-123",
		"email": "user@example.com",
		"exp":   time.Now().Add(time.Hour).Unix(),
	})
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/version/client-config":
			return jsonHTTPResponse(200, `{"supabaseUrl":"http://local.test","supabaseAnonKey":"anon-key"}`), nil
		case "/auth/init":
			return jsonHTTPResponse(200, `{"shortId":"login-123"}`), nil
		case "/auth/sessions/login-123/poll":
			return jsonHTTPResponse(200, fmt.Sprintf(
				`{"pending":false,"accessToken":%q,"refreshToken":"refresh-token","expiresAt":4070908800,"userId":"user-123"}`,
				accessToken,
			)), nil
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer "+accessToken {
				t.Fatalf("unexpected Supabase user auth header: %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"user-123"}`), nil
		case "/local-agent-managers/register":
			return jsonHTTPResponse(400, `{"message":"Unable to create Cloudflare tunnel","error":"Bad Request","statusCode":400}`), nil
		default:
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
	})}

	output, err := captureStdout(t, func() error {
		return m.login(context.Background())
	})
	if err != nil {
		t.Fatalf("expected login to succeed with a warning after registration failure, got %v", err)
	}

	session, loadErr := m.loadSession()
	if loadErr != nil {
		t.Fatalf("loadSession error: %v", loadErr)
	}
	if session.AccessToken != accessToken {
		t.Fatalf("expected saved access token, got %q", session.AccessToken)
	}
	if session.UserEmail != "user@example.com" {
		t.Fatalf("expected saved user email user@example.com, got %q", session.UserEmail)
	}
	if !strings.Contains(output, "Authentication complete.") {
		t.Fatalf("expected successful login output, got %q", output)
	}
	if !strings.Contains(output, "Unable to create Cloudflare tunnel") {
		t.Fatalf("expected registration warning in output, got %q", output)
	}
	if signalCalls != 1 {
		t.Fatalf("expected login to notify the running manager once, got %d calls", signalCalls)
	}
}

func TestLoginRejectsMalformedAccessToken(t *testing.T) {
	disableBrowserOpenForTest(t)

	originalKeyringSet := keyringSet
	keyringSet = func(service, user, password string) error {
		t.Fatalf("malformed login token must not be saved")
		return nil
	}
	t.Cleanup(func() {
		keyringSet = originalKeyringSet
	})

	originalStdin := os.Stdin
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe error: %v", err)
	}
	if _, err := writer.Write([]byte("\n")); err != nil {
		t.Fatalf("stdin write error: %v", err)
	}
	_ = writer.Close()
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = originalStdin
	})

	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "manager-state.json")
	m, err := newManager(config{
		APIBaseURL: "http://local.test",
		WebBaseURL: "https://app.passiveagents.test",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}

	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/version/client-config":
			return jsonHTTPResponse(200, `{"supabaseUrl":"http://local.test","supabaseAnonKey":"anon-key"}`), nil
		case "/auth/init":
			return jsonHTTPResponse(200, `{"shortId":"login-123"}`), nil
		case "/auth/sessions/login-123/poll":
			return jsonHTTPResponse(200, `{"pending":false,"accessToken":"not-a-jwt","refreshToken":"refresh-token","expiresAt":4070908800,"userId":"user-123"}`), nil
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer not-a-jwt" {
				t.Fatalf("unexpected Supabase user auth header: %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"different-user"}`), nil
		case "/local-agent-managers/register":
			t.Fatalf("malformed login token must not register manager")
			return nil, nil
		default:
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
	})}

	_, err = captureStdout(t, func() error {
		return m.login(context.Background())
	})
	if err == nil || !strings.Contains(err.Error(), "invalid access token") {
		t.Fatalf("expected invalid access token error, got %v", err)
	}
}

func TestLoginSyncsNewSessionToRegisteredManager(t *testing.T) {
	disableBrowserOpenForTest(t)

	originalKeyringSet := keyringSet
	originalKeyringGet := keyringGet
	var storedSessionRaw string
	keyringSet = func(service, user, password string) error {
		storedSessionRaw = password
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if storedSessionRaw == "" {
			return "", errors.New("secret not found in keyring")
		}
		return storedSessionRaw, nil
	}
	t.Cleanup(func() {
		keyringSet = originalKeyringSet
		keyringGet = originalKeyringGet
	})

	originalStdin := os.Stdin
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe error: %v", err)
	}
	if _, err := writer.Write([]byte("\n")); err != nil {
		t.Fatalf("stdin write error: %v", err)
	}
	_ = writer.Close()
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = originalStdin
	})

	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "manager-state.json")
	m, err := newManager(config{
		APIBaseURL: "http://local.test",
		WebBaseURL: "https://app.passiveagents.test",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.state.ManagerID = "manager-1"
	m.state.TunnelToken = "tunnel-token"
	m.state.AuthRequired = true
	m.state.ReauthSessionID = "login-stale"
	m.state.ReauthCodeVerifier = "verifier-stale"
	if err := writeState(stateFile, m.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	sessionSyncCalls := 0
	accessToken := mustTestJWT(t, jwt.MapClaims{
		"sub":   "user-123",
		"email": "user@example.com",
		"exp":   time.Now().Add(time.Hour).Unix(),
	})
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/version/client-config":
			return jsonHTTPResponse(200, `{"supabaseUrl":"http://local.test","supabaseAnonKey":"anon-key"}`), nil
		case "/auth/init":
			return jsonHTTPResponse(200, `{"shortId":"login-123"}`), nil
		case "/auth/sessions/login-123/poll":
			return jsonHTTPResponse(200, fmt.Sprintf(
				`{"pending":false,"accessToken":%q,"refreshToken":"refresh-token","expiresAt":4070908800,"userId":"user-123"}`,
				accessToken,
			)), nil
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer "+accessToken {
				t.Fatalf("unexpected Supabase user auth header: %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"user-123"}`), nil
		case "/local-agent-managers/manager-1/session":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode session sync payload: %v", err)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer "+accessToken {
				t.Fatalf("unexpected session sync auth header: %q", got)
			}
			if got := payload["refreshToken"]; got != "refresh-token" {
				t.Fatalf("unexpected synced refresh token: %#v", got)
			}
			sessionSyncCalls++
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		case "/api/managers/manager-1/heartbeat":
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		default:
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
	})}

	if _, err := captureStdout(t, func() error {
		return m.login(context.Background())
	}); err != nil {
		t.Fatalf("expected login to succeed, got %v", err)
	}
	if sessionSyncCalls == 0 {
		t.Fatal("expected login to sync the new session to the registered manager")
	}
}

func TestReloadAuthStateFromDiskRefreshesRunningManagerIdentity(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	freshToken := mustTestJWT(t, jwt.MapClaims{
		"sub": "user-fresh",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "manager-state.json")
	mgr, err := newManager(config{
		StateFile:       stateFile,
		SupabaseURL:     "http://local.test",
		SupabaseAnonKey: "anon-key",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.userID = "user-stale"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-stale"
	mgr.state.ReauthCodeVerifier = "verifier-stale"
	if err := writeState(stateFile, persistedState{}); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  freshToken,
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
		UserID:       "user-fresh",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"error":"jwks unavailable"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer "+freshToken {
				return nil, fmt.Errorf("unexpected authorization header %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"user-fresh"}`), nil
		default:
			return nil, fmt.Errorf("unexpected path %s", r.URL.Path)
		}
	})}

	if err := mgr.reloadAuthStateFromDisk(); err != nil {
		t.Fatalf("reloadAuthStateFromDisk error: %v", err)
	}
	if got := mgr.userID; got != "user-fresh" {
		t.Fatalf("expected running manager user id to refresh, got %q", got)
	}
	if mgr.state.AuthRequired {
		t.Fatalf("expected auth required to be cleared from disk state")
	}
	if mgr.state.ReauthSessionID != "" || mgr.state.ReauthCodeVerifier != "" {
		t.Fatalf("expected reauth fields to clear, got session=%q verifier=%q", mgr.state.ReauthSessionID, mgr.state.ReauthCodeVerifier)
	}
}

func TestVersionCommandPrintsStampedBuildMetadata(t *testing.T) {
	originalVersion := managerVersion
	originalChannel := managerInstallChannel
	t.Cleanup(func() {
		managerVersion = originalVersion
		managerInstallChannel = originalChannel
	})

	managerVersion = "9.8.7"
	managerInstallChannel = "homebrew"

	output, err := executeCLITestCommand(
		t,
		config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
		"version",
	)
	if err != nil {
		t.Fatalf("version command returned error: %v", err)
	}

	expected := fmt.Sprintf(
		"passiveagents 9.8.7 (%s/%s, homebrew)",
		runtime.GOOS,
		runtime.GOARCH,
	)
	if !strings.Contains(output, expected) {
		t.Fatalf("expected output to contain %q, got %q", expected, output)
	}
}

func TestVersionFlagPrintsStampedBuildMetadata(t *testing.T) {
	originalVersion := managerVersion
	originalChannel := managerInstallChannel
	t.Cleanup(func() {
		managerVersion = originalVersion
		managerInstallChannel = originalChannel
	})

	managerVersion = "9.8.7"
	managerInstallChannel = "homebrew"

	output, err := executeCLITestCommand(
		t,
		config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
		"--version",
	)
	if err != nil {
		t.Fatalf("version flag returned error: %v", err)
	}

	expected := fmt.Sprintf(
		"passiveagents 9.8.7 (%s/%s, homebrew)",
		runtime.GOOS,
		runtime.GOARCH,
	)
	if !strings.Contains(output, expected) {
		t.Fatalf("expected output to contain %q, got %q", expected, output)
	}
}

func TestAddCommandHelpShowsSingleFolderUsage(t *testing.T) {
	output, err := executeCLITestCommand(
		t,
		config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
		"add",
		"--help",
	)
	if err != nil {
		t.Fatalf("add help returned error: %v\noutput=%s", err, output)
	}
	if !strings.Contains(output, "Usage:\n  passiveagents add folder <folder-path> [flags]") {
		t.Fatalf("expected single add folder usage, got %q", output)
	}
	if strings.Contains(output, "Available Commands:") {
		t.Fatalf("expected add help to avoid nested subcommands, got %q", output)
	}
}

func TestStatusCommandReportsStoppedWhenNoPIDExists(t *testing.T) {
	useIsolatedServiceHomeDir(t)
	output, err := executeCLITestCommand(
		t,
		config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
		"status",
	)
	if err != nil {
		t.Fatalf("status command returned error: %v", err)
	}
	if strings.TrimSpace(output) != "Status: stopped" {
		t.Fatalf("unexpected status output: %q", output)
	}
}

func TestStatusCommandReportsRunningManagerPID(t *testing.T) {
	useIsolatedServiceHomeDir(t)
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	proc := startPassiveAgentsTestProcess(t)
	if err := writeManagerPIDToState(stateFile, proc.Process.Pid); err != nil {
		t.Fatalf("writeManagerPIDToState error: %v", err)
	}

	output, err := executeCLITestCommand(t, config{StateFile: stateFile}, "status")
	if err != nil {
		t.Fatalf("status command returned error: %v", err)
	}
	expected := fmt.Sprintf("Status: manager running (PID: %d)", proc.Process.Pid)
	if strings.TrimSpace(output) != expected {
		t.Fatalf("unexpected status output: %q", output)
	}
}

func TestStatusCommandReportsStalePID(t *testing.T) {
	useIsolatedServiceHomeDir(t)
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	proc := startPassiveAgentsTestProcess(t)
	pid := proc.Process.Pid
	stopPassiveAgentsTestProcess(t, proc)
	if err := writeManagerPIDToState(stateFile, pid); err != nil {
		t.Fatalf("writeManagerPIDToState error: %v", err)
	}

	output, err := executeCLITestCommand(t, config{StateFile: stateFile}, "status")
	if err != nil {
		t.Fatalf("status command returned error: %v", err)
	}
	expected := fmt.Sprintf("Status: stopped (stale pid %d)", pid)
	if strings.TrimSpace(output) != expected {
		t.Fatalf("unexpected status output: %q", output)
	}
}

func TestChatCommandRejectsUnknownTargetBeforeManagerSetup(t *testing.T) {
	_, err := executeCLITestCommand(
		t,
		config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
		"chat",
		"worker",
		"instance-1",
	)
	if err == nil || !strings.Contains(err.Error(), "unknown chat target: worker") {
		t.Fatalf("expected unknown chat target error, got %v", err)
	}
}

func TestListCommandRejectsUnknownTargetBeforeManagerSetup(t *testing.T) {
	_, err := executeCLITestCommand(
		t,
		config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
		"list",
		"workers",
	)
	if err == nil || !strings.Contains(err.Error(), "unknown list target: workers") {
		t.Fatalf("expected unknown list target error, got %v", err)
	}
}

func TestAgentLogsCommandRejectsUnknownAction(t *testing.T) {
	_, err := executeCLITestCommand(
		t,
		config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")},
		"agent",
		"tail",
		"instance-1",
	)
	if err == nil || !strings.Contains(err.Error(), "unknown agent action: tail") {
		t.Fatalf("expected unknown agent action error, got %v", err)
	}
}

func TestRenderWorkingTasksIncludesLocalAndReadyTasks(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	proc := startPassiveAgentsTestProcess(t)
	if err := writeState(stateFile, persistedState{
		Instances: map[string]persistedWorker{
			"instance-1": {
				PID:     proc.Process.Pid,
				TaskID:  "task-1",
				AgentID: "agent-1",
			},
		},
	}); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	m, err := newManager(config{
		APIBaseURL: "http://local.test",
		UserJWT:    "user-jwt",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.RequestURI() {
		case "/tasks?status=READY":
			return jsonHTTPResponse(200, `[
				{"id":"task-1","name":"Print triangle","status":"READY"},
				{"id":"task-2","name":"Review PR","status":"READY"}
			]`), nil
		case "/tasks":
			return jsonHTTPResponse(200, `[{"id":"task-1","name":"Print triangle","status":"IN_PROGRESS"}]`), nil
		case "/agents":
			return jsonHTTPResponse(200, `[{"id":"agent-1","name":"Triangle Agent"}]`), nil
		default:
			return nil, fmt.Errorf("unexpected request uri %s", r.URL.RequestURI())
		}
	})}

	output, err := m.renderWorkingTasks(context.Background())
	if err != nil {
		t.Fatalf("renderWorkingTasks error: %v", err)
	}

	if !strings.Contains(output, "task-1") || !strings.Contains(output, "Triangle Agent") || !strings.Contains(output, "IN_PROGRESS") {
		t.Fatalf("expected local running task row in output, got %q", output)
	}
	if !strings.Contains(output, "task-2") || !strings.Contains(output, "Review PR") || !strings.Contains(output, "READY") {
		t.Fatalf("expected ready task row in output, got %q", output)
	}
	if strings.Count(output, "task-1") != 1 {
		t.Fatalf("expected running task to be deduplicated from ready list, got %q", output)
	}
}

func TestRenderAgentInstancesIncludesResolvedMetadata(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	proc := startPassiveAgentsTestProcess(t)
	if err := writeState(stateFile, persistedState{
		Instances: map[string]persistedWorker{
			"instance-1": {
				PID:     proc.Process.Pid,
				TaskID:  "task-1",
				AgentID: "agent-1",
			},
		},
	}); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	m, err := newManager(config{
		APIBaseURL: "http://local.test",
		UserJWT:    "user-jwt",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/agents":
			return jsonHTTPResponse(200, `[{"id":"agent-1","name":"Triangle Agent","role":"Renderer"}]`), nil
		case "/tasks":
			return jsonHTTPResponse(200, `[{"id":"task-1","name":"Print triangle","status":"IN_PROGRESS"}]`), nil
		default:
			return nil, fmt.Errorf("unexpected request path %s", r.URL.Path)
		}
	})}

	output, err := m.renderAgentInstances(context.Background())
	if err != nil {
		t.Fatalf("renderAgentInstances error: %v", err)
	}

	if !strings.Contains(output, "instance-1") || !strings.Contains(output, "Triangle Agent") || !strings.Contains(output, "Renderer") {
		t.Fatalf("expected resolved agent metadata in output, got %q", output)
	}
	if !strings.Contains(output, "Print triangle") || !strings.Contains(output, "IN_PROGRESS") {
		t.Fatalf("expected resolved task metadata in output, got %q", output)
	}
}

func TestResetTaskForTestingRemovesMatchingInstancesAndUpdatesTask(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	proc := startPassiveAgentsTestProcess(t)
	if err := writeState(stateFile, persistedState{
		Instances: map[string]persistedWorker{
			"instance-1": {
				PID:     proc.Process.Pid,
				TaskID:  "task-1",
				AgentID: "agent-1",
			},
			"instance-2": {
				PID:     999999,
				TaskID:  "task-2",
				AgentID: "agent-2",
			},
		},
	}); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	m, err := newManager(config{
		APIBaseURL: "http://local.test",
		UserJWT:    "user-jwt",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}

	var payload map[string]string
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPut || r.URL.Path != "/tasks/task-1" {
			return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			return nil, err
		}
		return jsonHTTPResponse(200, `{}`), nil
	})}

	output, err := captureStdout(t, func() error {
		return m.resetTaskForTesting(context.Background(), "task-1")
	})
	if err != nil {
		t.Fatalf("resetTaskForTesting error: %v", err)
	}
	if payload["status"] != "READY" {
		t.Fatalf("expected task reset payload, got %#v", payload)
	}
	if !strings.Contains(output, "Task task-1 reset to READY. removed_instances=1 stopped_processes=1") {
		t.Fatalf("unexpected reset output: %q", output)
	}

	waitForPassiveAgentsTestProcessExit(t, proc)
	state, err := readState(stateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if len(state.Instances) != 1 || state.Instances["instance-2"].TaskID != "task-2" {
		t.Fatalf("expected only unrelated instance to remain, got %#v", state.Instances)
	}
}

func TestTailManagerLogsPrintsCurrentFile(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "manager.log")
	if err := os.WriteFile(logPath, []byte("line one\nline two\n"), 0o600); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	output, err := captureStdout(t, func() error {
		return tailManagerLogs(logPath, false)
	})
	if err != nil {
		t.Fatalf("tailManagerLogs error: %v", err)
	}
	if output != "line one\nline two\n" {
		t.Fatalf("unexpected manager log output: %q", output)
	}
}

func TestTailLocalAgentLogsPrintsTranscriptAndRawLogs(t *testing.T) {
	baseDir := t.TempDir()
	stateFile := filepath.Join(baseDir, "manager-state.json")
	transcriptPath := filepath.Join(baseDir, "instances", "instance-1", "transcript.ndjson")
	rawPath := filepath.Join(baseDir, "instances", "instance-1", "logs.raw")
	if err := os.MkdirAll(filepath.Dir(transcriptPath), 0o700); err != nil {
		t.Fatalf("mkdir transcript dir: %v", err)
	}
	transcript := strings.Join([]string{
		`{"line":"hello from agent\n","log_type":"stdout","timestamp":"2026-03-19T00:00:00Z"}`,
		`{"line":"Need a code review","log_type":"assistant","timestamp":"2026-03-19T00:00:01Z"}`,
		"",
	}, "\n")
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	if err := os.WriteFile(rawPath, []byte("\x1b[32mraw output\x1b[0m\n"), 0o600); err != nil {
		t.Fatalf("write raw log: %v", err)
	}
	if err := writeState(stateFile, persistedState{
		Instances: map[string]persistedWorker{
			"instance-1": {
				PID:                 1234,
				TaskID:              "task-1",
				AgentID:             "agent-1",
				LocalLogFile:        rawPath,
				LocalTranscriptFile: transcriptPath,
			},
		},
	}); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	transcriptOutput, err := captureStdout(t, func() error {
		return tailLocalAgentLogs(stateFile, "instance-1", false, false)
	})
	if err != nil {
		t.Fatalf("tailLocalAgentLogs transcript error: %v", err)
	}
	if !strings.Contains(transcriptOutput, "hello from agent") || !strings.Contains(transcriptOutput, "assistant> Need a code review") {
		t.Fatalf("unexpected transcript output: %q", transcriptOutput)
	}

	rawOutput, err := captureStdout(t, func() error {
		return tailLocalAgentLogs(stateFile, "instance-1", false, true)
	})
	if err != nil {
		t.Fatalf("tailLocalAgentLogs raw error: %v", err)
	}
	if rawOutput != "\x1b[32mraw output\x1b[0m\n" {
		t.Fatalf("unexpected raw output: %q", rawOutput)
	}
}

func TestManagerLogsWarnsWhenManagerAuthRequired(t *testing.T) {
	tempHome := t.TempDir()
	stateFile := filepath.Join(tempHome, "manager-state.json")
	logFile := filepath.Join(tempHome, "manager.log")
	if err := os.WriteFile(logFile, []byte("manager log line\n"), 0o600); err != nil {
		t.Fatalf("write manager log: %v", err)
	}
	if err := writeState(stateFile, persistedState{AuthRequired: true}); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	root := newRootCommand(config{
		StateFile:      stateFile,
		ManagerLogFile: logFile,
		APIBaseURL:     "http://local.test",
		WebBaseURL:     "http://localhost:3000",
	})
	var stderr bytes.Buffer
	root.SetErr(&stderr)
	root.SetArgs([]string{"manager", "logs"})

	stdout, err := captureStdout(t, func() error {
		return root.Execute()
	})
	if err != nil {
		t.Fatalf("manager logs error: %v", err)
	}
	if !strings.Contains(stderr.String(), loginAgainCLIMessage) {
		t.Fatalf("expected login warning on stderr, got %q", stderr.String())
	}
	if !strings.Contains(stdout, "manager log line") {
		t.Fatalf("expected manager logs on stdout, got %q", stdout)
	}
}

func TestStopCommandSkipsAuthRequiredNotice(t *testing.T) {
	root := newRootCommand(config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")})
	cmd, _, err := root.Find([]string{"stop"})
	if err != nil {
		t.Fatalf("find stop command: %v", err)
	}
	if !shouldSkipAuthRequiredNotice(cmd) {
		t.Fatal("expected stop command to skip auth required notice")
	}
}

func TestBroadcastBrowserTerminalOutputEnqueuesSubscribedClientOnly(t *testing.T) {
	m, err := newManager(config{StateFile: filepath.Join(t.TempDir(), "manager-state.json")})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.workers["instance-1"] = &worker{instanceID: "instance-1"}

	subscribed := &browserClient{
		subscribedInstance: map[string]struct{}{"instance-1": {}},
		outbound:           make(chan map[string]any, 1),
	}
	unsubscribed := &browserClient{
		subscribedInstance: map[string]struct{}{},
		outbound:           make(chan map[string]any, 1),
	}
	m.browserClients[subscribed] = struct{}{}
	m.browserClients[unsubscribed] = struct{}{}

	m.broadcastBrowserTerminalOutput("instance-1", 42, []byte("hello"))

	select {
	case payload := <-subscribed.outbound:
		if payload["type"] != "terminal_output" {
			t.Fatalf("unexpected payload type: %#v", payload)
		}
		if payload["instance_id"] != "instance-1" {
			t.Fatalf("unexpected instance id: %#v", payload)
		}
		if payload["sequence"] != uint64(42) {
			t.Fatalf("unexpected sequence: %#v", payload)
		}
		expected := base64.StdEncoding.EncodeToString([]byte("hello"))
		if payload["data_base64"] != expected {
			t.Fatalf("unexpected payload data: %#v", payload)
		}
	default:
		t.Fatalf("expected subscribed client to receive terminal output")
	}

	select {
	case payload := <-unsubscribed.outbound:
		t.Fatalf("unexpected payload for unsubscribed client: %#v", payload)
	default:
	}
}

func TestAuthenticateBrowserClientAcceptsMatchingManagerUser(t *testing.T) {
	token := mustTestJWT(t, jwt.MapClaims{
		"sub": "user-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	mgr := &manager{
		cfg: config{
			SupabaseURL:     "https://example.supabase.co",
			SupabaseAnonKey: "anon-key",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/auth/v1/keys":
					return testJSONResponse(t, http.StatusInternalServerError, map[string]any{
						"error": "jwks unavailable",
					}), nil
				case "/auth/v1/user":
					if got := req.Header.Get("Authorization"); got != "Bearer "+token {
						return testJSONResponse(t, http.StatusUnauthorized, map[string]any{
							"error": fmt.Sprintf("unexpected authorization header %q", got),
						}), nil
					}
					if got := req.Header.Get("apikey"); got != "anon-key" {
						return testJSONResponse(t, http.StatusUnauthorized, map[string]any{
							"error": fmt.Sprintf("unexpected apikey header %q", got),
						}), nil
					}
					return testJSONResponse(t, http.StatusOK, map[string]any{
						"id": "user-1",
					}), nil
				default:
					return nil, fmt.Errorf("unexpected request path %s", req.URL.Path)
				}
			}),
		},
		userID: "user-1",
	}
	client := &browserClient{
		subscribedInstance: map[string]struct{}{},
	}

	if err := mgr.authenticateBrowserClient(context.Background(), client, token); err != nil {
		t.Fatalf("authenticateBrowserClient error: %v", err)
	}
	if !browserClientAuthenticated(client) {
		t.Fatalf("expected browser client to be authenticated")
	}
}

func TestAuthenticateBrowserClientRejectsDifferentManagerUser(t *testing.T) {
	token := mustTestJWT(t, jwt.MapClaims{
		"sub": "user-2",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	mgr := &manager{
		cfg: config{
			SupabaseURL:     "https://example.supabase.co",
			SupabaseAnonKey: "anon-key",
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/auth/v1/keys":
					return testJSONResponse(t, http.StatusInternalServerError, map[string]any{
						"error": "jwks unavailable",
					}), nil
				case "/auth/v1/user":
					if got := req.Header.Get("Authorization"); got != "Bearer "+token {
						return testJSONResponse(t, http.StatusUnauthorized, map[string]any{
							"error": fmt.Sprintf("unexpected authorization header %q", got),
						}), nil
					}
					if got := req.Header.Get("apikey"); got != "anon-key" {
						return testJSONResponse(t, http.StatusUnauthorized, map[string]any{
							"error": fmt.Sprintf("unexpected apikey header %q", got),
						}), nil
					}
					return testJSONResponse(t, http.StatusOK, map[string]any{
						"id": "user-2",
					}), nil
				default:
					return nil, fmt.Errorf("unexpected request path %s", req.URL.Path)
				}
			}),
		},
		userID: "user-1",
	}
	client := &browserClient{
		subscribedInstance: map[string]struct{}{},
	}

	err := mgr.authenticateBrowserClient(context.Background(), client, token)
	if err == nil {
		t.Fatalf("expected mismatched browser user to be rejected")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleBrowserWakeInstanceIgnoresCanceledRequestContext(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wake-context-*")
	if err != nil {
		t.Fatalf("mkdir temp dir: %v", err)
	}
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/version/client-config":
			return testJSONResponse(t, http.StatusOK, map[string]any{
				"supabaseUrl":     "https://example.supabase.co",
				"supabaseAnonKey": "anon-key",
			}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/prepare-command-route":
			return testJSONResponse(t, http.StatusOK, map[string]any{
				"agentId": "550e8400-e29b-41d4-a716-446655440000",
				"task": map[string]any{
					"id":   "task-1",
					"name": "Wake task",
				},
			}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
			return testJSONResponse(t, http.StatusOK, []map[string]any{
				{
					"id":         "550e8400-e29b-41d4-a716-446655440000",
					"runtime_id": "runtime-1",
					"agent_runtime": map[string]any{
						"command_template": "printf 'hello\\n'",
					},
				},
			}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/task-1/checkpoints":
			return testJSONResponse(t, http.StatusOK, []map[string]any{}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return testJSONResponse(t, http.StatusOK, map[string]any{
				"id":                "instance-1",
				"manager_subdomain": "",
			}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			return testJSONResponse(t, http.StatusOK, map[string]any{
				"claimed": true,
			}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/update-task":
			return testJSONResponse(t, http.StatusOK, map[string]any{"ok": true}), nil
		default:
			return testJSONResponse(t, http.StatusNotFound, map[string]any{
				"error": fmt.Sprintf("unexpected path %s", r.URL.Path),
			}), nil
		}
	})

	m, err := newManager(config{
		WebBaseURL:       "http://localhost:3000",
		APIBaseURL:       "http://local.test",
		UserJWT:          "user-jwt",
		MaxConcurrent:    1,
		LogFlushInterval: 100 * time.Millisecond,
		StateFile:        filepath.Join(tempDir, "manager-state.json"),
		CPUThreshold:     80,
		SystemReserveMB:  1024,
		MBPerAgent:       1024,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: transport}
	m.state.ManagerID = "manager-1"
	t.Cleanup(func() {
		m.mu.Lock()
		workers := make([]*worker, 0, len(m.workers))
		for _, w := range m.workers {
			if w != nil {
				workers = append(workers, w)
			}
		}
		m.mu.Unlock()
		for _, w := range workers {
			if w.cmd != nil && w.cmd.Process != nil {
				_ = killManagedPID(w.cmd.Process.Pid)
				select {
				case <-w.done:
				case <-time.After(2 * time.Second):
					_, _ = w.cmd.Process.Wait()
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		_ = os.RemoveAll(tempDir)
	})

	client, readEvent := newTestBrowserClient(t)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := m.handleBrowserWakeInstance(canceledCtx, client, "message-1", "instance-1", "task-1"); err != nil {
		t.Fatalf("handleBrowserWakeInstance error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		event, err := readEvent(200 * time.Millisecond)
		if err != nil {
			continue
		}
		if got, _ := event["type"].(string); got != "wake_accepted" {
			continue
		}
		if got, _ := event["status"].(string); got != "accepted" {
			t.Fatalf("expected accepted wake event, got %#v", event)
		}
		return
	}

	t.Fatal("timed out waiting for wake_accepted event")
}

func TestShouldReuseReloadedSessionWhenForceRefreshAlreadyRotated(t *testing.T) {
	initial := storedSession{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    time.Now().Add(-time.Minute).Unix(),
	}
	reloaded := storedSession{
		AccessToken:  "new-access",
		RefreshToken: "new-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}

	if !shouldReuseReloadedSession(true, initial, reloaded) {
		t.Fatalf("expected reloaded rotated session to be reused")
	}
	if shouldReuseReloadedSession(false, initial, reloaded) {
		t.Fatalf("did not expect reuse when forceRefresh is false")
	}
}

func TestGetFreshStoredSessionReloadsAfterRefreshTokenRotation(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	initial := storedSession{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    time.Now().Add(-time.Hour).Unix(),
	}
	initialRaw, err := json.Marshal(initial)
	if err != nil {
		t.Fatalf("marshal initial session: %v", err)
	}
	if err := keyringSet(keyringService, keyringUser, string(initialRaw)); err != nil {
		t.Fatalf("seed keyring session: %v", err)
	}
	if err := writeFallbackSession(initialRaw); err != nil {
		t.Fatalf("seed fallback session: %v", err)
	}

	rotated := storedSession{
		AccessToken:  "rotated-access",
		RefreshToken: "rotated-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}

	mgr := &manager{}
	refreshCalls := 0
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		refreshCalls++
		if refreshCalls == 1 {
			rotatedRaw, _ := json.Marshal(rotated)
			if err := keyringSet(keyringService, keyringUser, string(rotatedRaw)); err != nil {
				t.Fatalf("update keyring session: %v", err)
			}
			if err := writeFallbackSession(rotatedRaw); err != nil {
				t.Fatalf("update fallback session: %v", err)
			}
			return storedSession{}, fmt.Errorf("refresh session failed: refresh_token_already_used")
		}
		return rotated, nil
	}

	session, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("getFreshStoredSession error: %v", err)
	}
	if session.AccessToken != rotated.AccessToken {
		t.Fatalf("expected rotated access token, got %s", session.AccessToken)
	}
	if refreshCalls != 1 {
		t.Fatalf("expected single refresh attempt, got %d", refreshCalls)
	}
}

func TestGetFreshStoredSessionCreatesCrossProcessRefreshLock(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	sessionSeed := storedSession{
		AccessToken:  "expiring-access",
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(time.Minute).Unix(),
	}
	sessionRaw, err := json.Marshal(sessionSeed)
	if err != nil {
		t.Fatalf("marshal session seed: %v", err)
	}
	if err := keyringSet(keyringService, keyringUser, string(sessionRaw)); err != nil {
		t.Fatalf("seed keyring session: %v", err)
	}
	if err := writeFallbackSession(sessionRaw); err != nil {
		t.Fatalf("seed fallback session: %v", err)
	}

	refreshed := storedSession{
		AccessToken:  "fresh-access",
		RefreshToken: "fresh-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	mgr := &manager{}
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		if refreshToken != sessionSeed.RefreshToken {
			t.Fatalf("expected refresh token %q, got %q", sessionSeed.RefreshToken, refreshToken)
		}
		return refreshed, nil
	}

	session, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("getFreshStoredSession error: %v", err)
	}
	if session.AccessToken != refreshed.AccessToken {
		t.Fatalf("expected refreshed access token %q, got %q", refreshed.AccessToken, session.AccessToken)
	}
	lockPath, err := sessionRefreshLockPath()
	if err != nil {
		t.Fatalf("sessionRefreshLockPath error: %v", err)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected refresh lock file at %s: %v", lockPath, err)
	}
}

func TestGetFreshStoredSessionBacksOffAfterRefreshRateLimit(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	sessionSeed := storedSession{
		AccessToken:  "still-valid-access",
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(2 * time.Minute).Unix(),
	}
	sessionRaw, err := json.Marshal(sessionSeed)
	if err != nil {
		t.Fatalf("marshal session seed: %v", err)
	}
	if err := keyringSet(keyringService, keyringUser, string(sessionRaw)); err != nil {
		t.Fatalf("seed keyring session: %v", err)
	}
	if err := writeFallbackSession(sessionRaw); err != nil {
		t.Fatalf("seed fallback session: %v", err)
	}

	mgr := &manager{}
	refreshCalls := 0
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		refreshCalls++
		return storedSession{}, fmt.Errorf(
			"refresh session failed: response status code 429: {\"code\":429,\"error_code\":\"over_request_rate_limit\",\"msg\":\"Request rate limit reached\"}",
		)
	}

	first, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("first getFreshStoredSession error: %v", err)
	}
	second, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("second getFreshStoredSession error: %v", err)
	}
	if first.AccessToken != sessionSeed.AccessToken {
		t.Fatalf("expected first access token %q, got %q", sessionSeed.AccessToken, first.AccessToken)
	}
	if second.AccessToken != sessionSeed.AccessToken {
		t.Fatalf("expected second access token %q, got %q", sessionSeed.AccessToken, second.AccessToken)
	}
	if refreshCalls != 1 {
		t.Fatalf("expected one refresh attempt during backoff window, got %d", refreshCalls)
	}
	if mgr.sessionRefreshRetryAt.IsZero() {
		t.Fatalf("expected refresh retry backoff to be recorded")
	}
}

func TestGetFreshStoredSessionClearsStaleAuthRequiredWhenSessionIsUsable(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL: "http://local.test",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	sessionSeed := storedSession{
		AccessToken:  "usable-access",
		RefreshToken: "usable-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	sessionRaw, err := json.Marshal(sessionSeed)
	if err != nil {
		t.Fatalf("marshal session seed: %v", err)
	}
	if err := keyringSet(keyringService, keyringUser, string(sessionRaw)); err != nil {
		t.Fatalf("seed keyring session: %v", err)
	}

	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("unexpected request while reusing usable session: %s", r.URL.Path)
	})}

	session, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("getFreshStoredSession error: %v", err)
	}
	if session.AccessToken != sessionSeed.AccessToken {
		t.Fatalf("expected usable session, got %#v", session)
	}
	if mgr.state.AuthRequired || mgr.state.ReauthSessionID != "" || mgr.state.ReauthCodeVerifier != "" {
		t.Fatalf("expected stale auth state to clear, got %#v", mgr.state)
	}
}

func TestGetFreshStoredSessionKeepsPendingReauthWhenAccessTokenStillWorks(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL: "http://local.test",
		StateFile:  stateFile,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-123"
	mgr.state.ReauthCodeVerifier = "verifier-123"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    4102441200,
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	sessionSeed := storedSession{
		AccessToken:  "still-valid-access",
		RefreshToken: "dead-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	sessionRaw, err := json.Marshal(sessionSeed)
	if err != nil {
		t.Fatalf("marshal session seed: %v", err)
	}
	if err := keyringSet(keyringService, keyringUser, string(sessionRaw)); err != nil {
		t.Fatalf("seed keyring session: %v", err)
	}

	session, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("getFreshStoredSession error: %v", err)
	}
	if session.AccessToken != sessionSeed.AccessToken {
		t.Fatalf("expected usable session, got %#v", session)
	}
	if !mgr.state.AuthRequired || mgr.state.ReauthSessionID != "login-123" || mgr.state.ReauthCodeVerifier != "verifier-123" {
		t.Fatalf("expected pending reauth state to persist, got %#v", mgr.state)
	}
}

func TestGetFreshStoredSessionBacksOffWhenRotatedTokenRetryHitsRateLimit(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	initial := storedSession{
		AccessToken:  "initial-access",
		RefreshToken: "stale-refresh",
		ExpiresAt:    time.Now().Add(2 * time.Minute).Unix(),
	}
	reloaded := storedSession{
		AccessToken:  "rotated-access",
		RefreshToken: "rotated-refresh",
		ExpiresAt:    time.Now().Add(2 * time.Minute).Unix(),
	}
	initialRaw, err := json.Marshal(initial)
	if err != nil {
		t.Fatalf("marshal initial session: %v", err)
	}
	if err := keyringSet(keyringService, keyringUser, string(initialRaw)); err != nil {
		t.Fatalf("seed keyring session: %v", err)
	}
	if err := writeFallbackSession(initialRaw); err != nil {
		t.Fatalf("seed fallback session: %v", err)
	}

	mgr := &manager{}
	refreshCalls := 0
	rateLimitErr := fmt.Errorf(
		"refresh session failed: response status code 429: {\"code\":429,\"error_code\":\"over_request_rate_limit\",\"msg\":\"Request rate limit reached\"}",
	)
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		refreshCalls++
		if refreshCalls == 1 {
			reloadedRaw, _ := json.Marshal(reloaded)
			if err := keyringSet(keyringService, keyringUser, string(reloadedRaw)); err != nil {
				t.Fatalf("update keyring session: %v", err)
			}
			if err := writeFallbackSession(reloadedRaw); err != nil {
				t.Fatalf("update fallback session: %v", err)
			}
			return storedSession{}, fmt.Errorf("refresh session failed: refresh_token_already_used")
		}
		if refreshToken != reloaded.RefreshToken {
			t.Fatalf("expected rotated refresh token %q, got %q", reloaded.RefreshToken, refreshToken)
		}
		return storedSession{}, rateLimitErr
	}

	session, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("getFreshStoredSession error: %v", err)
	}
	if session.AccessToken != reloaded.AccessToken {
		t.Fatalf("expected reloaded access token %q, got %q", reloaded.AccessToken, session.AccessToken)
	}
	if refreshCalls != 2 {
		t.Fatalf("expected two refresh attempts, got %d", refreshCalls)
	}
	if mgr.sessionRefreshRetryAt.IsZero() {
		t.Fatalf("expected retry backoff to be recorded after rotated-token rate limit")
	}
}

func TestGetFreshStoredSessionDoesNotReuseTokenOnForcedRefreshRateLimit(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		if keyringValue == "" {
			return "", fmt.Errorf("missing session")
		}
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	sessionSeed := storedSession{
		AccessToken:  "rejected-access",
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(2 * time.Minute).Unix(),
	}
	sessionRaw, err := json.Marshal(sessionSeed)
	if err != nil {
		t.Fatalf("marshal session seed: %v", err)
	}
	if err := keyringSet(keyringService, keyringUser, string(sessionRaw)); err != nil {
		t.Fatalf("seed keyring session: %v", err)
	}
	if err := writeFallbackSession(sessionRaw); err != nil {
		t.Fatalf("seed fallback session: %v", err)
	}

	mgr := &manager{}
	refreshCalls := 0
	refreshErr := fmt.Errorf(
		"refresh session failed: response status code 429: {\"code\":429,\"error_code\":\"over_request_rate_limit\",\"msg\":\"Request rate limit reached\"}",
	)
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		refreshCalls++
		return storedSession{}, refreshErr
	}

	_, err = mgr.getFreshStoredSession(context.Background(), true)
	if !errors.Is(err, refreshErr) {
		t.Fatalf("expected forced refresh to return refresh error, got %v", err)
	}
	if refreshCalls != 1 {
		t.Fatalf("expected single refresh attempt, got %d", refreshCalls)
	}
	if !mgr.sessionRefreshRetryAt.IsZero() {
		t.Fatalf("expected forced refresh to skip retry backoff")
	}
}

func TestGetFreshStoredSessionMarksAuthRequiredAndPersistsPendingReauthWhenRefreshFails(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:  "http://local.test",
		WebBaseURL:  "https://app.passiveagents.com",
		StateFile:   stateFile,
		MachineName: "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  "old-access",
		RefreshToken: "stale-refresh",
		ExpiresAt:    time.Now().Add(2 * time.Minute).Unix(),
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	authInitCalls := 0
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/auth/init" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode auth init payload: %v", err)
		}
		if _, ok := payload["challenge"].(string); !ok {
			t.Fatalf("expected challenge in auth init payload, got %#v", payload["challenge"])
		}
		if _, ok := payload["state"].(string); !ok {
			t.Fatalf("expected state token in auth init payload, got %#v", payload["state"])
		}
		if got := payload["port"]; got != float64(54321) {
			t.Fatalf("expected auth init port 54321, got %#v", got)
		}
		authInitCalls++
		return jsonHTTPResponse(200, `{"shortId":"login-123"}`), nil
	})}
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		if refreshToken != "stale-refresh" {
			t.Fatalf("unexpected refresh token: %q", refreshToken)
		}
		return storedSession{}, fmt.Errorf("refresh session failed: refresh_token_already_used")
	}

	_, err = mgr.getFreshStoredSession(context.Background(), false)
	if err == nil {
		t.Fatal("expected refresh failure")
	}
	if got := err.Error(); got != "refresh session failed: refresh token invalid or already used; run 'passiveagents login'" {
		t.Fatalf("unexpected refresh error: %q", got)
	}
	if authInitCalls != 1 {
		t.Fatalf("expected one auth init call, got %d", authInitCalls)
	}
	if !mgr.state.AuthRequired {
		t.Fatal("expected auth_required to be set")
	}
	if mgr.state.ReauthSessionID != "login-123" {
		t.Fatalf("expected reauth session id login-123, got %q", mgr.state.ReauthSessionID)
	}
	if strings.TrimSpace(mgr.state.ReauthCodeVerifier) == "" {
		t.Fatal("expected reauth code verifier to be persisted")
	}

	state, err := readState(stateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if !state.AuthRequired {
		t.Fatal("expected persisted auth_required to be true")
	}
	if state.ReauthSessionID != "login-123" {
		t.Fatalf("expected persisted reauth session id login-123, got %q", state.ReauthSessionID)
	}
	if strings.TrimSpace(state.ReauthCodeVerifier) == "" {
		t.Fatal("expected persisted reauth code verifier")
	}
}

func TestBeginManagerReauthBroadcastsExistingPendingStateToBrowserClient(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:  "http://local.test",
		WebBaseURL:  "https://app.passiveagents.com",
		StateFile:   stateFile,
		MachineName: "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-123"
	mgr.state.ReauthCodeVerifier = "verifier-123"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	client := &browserClient{
		outbound: make(chan map[string]any, 1),
	}
	mgr.browserMu.Lock()
	mgr.browserClients[client] = struct{}{}
	mgr.browserMu.Unlock()

	if err := mgr.beginManagerReauthWithHeartbeat(context.Background(), false); err != nil {
		t.Fatalf("beginManagerReauthWithHeartbeat error: %v", err)
	}

	select {
	case payload := <-client.outbound:
		if payload["type"] != "manager_auth_state" {
			t.Fatalf("expected manager_auth_state payload, got %#v", payload["type"])
		}
		if payload["reauth_required"] != true {
			t.Fatalf("expected reauth_required payload, got %#v", payload["reauth_required"])
		}
		if payload["reauth_session_id"] != "login-123" {
			t.Fatalf("expected reauth session id payload, got %#v", payload["reauth_session_id"])
		}
		if payload["reauth_url"] != "https://app.passiveagents.com/login-cli?id=login-123" {
			t.Fatalf("expected reauth url payload, got %#v", payload["reauth_url"])
		}
	case <-time.After(time.Second):
		t.Fatal("expected auth-required websocket event")
	}
}

func TestPollPendingReauthSessionStoresRotatedTokensAndClearsAuthRequired(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:  "http://local.test",
		StateFile:   stateFile,
		MachineName: "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-123"
	mgr.state.ReauthCodeVerifier = "verifier-123"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    4102441200,
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	newAccess := mustTestJWT(t, jwt.MapClaims{
		"sub": "user-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	pollCalls := 0
	syncCalls := 0
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/sessions/login-123/poll":
			if r.Method != http.MethodGet {
				t.Fatalf("unexpected poll method: %s", r.Method)
			}
			if got := r.URL.Query().Get("codeVerifier"); got != "verifier-123" {
				t.Fatalf("unexpected code verifier: %q", got)
			}
			pollCalls++
			return jsonHTTPResponse(200, fmt.Sprintf(
				`{"pending":false,"accessToken":%q,"refreshToken":"new-refresh","expiresAt":%d,"userId":"user-1"}`,
				newAccess,
				time.Now().Add(time.Hour).Unix(),
			)), nil
		case "/local-agent-managers/manager-1/session":
			if r.Method != http.MethodPost {
				t.Fatalf("unexpected sync method: %s", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer "+newAccess {
				t.Fatalf("unexpected sync authorization header: %q", got)
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode sync payload: %v", err)
			}
			if got := payload["refreshToken"]; got != "new-refresh" {
				t.Fatalf("unexpected synced refresh token: %#v", got)
			}
			syncCalls++
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		case "/api/managers/manager-1/heartbeat":
			if r.Method != http.MethodPatch {
				t.Fatalf("unexpected heartbeat method: %s", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer "+newAccess {
				t.Fatalf("unexpected heartbeat authorization header: %q", got)
			}
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer "+newAccess {
				t.Fatalf("unexpected Supabase user auth header: %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"user-1"}`), nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	if err := mgr.pollPendingReauthSession(context.Background()); err != nil {
		t.Fatalf("pollPendingReauthSession error: %v", err)
	}
	if pollCalls != 1 {
		t.Fatalf("expected one poll call, got %d", pollCalls)
	}
	if syncCalls != 1 {
		t.Fatalf("expected one post-reauth session sync, got %d", syncCalls)
	}
	if mgr.state.AuthRequired {
		t.Fatal("expected auth_required to be cleared")
	}
	if mgr.state.ReauthSessionID != "" {
		t.Fatalf("expected reauth session id to be cleared, got %q", mgr.state.ReauthSessionID)
	}
	if mgr.state.ReauthCodeVerifier != "" {
		t.Fatalf("expected reauth code verifier to be cleared, got %q", mgr.state.ReauthCodeVerifier)
	}
	if mgr.userID != "user-1" {
		t.Fatalf("expected user id to be updated, got %q", mgr.userID)
	}

	session, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession error: %v", err)
	}
	if session.AccessToken != newAccess || session.RefreshToken != "new-refresh" {
		t.Fatalf("expected rotated session to be stored, got %#v", session)
	}

	state, err := readState(stateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if state.AuthRequired {
		t.Fatal("expected persisted auth_required to be false")
	}
	if state.ReauthSessionID != "" || state.ReauthCodeVerifier != "" {
		t.Fatalf("expected persisted reauth state to be cleared, got %#v", state)
	}

	freshSession, err := mgr.getFreshStoredSession(context.Background(), false)
	if err != nil {
		t.Fatalf("getFreshStoredSession error: %v", err)
	}
	if freshSession.AccessToken != newAccess || freshSession.RefreshToken != "new-refresh" {
		t.Fatalf("expected stored session to resume normal use, got %#v", freshSession)
	}
}

func TestPollPendingReauthSessionRejectsMalformedAccessToken(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:      "http://local.test",
		SupabaseURL:     "http://supabase.test",
		SupabaseAnonKey: "anon-key",
		StateFile:       stateFile,
		MachineName:     "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-123"
	mgr.state.ReauthCodeVerifier = "verifier-123"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    4102441200,
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/sessions/login-123/poll":
			return jsonHTTPResponse(200, `{"pending":false,"accessToken":"not-a-jwt","refreshToken":"new-refresh","expiresAt":4102444800,"userId":"user-1"}`), nil
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer not-a-jwt" {
				t.Fatalf("unexpected Supabase user auth header: %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"different-user"}`), nil
		case "/local-agent-managers/manager-1/session":
			t.Fatalf("malformed reauth token must not be synced to backend")
			return nil, nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	err = mgr.pollPendingReauthSession(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid access token") {
		t.Fatalf("expected invalid access token error, got %v", err)
	}

	session, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession error: %v", err)
	}
	if session.AccessToken != "old-access" || session.RefreshToken != "old-refresh" {
		t.Fatalf("expected original session to remain stored, got %#v", session)
	}
	if !mgr.state.AuthRequired {
		t.Fatal("expected auth_required to remain set")
	}
}

func TestHandleBrowserSyncUserSessionStoresRotatedTokensAndClearsAuthRequired(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:      "http://local.test",
		SupabaseURL:     "http://supabase.test",
		SupabaseAnonKey: "anon-key",
		StateFile:       stateFile,
		MachineName:     "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-123"
	mgr.state.ReauthCodeVerifier = "verifier-123"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    time.Now().Add(2 * time.Minute).Unix(),
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	freshToken := mustTestJWT(t, jwt.MapClaims{
		"sub":   "user-1",
		"email": "user@example.com",
		"exp":   time.Now().Add(time.Hour).Unix(),
	})
	syncCalls := 0
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer "+freshToken {
				t.Fatalf("unexpected auth header: %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"user-1"}`), nil
		case "/api/managers/manager-1/heartbeat":
			if got := r.Header.Get("Authorization"); got != "Bearer "+freshToken {
				t.Fatalf("unexpected heartbeat authorization header: %q", got)
			}
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		case "/local-agent-managers/manager-1/session":
			if got := r.Header.Get("Authorization"); got != "Bearer "+freshToken {
				t.Fatalf("unexpected sync authorization header: %q", got)
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode sync payload: %v", err)
			}
			if got := payload["refreshToken"]; got != "browser-refresh" {
				t.Fatalf("unexpected synced refresh token: %#v", got)
			}
			syncCalls++
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	expiresAt := time.Now().Add(time.Hour).Unix()
	if err := mgr.handleBrowserSyncUserSession(context.Background(), freshToken, "browser-refresh", expiresAt, "user-1"); err != nil {
		t.Fatalf("handleBrowserSyncUserSession error: %v", err)
	}
	if syncCalls != 1 {
		t.Fatalf("expected one manager session sync, got %d", syncCalls)
	}
	if mgr.state.AuthRequired {
		t.Fatal("expected auth_required to be cleared")
	}
	if mgr.state.ReauthSessionID != "" || mgr.state.ReauthCodeVerifier != "" {
		t.Fatalf("expected reauth state to be cleared, got %#v", mgr.state)
	}
	session, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession error: %v", err)
	}
	if session.AccessToken != freshToken || session.RefreshToken != "browser-refresh" || session.UserID != "user-1" {
		t.Fatalf("expected browser session to be stored, got %#v", session)
	}
	state, err := readState(stateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if state.AuthRequired || state.ReauthSessionID != "" || state.ReauthCodeVerifier != "" {
		t.Fatalf("expected persisted reauth state to be cleared, got %#v", state)
	}
}

func TestPollPendingReauthSessionRejectsWrongAccountTokens(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:  "http://local.test",
		StateFile:   stateFile,
		MachineName: "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-123"
	mgr.state.ReauthCodeVerifier = "verifier-123"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    4102441200,
		UserID:       "manager-user",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	wrongAccess := mustTestJWT(t, jwt.MapClaims{
		"sub": "browser-user",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/sessions/login-123/poll":
			return jsonHTTPResponse(200, fmt.Sprintf(
				`{"pending":false,"accessToken":%q,"refreshToken":"wrong-refresh","expiresAt":%d,"userId":"browser-user"}`,
				wrongAccess,
				time.Now().Add(time.Hour).Unix(),
			)), nil
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			if got := r.Header.Get("Authorization"); got != "Bearer "+wrongAccess {
				t.Fatalf("unexpected Supabase user auth header: %q", got)
			}
			return jsonHTTPResponse(200, `{"id":"browser-user"}`), nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	err = mgr.pollPendingReauthSession(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not match the manager account") {
		t.Fatalf("expected account mismatch error, got %v", err)
	}

	session, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession error: %v", err)
	}
	if session.AccessToken != "old-access" || session.RefreshToken != "old-refresh" {
		t.Fatalf("expected original session to remain stored, got %#v", session)
	}
	if !mgr.state.AuthRequired {
		t.Fatal("expected auth_required to remain set")
	}
}

func TestPollPendingReauthSessionReinitializesAfterInvalidStoredPayload(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:  "http://local.test",
		StateFile:   stateFile,
		MachineName: "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-123"
	mgr.state.ReauthCodeVerifier = "verifier-123"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	authInitCalls := 0
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/sessions/login-123/poll":
			return jsonHTTPResponse(400, `{"message":"Stored session token payload is invalid","error":"Bad Request","statusCode":400}`), nil
		case "/auth/init":
			authInitCalls++
			return jsonHTTPResponse(200, `{"shortId":"login-456"}`), nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	if err := mgr.pollPendingReauthSession(context.Background()); err != nil {
		t.Fatalf("pollPendingReauthSession error: %v", err)
	}
	if authInitCalls != 1 {
		t.Fatalf("expected one auth init call, got %d", authInitCalls)
	}
	if !mgr.state.AuthRequired {
		t.Fatal("expected auth_required to remain set")
	}
	if mgr.state.ReauthSessionID != "login-456" {
		t.Fatalf("expected reauth session id login-456, got %q", mgr.state.ReauthSessionID)
	}
	if strings.TrimSpace(mgr.state.ReauthCodeVerifier) == "" {
		t.Fatal("expected replacement reauth code verifier")
	}
	if mgr.state.ReauthCodeVerifier == "verifier-123" {
		t.Fatal("expected replacement reauth code verifier to change")
	}
}

func TestPollPendingReauthSessionReinitializesAfterMissingSession(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:  "http://local.test",
		StateFile:   stateFile,
		MachineName: "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	mgr.state.AuthRequired = true
	mgr.state.ReauthSessionID = "login-dead"
	mgr.state.ReauthCodeVerifier = "verifier-dead"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	authInitCalls := 0
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/sessions/login-dead/poll":
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		case "/auth/init":
			authInitCalls++
			return jsonHTTPResponse(200, `{"shortId":"login-fresh"}`), nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	if err := mgr.pollPendingReauthSession(context.Background()); err != nil {
		t.Fatalf("pollPendingReauthSession error: %v", err)
	}
	if authInitCalls != 1 {
		t.Fatalf("expected one auth init call, got %d", authInitCalls)
	}
	if !mgr.state.AuthRequired {
		t.Fatal("expected auth_required to remain set")
	}
	if mgr.state.ReauthSessionID != "login-fresh" {
		t.Fatalf("expected reauth session id login-fresh, got %q", mgr.state.ReauthSessionID)
	}
	if strings.TrimSpace(mgr.state.ReauthCodeVerifier) == "" {
		t.Fatal("expected replacement reauth code verifier")
	}
	if mgr.state.ReauthCodeVerifier == "verifier-dead" {
		t.Fatal("expected replacement reauth code verifier to change")
	}
}

func TestRefreshManagerUserIdentityRefreshesWhenStoredAccessTokenIsInvalidBeforeExpiry(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:      "http://local.test",
		WebBaseURL:      "http://localhost:3000",
		SupabaseURL:     "http://supabase.test",
		SupabaseAnonKey: "anon-key",
		StateFile:       stateFile,
		MachineName:     "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	staleToken := mustTestJWT(t, jwt.MapClaims{
		"sub":   "user-1",
		"email": "stale@example.com",
		"exp":   time.Now().Add(10 * time.Minute).Unix(),
	})
	freshToken := mustTestJWT(t, jwt.MapClaims{
		"sub":   "user-fresh",
		"email": "fresh@example.com",
		"exp":   time.Now().Add(time.Hour).Unix(),
	})
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  staleToken,
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(10 * time.Minute).Unix(),
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	authInitCalls := 0
	refreshCalls := 0
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		if refreshToken != "refresh-token" {
			t.Fatalf("unexpected refresh token: %q", refreshToken)
		}
		refreshCalls++
		return storedSession{
			AccessToken:  freshToken,
			RefreshToken: "rotated-refresh",
			ExpiresAt:    time.Now().Add(time.Hour).Unix(),
			UserID:       "user-fresh",
			UserEmail:    "fresh@example.com",
		}, nil
	}
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			switch r.Header.Get("Authorization") {
			case "Bearer " + staleToken:
				return jsonHTTPResponse(401, `{"message":"invalid token"}`), nil
			case "Bearer " + freshToken:
				return jsonHTTPResponse(200, `{"id":"user-fresh"}`), nil
			default:
				t.Fatalf("unexpected auth header: %q", r.Header.Get("Authorization"))
				return nil, nil
			}
		case "/auth/init":
			authInitCalls++
			return jsonHTTPResponse(200, `{"shortId":"login-123"}`), nil
		case "/local-agent-managers/manager-1/session":
			if got := r.Header.Get("Authorization"); got != "Bearer "+freshToken {
				t.Fatalf("unexpected session sync authorization header: %q", got)
			}
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	if err := mgr.refreshManagerUserIdentity(context.Background()); err != nil {
		t.Fatalf("refreshManagerUserIdentity error: %v", err)
	}
	if refreshCalls != 1 {
		t.Fatalf("expected one forced refresh, got %d", refreshCalls)
	}
	if authInitCalls != 0 {
		t.Fatalf("expected no auth init call, got %d", authInitCalls)
	}
	if mgr.state.AuthRequired {
		t.Fatal("expected auth_required to stay false")
	}
	if mgr.userID != "user-fresh" {
		t.Fatalf("expected refreshed user id, got %q", mgr.userID)
	}
	session, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession error: %v", err)
	}
	if session.AccessToken != freshToken || session.RefreshToken != "rotated-refresh" {
		t.Fatalf("expected refreshed session to be stored, got %#v", session)
	}
	state, err := readState(stateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if state.AuthRequired {
		t.Fatal("expected persisted auth_required to stay false")
	}
}

func TestRefreshManagerUserIdentityMarksAuthRequiredWhenStoredAccessTokenIsInvalidAndRefreshFails(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	stateFile := filepath.Join(tempHome, "manager-state.json")
	mgr, err := newManager(config{
		APIBaseURL:      "http://local.test",
		WebBaseURL:      "http://localhost:3000",
		SupabaseURL:     "http://supabase.test",
		SupabaseAnonKey: "anon-key",
		StateFile:       stateFile,
		MachineName:     "regression-box",
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.state.ManagerID = "manager-1"
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}
	if err := mgr.saveSession(storedSession{
		AccessToken:  "wrong-backend-access",
		RefreshToken: "refresh-token",
		ExpiresAt:    time.Now().Add(10 * time.Minute).Unix(),
		UserID:       "user-1",
	}); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	authInitCalls := 0
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		if refreshToken != "refresh-token" {
			t.Fatalf("unexpected refresh token: %q", refreshToken)
		}
		return storedSession{}, fmt.Errorf("refresh session failed: refresh_token_already_used")
	}
	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/auth/v1/keys":
			return jsonHTTPResponse(500, `{"message":"jwks unavailable in test"}`), nil
		case "/auth/v1/user":
			return jsonHTTPResponse(401, `{"message":"invalid token"}`), nil
		case "/auth/init":
			authInitCalls++
			return jsonHTTPResponse(200, `{"shortId":"login-123"}`), nil
		case "/api/managers/manager-1/heartbeat":
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}

	err = mgr.refreshManagerUserIdentity(context.Background())
	if err == nil {
		t.Fatal("expected refresh failure")
	}
	if got := wrapRefreshTokenError(err).Error(); got != "refresh session failed: refresh token invalid or already used; run 'passiveagents login'" {
		t.Fatalf("unexpected refresh error: %q", got)
	}
	if authInitCalls != 1 {
		t.Fatalf("expected one auth init call, got %d", authInitCalls)
	}
	if !mgr.state.AuthRequired {
		t.Fatal("expected auth_required to be set")
	}
	if mgr.state.ReauthSessionID != "login-123" {
		t.Fatalf("expected reauth session id login-123, got %q", mgr.state.ReauthSessionID)
	}
	state, err := readState(stateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if !state.AuthRequired {
		t.Fatal("expected persisted auth_required")
	}
}

func TestManagerRequestJSONWrapsInvalidRefreshTokenErrorWhenRecoveryUnavailable(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	mgr, err := newManager(config{
		APIBaseURL: "http://local.test",
		StateFile:  filepath.Join(tempHome, "manager-state.json"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	sessionSeed := storedSession{
		AccessToken:  "old-access",
		RefreshToken: "stale-refresh",
		ExpiresAt:    time.Now().Add(2 * time.Minute).Unix(),
	}
	if err := mgr.saveSession(sessionSeed); err != nil {
		t.Fatalf("saveSession error: %v", err)
	}

	mgr.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/auth/init" {
			t.Fatalf("unexpected API request: %s", r.URL.Path)
		}
		return jsonHTTPResponse(200, `{"shortId":"login-123"}`), nil
	})}
	mgr.state.ManagerID = "manager-1"
	mgr.refreshSessionHook = func(ctx context.Context, refreshToken string) (storedSession, error) {
		return storedSession{}, fmt.Errorf(
			"refresh session failed: response status code 400: {\"code\":400,\"error_code\":\"validation_failed\",\"msg\":\"Refresh token is not valid\"}",
		)
	}

	err = mgr.managerRequestJSON(context.Background(), http.MethodGet, "/managers/tasks/agent-personas", nil, nil)
	if err == nil {
		t.Fatalf("expected managerRequestJSON error")
	}
	if got := err.Error(); got != "refresh session failed: refresh token invalid or already used; run 'passiveagents login'" {
		t.Fatalf("expected wrapped refresh error, got %q", got)
	}
}

func TestCleanCLIErrorPromptsLoginAgainForUnrecoverableAuthFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "wrapped refresh token failure",
			err:  fmt.Errorf("refresh session failed: refresh token invalid or already used; run 'passiveagents login'"),
		},
		{
			name: "missing stored session",
			err:  fmt.Errorf("no stored session; run 'passiveagents login' first"),
		},
		{
			name: "missing refresh token",
			err:  fmt.Errorf("missing refresh token; run login again"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cleanCLIError(tc.err)
			want := "Please login again by running 'passiveagents login'."
			if got != want {
				t.Fatalf("cleanCLIError() = %q, want %q", got, want)
			}
		})
	}
}

func TestLoadSessionPrefersCanonicalKeyringSession(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	origSet := keyringSet
	origGet := keyringGet
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	keyringSession := storedSession{
		AccessToken:  "keyring-access",
		RefreshToken: "keyring-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	keyringRaw, err := json.Marshal(keyringSession)
	if err != nil {
		t.Fatalf("marshal keyring session: %v", err)
	}
	keyringGet = func(service, user string) (string, error) {
		return string(keyringRaw), nil
	}
	keyringSet = func(service, user, value string) error {
		return nil
	}

	fallbackSession := storedSession{
		AccessToken:  "fallback-access",
		RefreshToken: "fallback-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	fallbackRaw, err := json.Marshal(fallbackSession)
	if err != nil {
		t.Fatalf("marshal fallback session: %v", err)
	}
	if err := writeStoredSession(fallbackRaw); err != nil {
		t.Fatalf("write stored session: %v", err)
	}

	mgr := &manager{}
	session, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession error: %v", err)
	}
	if session.AccessToken != keyringSession.AccessToken {
		t.Fatalf("expected keyring access token %q, got %q", keyringSession.AccessToken, session.AccessToken)
	}
	if session.RefreshToken != keyringSession.RefreshToken {
		t.Fatalf("expected keyring refresh token %q, got %q", keyringSession.RefreshToken, session.RefreshToken)
	}
}

func TestLoadSessionFallsBackWhenKeyringSessionIsMissing(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	origSet := keyringSet
	origGet := keyringGet
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	keyringGet = func(service, user string) (string, error) {
		return "", errors.New("secret not found in keyring")
	}
	keyringSet = func(service, user, value string) error {
		return nil
	}

	stored := storedSession{
		AccessToken:  "stored-access",
		RefreshToken: "stored-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	storedRaw, err := json.Marshal(stored)
	if err != nil {
		t.Fatalf("marshal stored session: %v", err)
	}
	if err := writeStoredSession(storedRaw); err != nil {
		t.Fatalf("write stored session: %v", err)
	}

	mgr := &manager{}
	session, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession error: %v", err)
	}
	if session.AccessToken != stored.AccessToken {
		t.Fatalf("expected stored access token %q, got %q", stored.AccessToken, session.AccessToken)
	}
	if session.RefreshToken != stored.RefreshToken {
		t.Fatalf("expected stored refresh token %q, got %q", stored.RefreshToken, session.RefreshToken)
	}
}

func TestLoadSessionDoesNotMaskKeyringErrorsWithFallbackData(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	origSet := keyringSet
	origGet := keyringGet
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	keyringGet = func(service, user string) (string, error) {
		return "", errors.New("keyring permission denied")
	}
	keyringSet = func(service, user, value string) error {
		return nil
	}

	stored := storedSession{
		AccessToken:  "stored-access",
		RefreshToken: "stored-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	storedRaw, err := json.Marshal(stored)
	if err != nil {
		t.Fatalf("marshal stored session: %v", err)
	}
	if err := writeStoredSession(storedRaw); err != nil {
		t.Fatalf("write stored session: %v", err)
	}

	_, err = (&manager{}).loadSession()
	if err == nil {
		t.Fatal("expected loadSession error")
	}
	if got := err.Error(); got != "keyring permission denied" {
		t.Fatalf("loadSession error = %q, want %q", got, "keyring permission denied")
	}
}

func TestSaveSessionKeepsFallbackMirrorWhenKeyringSucceeds(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	var keyringValue string
	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		return keyringValue, nil
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	legacyKeyPath := filepath.Join(tempHome, ".passiveagents", authKeyFileName)
	legacyKey := make([]byte, 32)
	if _, err := cryptorand.Read(legacyKey); err != nil {
		t.Fatalf("cryptorand.Read: %v", err)
	}
	if err := writeFileAtomic(legacyKeyPath, []byte(base64.RawStdEncoding.EncodeToString(legacyKey)), 0o600, 0o700); err != nil {
		t.Fatalf("writeFileAtomic legacy key: %v", err)
	}

	session := storedSession{
		AccessToken:  "fresh-access",
		RefreshToken: "fresh-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	mgr := &manager{}
	if err := mgr.saveSession(session); err != nil {
		t.Fatalf("saveSession: %v", err)
	}

	keyringSession, err := decodeStoredSession([]byte(keyringValue))
	if err != nil {
		t.Fatalf("decode keyring session: %v", err)
	}
	if keyringSession.RefreshToken != session.RefreshToken {
		t.Fatalf("expected keyring refresh token %q, got %q", session.RefreshToken, keyringSession.RefreshToken)
	}

	storedRaw, err := readStoredSession()
	if err != nil {
		t.Fatalf("readStoredSession: %v", err)
	}
	fallbackSession, err := decodeStoredSession(storedRaw)
	if err != nil {
		t.Fatalf("decodeStoredSession fallback: %v", err)
	}
	if fallbackSession.RefreshToken != session.RefreshToken {
		t.Fatalf("expected fallback refresh token %q, got %q", session.RefreshToken, fallbackSession.RefreshToken)
	}
	if _, err := os.Stat(legacyKeyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected legacy fallback key to be removed, got err=%v", err)
	}

	keyringGet = func(service, user string) (string, error) {
		return "", fmt.Errorf("keyring is not available")
	}

	loaded, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession fallback restore: %v", err)
	}
	if loaded.RefreshToken != session.RefreshToken {
		t.Fatalf("expected restored refresh token %q, got %q", session.RefreshToken, loaded.RefreshToken)
	}
}

func TestSaveSessionFallsBackWhenKeyringDBusUnavailable(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		return fmt.Errorf("dial unix /run/user/1000/bus: connect: no such file or directory")
	}
	keyringGet = func(service, user string) (string, error) {
		return "", fmt.Errorf("secret not found in keyring")
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	session := storedSession{
		AccessToken:  "stored-access",
		RefreshToken: "stored-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	mgr := &manager{}
	if err := mgr.saveSession(session); err != nil {
		t.Fatalf("saveSession: %v", err)
	}

	storedRaw, err := readStoredSession()
	if err != nil {
		t.Fatalf("readStoredSession: %v", err)
	}
	storedSession, err := decodeStoredSession(storedRaw)
	if err != nil {
		t.Fatalf("decodeStoredSession: %v", err)
	}
	if storedSession.RefreshToken != session.RefreshToken {
		t.Fatalf("expected stored refresh token %q, got %q", session.RefreshToken, storedSession.RefreshToken)
	}

	path, err := sessionFilePath()
	if err != nil {
		t.Fatalf("sessionFilePath: %v", err)
	}
	encryptedRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read encrypted fallback file: %v", err)
	}
	if bytes.Contains(encryptedRaw, []byte(session.AccessToken)) {
		t.Fatal("expected fallback file to avoid plaintext access token")
	}
	if bytes.Contains(encryptedRaw, []byte(session.RefreshToken)) {
		t.Fatal("expected fallback file to avoid plaintext refresh token")
	}
	legacyKeyPath := filepath.Join(tempHome, ".passiveagents", authKeyFileName)
	if _, err := os.Stat(legacyKeyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no legacy fallback key file, got err=%v", err)
	}
}

func TestSaveSessionCreatesLockedDownStoredSessionFile(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	origSet := keyringSet
	origGet := keyringGet
	keyringSet = func(service, user, value string) error {
		return fmt.Errorf("dial unix /run/user/1000/bus: connect: no such file or directory")
	}
	keyringGet = func(service, user string) (string, error) {
		return "", errors.New("secret not found in keyring")
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
	})

	session := storedSession{
		AccessToken:  "stored-access",
		RefreshToken: "stored-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	mgr := &manager{}
	if err := mgr.saveSession(session); err != nil {
		t.Fatalf("saveSession: %v", err)
	}

	path, err := sessionFilePath()
	if err != nil {
		t.Fatalf("sessionFilePath: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat stored session file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected stored session file permissions 0600, got %#o", got)
	}
}

func TestSaveSessionIgnoresStoredMirrorFailureAfterKeyringSuccess(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	origSet := keyringSet
	origGet := keyringGet
	origKeyMaterial := storedSessionKeyMaterial
	var keyringValue string
	keyringSet = func(service, user, value string) error {
		keyringValue = value
		return nil
	}
	keyringGet = func(service, user string) (string, error) {
		return keyringValue, nil
	}
	storedSessionKeyMaterial = func() ([]byte, error) {
		return nil, fmt.Errorf("stored session encryption unsupported on windows")
	}
	t.Cleanup(func() {
		keyringSet = origSet
		keyringGet = origGet
		storedSessionKeyMaterial = origKeyMaterial
	})

	session := storedSession{
		AccessToken:  "keyring-access",
		RefreshToken: "keyring-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	mgr := &manager{}
	if err := mgr.saveSession(session); err != nil {
		t.Fatalf("saveSession should succeed when keyring write succeeds, got %v", err)
	}

	loaded, err := mgr.loadSession()
	if err != nil {
		t.Fatalf("loadSession from keyring: %v", err)
	}
	if loaded.RefreshToken != session.RefreshToken {
		t.Fatalf("expected refresh token %q, got %q", session.RefreshToken, loaded.RefreshToken)
	}
}

func TestReadStoredSessionSupportsLegacyPlaintextFallback(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	legacySession := storedSession{
		AccessToken:  "legacy-access",
		RefreshToken: "legacy-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}
	legacyRaw, err := json.Marshal(legacySession)
	if err != nil {
		t.Fatalf("marshal legacy session: %v", err)
	}

	path, err := sessionFilePath()
	if err != nil {
		t.Fatalf("sessionFilePath: %v", err)
	}
	if err := writeFileAtomic(path, legacyRaw, 0o600, 0o700); err != nil {
		t.Fatalf("write legacy fallback file: %v", err)
	}

	storedRaw, err := readStoredSession()
	if err != nil {
		t.Fatalf("readStoredSession: %v", err)
	}
	storedSession, err := decodeStoredSession(storedRaw)
	if err != nil {
		t.Fatalf("decodeStoredSession: %v", err)
	}
	if storedSession.AccessToken != legacySession.AccessToken {
		t.Fatalf("expected legacy access token %q, got %q", legacySession.AccessToken, storedSession.AccessToken)
	}
	if storedSession.RefreshToken != legacySession.RefreshToken {
		t.Fatalf("expected legacy refresh token %q, got %q", legacySession.RefreshToken, storedSession.RefreshToken)
	}
}

func TestCleanPromptContextTextDropsLifecycleAndStatusNoise(t *testing.T) {
	raw := strings.Join([]string{
		"# Lifecycle Signals",
		"When you complete the task, output EXACTLY this line on its own:",
		"[TASK_COMPLETED] <one-paragraph summary of what you did>",
		"gpt-5.4 xhigh · 100% left · ~/repos/project",
		"Pasted Content 1020 chars",
		"Keep responses concise and actionable.",
		"Please fix the navbar alignment and update the tests.",
	}, "\n")

	cleaned := cleanPromptContextText(raw)
	if cleaned != "Please fix the navbar alignment and update the tests." {
		t.Fatalf("unexpected cleaned prompt context: %q", cleaned)
	}
}

func TestWriteAutomatedPromptDoesNotCreateUserTranscriptEntry(t *testing.T) {
	w := &worker{
		input: nopWriteCloser{Writer: &bytes.Buffer{}},
		logs:  make(chan logEntry, 2),
	}
	mgr := &manager{}

	if err := mgr.writeAutomatedPrompt(w, "internal bootstrap prompt"); err != nil {
		t.Fatalf("writeAutomatedPrompt error: %v", err)
	}
	select {
	case entry := <-w.logs:
		t.Fatalf("expected no logged bootstrap entry, got %#v", entry)
	default:
	}

	if err := mgr.writeAgentInputLine(w, "Hi Fran"); err != nil {
		t.Fatalf("writeAgentInputLine error: %v", err)
	}
	select {
	case entry := <-w.logs:
		if entry.LogType != "user" || entry.Line != "Hi Fran" {
			t.Fatalf("unexpected logged entry: %#v", entry)
		}
	default:
		t.Fatalf("expected user log entry after writeAgentInputLine")
	}
}

func TestWriteAutomatedPromptUsesCarriageReturnForGeminiPTY(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		input:          nopWriteCloser{Writer: &input},
		logs:           make(chan logEntry, 1),
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
	}
	mgr := &manager{}

	if err := mgr.writeAutomatedPrompt(w, "continue the task"); err != nil {
		t.Fatalf("writeAutomatedPrompt error: %v", err)
	}

	if got, want := input.String(), "\x15continue the task\r"; got != want {
		t.Fatalf("unexpected Gemini PTY bootstrap bytes: got %q want %q", got, want)
	}
}

func TestWriteAutomatedPromptUsesCarriageReturnForNonGeminiPTY(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		input:          nopWriteCloser{Writer: &input},
		logs:           make(chan logEntry, 1),
		usesPTY:        true,
		runtimeCommand: "codex --profile default",
	}
	mgr := &manager{}

	if err := mgr.writeAutomatedPrompt(w, "continue the task"); err != nil {
		t.Fatalf("writeAutomatedPrompt error: %v", err)
	}

	if got, want := input.String(), "\x15continue the task\r"; got != want {
		t.Fatalf("unexpected non-Gemini PTY bootstrap bytes: got %q want %q", got, want)
	}
}

func TestSubmitAgentInputUsesCarriageReturnForGeminiPTY(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		input:          nopWriteCloser{Writer: &input},
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
	}
	mgr := &manager{}

	if err := mgr.submitAgentInput(w); err != nil {
		t.Fatalf("submitAgentInput error: %v", err)
	}

	if got, want := input.String(), "\r"; got != want {
		t.Fatalf("unexpected Gemini PTY submit bytes: got %q want %q", got, want)
	}
}

func TestBootstrapPromptDelayForCommandUsesLongerDelayForGemini(t *testing.T) {
	oldDelay := bootstrapPromptDelay
	oldGeminiDelay := geminiBootstrapDelay
	bootstrapPromptDelay = 40 * time.Millisecond
	geminiBootstrapDelay = 70 * time.Millisecond
	t.Cleanup(func() {
		bootstrapPromptDelay = oldDelay
		geminiBootstrapDelay = oldGeminiDelay
	})

	if got, want := bootstrapPromptDelayForCommand("codex --profile default"), 40*time.Millisecond; got != want {
		t.Fatalf("unexpected non-Gemini bootstrap delay: got %s want %s", got, want)
	}
	if got, want := bootstrapPromptDelayForCommand("gemini --model auto"), 70*time.Millisecond; got != want {
		t.Fatalf("unexpected Gemini bootstrap delay: got %s want %s", got, want)
	}
}

func TestSupportsStartupBootstrapPrompt(t *testing.T) {
	if !supportsStartupBootstrapPrompt("gemini --model auto") {
		t.Fatal("expected Gemini to support startup bootstrap prompts")
	}
	if !supportsStartupBootstrapPrompt("codex --profile default") {
		t.Fatal("expected Codex to support startup bootstrap prompts")
	}
	if !supportsStartupBootstrapPrompt("claude --dangerously-skip-permissions") {
		t.Fatal("expected Claude to support startup bootstrap prompts")
	}
	if !supportsStartupBootstrapPrompt("opencode --continue") {
		t.Fatal("expected OpenCode to support startup bootstrap prompts")
	}
}

func TestCommandWithStartupBootstrapPromptFormatsGeminiInteractivePrompt(t *testing.T) {
	got := commandWithStartupBootstrapPrompt("gemini --model auto", "Read TASK_CONTEXT.md")
	want := "gemini --model auto -i 'Read TASK_CONTEXT.md'"
	if runtime.GOOS == "windows" {
		want = `gemini --model auto -i "Read TASK_CONTEXT.md"`
	}
	if got != want {
		t.Fatalf("unexpected Gemini startup command: got %q want %q", got, want)
	}
}

func TestCommandWithStartupBootstrapPromptFormatsCodexPromptArgument(t *testing.T) {
	got := commandWithStartupBootstrapPrompt("codex --profile default", "Fix the navbar")
	want := "codex --profile default 'Fix the navbar'"
	if runtime.GOOS == "windows" {
		want = `codex --profile default "Fix the navbar"`
	}
	if got != want {
		t.Fatalf("unexpected Codex startup command: got %q want %q", got, want)
	}
}

func TestCommandWithStartupBootstrapPromptFormatsOpenCodePromptFlag(t *testing.T) {
	got := commandWithStartupBootstrapPrompt("opencode --continue", "Review TASK_CONTEXT.md")
	want := "opencode --continue --prompt 'Review TASK_CONTEXT.md'"
	if runtime.GOOS == "windows" {
		want = `opencode --continue --prompt "Review TASK_CONTEXT.md"`
	}
	if got != want {
		t.Fatalf("unexpected OpenCode startup command: got %q want %q", got, want)
	}
}

func TestCommandWithStartupBootstrapPromptFormatsClaudePromptArgument(t *testing.T) {
	got := commandWithStartupBootstrapPrompt("claude --print", "Summarize today's work")
	want := "claude --print 'Summarize today'\"'\"'s work'"
	if runtime.GOOS == "windows" {
		want = `claude --print "Summarize today's work"`
	}
	if got != want {
		t.Fatalf("unexpected Claude startup command: got %q want %q", got, want)
	}
}

func TestWaitForAgentReadyAcceptsGeminiSnapshot(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "gemini --model auto",
	}
	w.outputBuffer.Write("Gemini CLI\nType your message or @path\n› ")

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected Gemini snapshot to count as ready, got %v", err)
	}
}

func TestWorkerPromptStatusForRuntimeTreatsGeminiTrustPromptAsReadyWithDraft(t *testing.T) {
	status := workerPromptStatusForRuntime("gemini --model auto", strings.Join([]string{
		"> You are in /home/karthik/.passiveagents/tasks/1cc5e843",
		"Do you trust the contents of this directory? Working",
		"with untrusted contents comes with higher risk of",
		"prompt injection.",
		"› 1. Yes, continue",
		"2. No, quit",
		"Press enter to continue",
	}, "\n"))

	if status.State != workerPromptStateReadyWithDraft {
		t.Fatalf("expected Gemini trust prompt to look ready-with-draft, got %q", status.State)
	}
}

func TestWorkerPromptStatusForRuntimeTreatsGeminiShellPromptAsBusy(t *testing.T) {
	status := workerPromptStatusForRuntime("gemini --model auto", strings.Join([]string{
		"karthik@machine:~/repos/PassiveAgents$",
	}, "\n"))

	if status.State != workerPromptStateBusy {
		t.Fatalf("expected Gemini shell prompt to stay busy, got %q", status.State)
	}
}

func TestDeferredBootstrapReasonForWorkerDoesNotSpecialCaseGeminiTrustPrompt(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		runtimeCommand: "gemini --model auto",
	}
	w.outputBuffer.Write(strings.Join([]string{
		"> You are in /home/karthik/.passiveagents/tasks/1cc5e843",
		"Do you trust the contents of this directory? Working",
		"with untrusted contents comes with higher risk of",
		"prompt injection.",
		"› 1. Yes, continue",
		"2. No, quit",
		"Press enter to continue",
	}, "\n"))

	reason, ok := deferredBootstrapReasonForWorker(w)
	if ok {
		t.Fatalf("expected no Gemini trust-specific bootstrap deferral, got %q", reason)
	}
}

func TestDeferredBootstrapReasonForWorkerDoesNotSpecialCaseGeminiConfirmationPrompt(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		runtimeCommand: "gemini --model auto",
	}
	w.outputBuffer.Write(strings.Join([]string{
		"Gemini CLI",
		"Allow execution?",
		"› 1. Allow once",
		"2. Allow always",
		"3. Deny",
		"Press enter to continue",
	}, "\n"))

	reason, ok := deferredBootstrapReasonForWorker(w)
	if ok {
		t.Fatalf("expected no Gemini confirmation-specific bootstrap deferral, got %q", reason)
	}
}

func TestWaitForAgentReadyAcceptsGeminiConfirmationPrompt(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "gemini --model auto",
	}
	w.outputBuffer.Write(strings.Join([]string{
		"Gemini CLI",
		"Allow execution?",
		"› 1. Allow once",
		"2. Allow always",
		"3. Deny",
		"Press enter to continue",
	}, "\n"))
	mgr := &manager{}

	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected Gemini confirmation prompt to count as ready, got %v", err)
	}
}

func TestWaitForAgentReadyAcceptsCodexSnapshot(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "codex --profile default",
	}
	w.outputBuffer.Write("Codex\nEsc to interrupt\n› ")

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected Codex snapshot to count as ready, got %v", err)
	}
}

func TestWaitForAgentReadyAcceptsOpenCodeSnapshot(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "opencode",
	}
	w.outputBuffer.Write("OpenCode\nEsc to interrupt\n› ")

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected OpenCode snapshot to count as ready, got %v", err)
	}
}

func TestWaitForAgentReadyAcceptsGeminiSnapshotWithDraft(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "gemini --model auto",
	}
	w.outputBuffer.Write(strings.Join([]string{
		"Gemini CLI",
		"› Read .passiveagents/AGENT_PERSONA.md and continue the task.",
	}, "\n"))

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected Gemini draft snapshot to count as ready, got %v", err)
	}
}

func TestWaitForAgentReadyAcceptsOpenCodeSnapshotWithDraft(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "opencode",
	}
	w.outputBuffer.Write(strings.Join([]string{
		"OpenCode",
		"Esc to interrupt",
		"› Continue the task",
	}, "\n"))

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected OpenCode draft snapshot to count as ready, got %v", err)
	}
}

func TestWorkerPromptStatusForWorkerPrefersRawGeminiReadySnapshotOverStaleTerminalState(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		runtimeCommand: "gemini --model auto",
	}
	w.outputBuffer.Write("Gemini CLI\nType your message or @path\n› ")
	w.terminalState.Write("karthik@machine:~/repos/PassiveAgents$ ")

	status := workerPromptStatusForWorker(w)
	if status.State != workerPromptStateReadyEmpty {
		t.Fatalf("expected raw Gemini ready snapshot to win over stale terminal state, got %q snapshot=%q", status.State, status.Snapshot)
	}
}

func TestWorkerPromptStatusForWorkerPrefersRawOpenCodeReadySnapshotOverStaleTerminalState(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		runtimeCommand: "opencode",
	}
	w.outputBuffer.Write("OpenCode\nEsc to interrupt\n› ")
	w.terminalState.Write("karthik@machine:~/repos/PassiveAgents$ ")

	status := workerPromptStatusForWorker(w)
	if status.State != workerPromptStateReadyEmpty {
		t.Fatalf("expected raw OpenCode ready snapshot to win over stale terminal state, got %q snapshot=%q", status.State, status.Snapshot)
	}
}

func TestWorkerPromptStatusForWorkerPreservesOpenCodeResumeChooserFromRawSnapshot(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		terminalState:  newVTScreenState(80, 24),
		runtimeCommand: "opencode",
	}
	w.outputBuffer.Write("OpenCode\nSession   Previous session\n  Continue  opencode -s ses_2c50dda34ffead9scs2rFuc7LN\n› ")
	w.terminalState.Write("OpenCode\nEsc to interrupt\n› ")

	status := workerPromptStatusForWorker(w)
	if status.State != workerPromptStateBusy {
		t.Fatalf("expected raw OpenCode resume chooser to keep worker busy, got %q snapshot=%q", status.State, status.Snapshot)
	}
}

func TestWaitForAgentReadyAcceptsVisibleOpenCodeResumeChooserSnapshot(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "opencode",
	}
	w.outputBuffer.Write("OpenCode\nSession   Manager handles pre-created task worktree issue\n  Continue  opencode -s ses_2c50dda34ffead9scs2rFuc7LN\n› ")

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected visible OpenCode resume chooser snapshot to count as attached, got %v", err)
	}
}

func TestWaitForAgentReadyAcceptsVisibleOpenCodeResumeCommandSnapshotWithoutChooserCopy(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "opencode",
	}
	w.outputBuffer.Write("OpenCode\nManager handles pre-created task worktree issue\nopencode -s ses_2c50dda34ffead9scs2rFuc7LN\n› ")

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected visible OpenCode resume command snapshot to count as attached, got %v", err)
	}
}

func TestExtractOpenCodeResumeCommandFindsSessionHint(t *testing.T) {
	snapshot := strings.Join([]string{
		"OpenCode",
		"Previous session available.",
		"Run opencode -s ses_2c50dda34ffead9scs2rFuc7LN to resume it.",
		"› ",
	}, "\n")

	got, ok := extractOpenCodeResumeCommand(snapshot)
	if !ok {
		t.Fatal("expected OpenCode resume command to be detected")
	}
	want := "opencode -s ses_2c50dda34ffead9scs2rFuc7LN"
	if got != want {
		t.Fatalf("unexpected OpenCode resume command: got %q want %q", got, want)
	}
}

func TestSpawnWorkerKeepsOpenCodeResumeHintVisibleWithoutRelaunch(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			return jsonHTTPResponse(200, `{"claimed":true}`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	tempDir, err := os.MkdirTemp("", "pa-opencode-resume-hint-*")
	if err != nil {
		t.Fatalf("mkdir tempDir: %v", err)
	}
	markerPath := filepath.Join(tempDir, "resume-command.txt")
	opencodeDir := filepath.Join(tempDir, "bin")
	if err := os.MkdirAll(opencodeDir, 0o755); err != nil {
		t.Fatalf("mkdir opencodeDir: %v", err)
	}
	opencodePath := filepath.Join(opencodeDir, "opencode")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "-s" ] && [ "$2" = "ses_2c50dda34ffead9scs2rFuc7LN" ]; then
  printf '%%s %%s' "$1" "$2" > %q
  exit 0
fi
trap 'exit 0' TERM
printf 'OpenCode\nSession   Manager handles pre-created task worktree issue\n  Continue  opencode -s ses_2c50dda34ffead9scs2rFuc7LN\n› '
while true; do sleep 1; done
`, markerPath)
	if err := os.WriteFile(opencodePath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode: %v", err)
	}
	t.Setenv("PATH", opencodeDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	m, err := newManager(config{
		WebBaseURL:       "http://localhost:3000",
		APIBaseURL:       "http://local.test",
		UserJWT:          "user-jwt",
		MaxConcurrent:    1,
		LogFlushInterval: 100 * time.Millisecond,
		StateFile:        filepath.Join(tempDir, "manager-state.json"),
		CPUThreshold:     80,
		SystemReserveMB:  1024,
		MBPerAgent:       1024,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: transport}
	m.state.ManagerID = "manager-1"
	t.Cleanup(func() {
		m.mu.Lock()
		workers := make([]*worker, 0, len(m.workers))
		for _, w := range m.workers {
			if w != nil {
				workers = append(workers, w)
			}
		}
		m.mu.Unlock()
		for _, w := range workers {
			if w.cmd != nil && w.cmd.Process != nil {
				_ = killManagedPID(w.cmd.Process.Pid)
			}
		}
		time.Sleep(200 * time.Millisecond)
		_ = os.RemoveAll(tempDir)
	})

	err = m.spawnWorker(
		context.Background(),
		apiAgentPersona{
			ID:        "agent-1",
			RuntimeID: "runtime-1",
			AgentRuntime: struct {
				Provider        string `json:"provider"`
				CommandTemplate string `json:"command_template"`
			}{
				CommandTemplate: "opencode",
			},
		},
		apiTask{ID: "task-1", Name: "Resume OpenCode task"},
		1,
		"instance-1",
	)
	if err != nil {
		t.Fatalf("spawnWorker error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		w := m.workers["instance-1"]
		m.mu.Unlock()
		if w != nil {
			if got := strings.TrimSpace(w.runtimeCommand); got != "opencode" {
				t.Fatalf("unexpected runtime command after resume hint: %q", got)
			}
			if _, err := os.Stat(markerPath); err == nil {
				t.Fatal("expected manager not to relaunch opencode with a saved session command")
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stat marker: %v", err)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for OpenCode resume hint worker to stay on bare opencode")
}

func TestWaitForAgentReadyAcceptsClaudeSnapshot(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		runtimeCommand: "claude",
	}
	w.outputBuffer.Write("Claude Code\nEsc to interrupt\n› ")

	mgr := &manager{}
	if err := mgr.waitForAgentReady(context.Background(), w, 500*time.Millisecond); err != nil {
		t.Fatalf("expected Claude snapshot to count as ready, got %v", err)
	}
}

func TestWorkerPromptStatusForRuntimeDistinguishesReadyEmptyAndReadyWithDraft(t *testing.T) {
	readyEmpty := workerPromptStatusForRuntime("gemini --model auto", strings.Join([]string{
		"Gemini CLI",
		"Type your message or @path",
		"› ",
	}, "\n"))
	if readyEmpty.State != workerPromptStateReadyEmpty {
		t.Fatalf("expected ready-empty Gemini state, got %q", readyEmpty.State)
	}
	if readyEmpty.Draft != "" {
		t.Fatalf("expected no Gemini draft for ready-empty state, got %q", readyEmpty.Draft)
	}

	readyWithDraft := workerPromptStatusForRuntime("gemini --model auto", strings.Join([]string{
		"Gemini CLI",
		"› Read .passiveagents/AGENT_PERSONA.md, .passiveagents/lessons.jsonl, and",
		".passiveagents/TASK_CONTEXT.md in the current workspace, then continue",
		"the task and follow the lifecycle signals in .passiveagents/TASK_CONTEXT.md.",
	}, "\n"))
	if readyWithDraft.State != workerPromptStateReadyWithDraft {
		t.Fatalf("expected ready-with-draft Gemini state, got %q", readyWithDraft.State)
	}
	if !strings.Contains(readyWithDraft.Draft, ".passiveagents/TASK_CONTEXT.md") {
		t.Fatalf("expected wrapped Gemini draft to include the task context path, got %q", readyWithDraft.Draft)
	}
}

func TestApplyBrowserTerminalInputWritesRawBytesToWorker(t *testing.T) {
	var input bytes.Buffer
	mgr := &manager{
		workers: map[string]*worker{
			"instance-1": {
				input:     nopWriteCloser{Writer: &input},
				startedAt: time.Now().Add(-workerIdleShutdownThreshold - time.Second),
			},
		},
	}

	mgr.applyBrowserTerminalInput("instance-1", "\x1bhello\r")

	if got := input.String(); got != "\x1bhello\r" {
		t.Fatalf("unexpected raw terminal input: %q", got)
	}
	if lastActivityAt := mgr.workers["instance-1"].lastActivityOrStartAt(); time.Since(lastActivityAt) > time.Second {
		t.Fatalf("expected browser terminal input to mark recent activity, got %s", lastActivityAt)
	}
}

func TestApplyBrowserTerminalInputAllowsPTYInputBeforeBootstrapObserved(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		instanceID:     "instance-1",
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
		input:          nopWriteCloser{Writer: &input},
	}
	w.livestreamReady.Store(true)

	mgr := &manager{
		workers: map[string]*worker{
			"instance-1": w,
		},
	}

	mgr.applyBrowserTerminalInput("instance-1", "typed")
	if got := input.String(); got != "typed" {
		t.Fatalf("expected PTY input to pass through before bootstrap observation, got %q", got)
	}
}

func TestHandleAttachClientInputMarksActivity(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		instanceID:     "instance-1",
		input:          nopWriteCloser{Writer: &input},
		terminalState:  newVTScreenState(80, 24),
		attachClients:  map[net.Conn]struct{}{},
		startedAt:      time.Now().Add(-workerIdleShutdownThreshold - time.Second),
		runtimeCommand: "gemini --model auto",
	}
	mgr := &manager{}

	server, client := net.Pipe()
	w.attachClients[server] = struct{}{}
	done := make(chan struct{})
	go func() {
		mgr.handleAttachClientInput(w, server)
		close(done)
	}()

	if _, err := fmt.Fprintf(client, "%s 80 24\ntyped", attachHandshakePrefix); err != nil {
		t.Fatalf("write attach input: %v", err)
	}
	_ = client.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for attach input handler")
	}
	if got := input.String(); got != "typed" {
		t.Fatalf("expected attach input to pass through, got %q", got)
	}
	if lastActivityAt := w.lastActivityOrStartAt(); time.Since(lastActivityAt) > time.Second {
		t.Fatalf("expected attach input to mark recent activity, got %s", lastActivityAt)
	}
}

func TestScanLifecycleMarkersDoesNotMarkWorkerLivestreamReadyWhenUserInputIsNeeded(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/update-task" {
			return jsonHTTPResponse(200, `{"ok":true}`), nil
		}
		return jsonHTTPResponse(404, `{"error":"not found"}`), nil
	})

	w := &worker{
		instanceID:     "instance-1",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
		done:           make(chan struct{}),
		usesPTY:        true,
		runtimeCommand: "gemini --model auto",
	}
	chunk := "[NEEDS_USER_INPUT] Which project should I update?\n"
	w.outputBuffer.Write(chunk)
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			UserJWT:    "token",
		},
		client: &http.Client{Transport: transport},
	}

	mgr.scanLifecycleMarkers(context.Background(), w, chunk)

	if w.livestreamReady.Load() {
		t.Fatal("expected NEEDS_USER_INPUT marker to avoid marking the worker as livestream-ready")
	}
}

func TestTerminalReplayBufferTrimsOldestChunks(t *testing.T) {
	buffer := &terminalReplayBuffer{maxBytes: 6}
	buffer.AppendOutput(1, 1, []byte("abc"))
	buffer.AppendOutput(2, 2, []byte("def"))
	buffer.AppendOutput(3, 3, []byte("ghi"))

	snapshot := buffer.Snapshot(3)
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 chunks after trim, got %d", len(snapshot))
	}
	if snapshot[0].eventID != 2 || snapshot[1].eventID != 3 {
		t.Fatalf("unexpected trimmed sequences: %#v", snapshot)
	}
}

func TestTerminalReplayBufferSnapshotRespectsEndSequence(t *testing.T) {
	buffer := &terminalReplayBuffer{maxBytes: 64}
	buffer.AppendOutput(1, 1, []byte("abc"))
	buffer.AppendResize(2, 120, 40)
	buffer.AppendOutput(3, 2, []byte("def"))
	buffer.AppendOutput(4, 3, []byte("ghi"))

	snapshot := buffer.Snapshot(2)
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 chunks up to end sequence, got %d", len(snapshot))
	}
	if snapshot[0].kind != "output" || snapshot[1].kind != "resize" {
		t.Fatalf("unexpected snapshot kinds: %#v", snapshot)
	}
}

func TestWriteStateReadStateRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, "state", "manager-state.json")

	input := persistedState{
		ManagerID: "manager-1",
		ExpiresAt: "2026-01-01T00:00:00Z",
		Instances: map[string]persistedWorker{
			"inst-1": {
				PID:       1234,
				TaskID:    "task-1",
				AgentID:   "agent-1",
				SessionID: "session-1",
			},
		},
	}

	if err := writeState(statePath, input); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	output, err := readState(statePath)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}

	if output.ManagerID != input.ManagerID {
		t.Fatalf("manager id mismatch: got %s want %s", output.ManagerID, input.ManagerID)
	}
	got, ok := output.Instances["inst-1"]
	if !ok {
		t.Fatalf("expected instance inst-1")
	}
	if got.PID != 1234 || got.TaskID != "task-1" || got.AgentID != "agent-1" || got.SessionID != "session-1" {
		t.Fatalf("unexpected instance payload: %#v", got)
	}
}

func TestLoadConfigIgnoresPersistedLoopbackBaseURLs(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	statePath := filepath.Join(tempHome, ".passiveagents", "manager-state.json")
	if err := os.MkdirAll(filepath.Dir(statePath), 0o755); err != nil {
		t.Fatalf("MkdirAll error: %v", err)
	}
	if err := os.WriteFile(statePath, []byte(`{
		"web_base_url":"http://localhost:3000",
		"api_base_url":"http://127.0.0.1:8080"
	}`), 0o600); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	cfg := loadConfig()
	if cfg.WebBaseURL != defaultWebBaseURL {
		t.Fatalf("expected default web base URL %q, got %q", defaultWebBaseURL, cfg.WebBaseURL)
	}
	if cfg.APIBaseURL != defaultAPIBaseURL {
		t.Fatalf("expected default API base URL %q, got %q", defaultAPIBaseURL, cfg.APIBaseURL)
	}
}

func TestReconcileLocalStateKeepsRunningInstancesFromPreviousManager(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sleep helper process setup is unix-specific")
	}

	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = killManagedPID(cmd.Process.Pid)
			_, _ = cmd.Process.Wait()
		}
	})

	statePath := filepath.Join(t.TempDir(), "manager-state.json")
	mgr := &manager{
		cfg: config{StateFile: statePath},
		state: persistedState{
			Instances: map[string]persistedWorker{
				"instance-1": {
					PID:       cmd.Process.Pid,
					TaskID:    "task-1",
					AgentID:   "agent-1",
					SessionID: "session-1",
					Status:    "working",
				},
			},
		},
		previousManagerPID: os.Getpid() + 1,
		logger:             slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	if err := writeState(statePath, mgr.state); err != nil {
		t.Fatalf("write state: %v", err)
	}

	mgr.reconcileLocalState()

	if len(mgr.state.Instances) != 1 {
		t.Fatalf("expected previous-manager instances to be preserved, got %#v", mgr.state.Instances)
	}

	saved, err := readState(statePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if len(saved.Instances) != 1 {
		t.Fatalf("expected saved state to preserve running instances, got %#v", saved.Instances)
	}
}

func TestRecoverRestartableWorkersRestartsDeadPersistedWorkerInSavedWorkingDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell helper process setup is unix-specific")
	}

	tempDir, err := os.MkdirTemp("", "pa-restartable-workers-*")
	if err != nil {
		t.Fatalf("mkdir tempDir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	savedWorkingDir := filepath.Join(tempDir, "saved-worktree")
	if err := os.MkdirAll(savedWorkingDir, 0o755); err != nil {
		t.Fatalf("mkdir savedWorkingDir: %v", err)
	}
	markerPath := filepath.Join(tempDir, "restarted-pwd.txt")
	command := fmt.Sprintf("pwd > %q; printf '› '; sleep 1", markerPath)

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
			return jsonHTTPResponse(200, fmt.Sprintf(`[{"id":"550e8400-e29b-41d4-a716-446655440000","runtime_id":"runtime-1","agent_runtimes":{"provider":"GEMINI","command_template":%q}}]`, command)), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/prepare-command-route":
			return jsonHTTPResponse(200, `{"agentId":"550e8400-e29b-41d4-a716-446655440000","task":{"id":"task-1","name":"Resume task","description":"continue","eligible_agent_ids":["550e8400-e29b-41d4-a716-446655440000"],"selected_folder_id":"folder-1","status":"READY","assigned_agent_instance_id":null}}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			return jsonHTTPResponse(200, `{"claimed":true}`), nil
		case r.Method == http.MethodGet && r.URL.Path == "/task-checkpoints":
			return jsonHTTPResponse(200, `[]`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	statePath := filepath.Join(tempDir, "manager-state.json")
	mgr, err := newManager(config{
		WebBaseURL:       "http://localhost:3000",
		APIBaseURL:       "http://local.test",
		UserJWT:          "user-jwt",
		MaxConcurrent:    1,
		LogFlushInterval: 100 * time.Millisecond,
		StateFile:        statePath,
		CPUThreshold:     80,
		SystemReserveMB:  1024,
		MBPerAgent:       1024,
		LessonsBaseDir:   filepath.Join(tempDir, "agents"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.client = &http.Client{Transport: transport}
	mgr.state.ManagerID = "manager-1"
	mgr.previousManagerPID = os.Getpid() + 1
	mgr.state.Instances = map[string]persistedWorker{
		"instance-1": {
			PID:            0,
			TaskID:         "task-1",
			AgentID:        "550e8400-e29b-41d4-a716-446655440000",
			SessionID:      "session-1",
			Status:         "working",
			WorkingDir:     savedWorkingDir,
			RuntimeCommand: command,
		},
	}
	if err := writeState(statePath, mgr.state); err != nil {
		t.Fatalf("write state: %v", err)
	}
	t.Cleanup(func() {
		mgr.mu.Lock()
		workers := make([]*worker, 0, len(mgr.workers))
		for _, w := range mgr.workers {
			if w != nil {
				workers = append(workers, w)
			}
		}
		mgr.mu.Unlock()
		for _, w := range workers {
			if w.cmd != nil && w.cmd.Process != nil {
				_ = killManagedPID(w.cmd.Process.Pid)
			}
		}
	})

	mgr.reconcileLocalState()
	if err := mgr.recoverRestartableWorkers(context.Background()); err != nil {
		t.Fatalf("recoverRestartableWorkers error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(markerPath)
		if readErr == nil {
			if got := strings.TrimSpace(string(raw)); got != savedWorkingDir {
				t.Fatalf("expected restart in saved working dir %q, got %q", savedWorkingDir, got)
			}
			mgr.mu.Lock()
			workers := make([]*worker, 0, len(mgr.workers))
			for _, w := range mgr.workers {
				if w != nil {
					workers = append(workers, w)
				}
			}
			mgr.mu.Unlock()
			for _, w := range workers {
				if w.cmd != nil && w.cmd.Process != nil {
					_ = killManagedPID(w.cmd.Process.Pid)
					<-w.done
				}
			}
			return
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatalf("read marker: %v", readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for recovered worker to run in saved working dir")
}

func TestRecoverOrphanedWorkersLeavesWorkingProcessAlone(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell helper process setup is unix-specific")
	}

	oldCmd := exec.Command("sh", "-lc", "trap 'exit 0' TERM; while true; do sleep 1; done")
	if err := oldCmd.Start(); err != nil {
		t.Fatalf("start orphaned worker: %v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- oldCmd.Wait()
	}()
	t.Cleanup(func() {
		if oldCmd.Process != nil && oldCmd.ProcessState == nil {
			_ = killManagedPID(oldCmd.Process.Pid)
		}
		select {
		case <-waitDone:
		default:
		}
	})

	tempDir := t.TempDir()
	markerPath := filepath.Join(tempDir, "should-not-restart.txt")
	command := fmt.Sprintf("pwd > %q; printf '› '; sleep 1", markerPath)
	statePath := filepath.Join(tempDir, "manager-state.json")

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/agents/instances/instance-1":
			return jsonHTTPResponse(200, `{"id":"instance-1","status":"WORKING","current_task_id":"task-1"}`), nil
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
			return jsonHTTPResponse(200, fmt.Sprintf(`[{"id":"550e8400-e29b-41d4-a716-446655440000","runtime_id":"runtime-1","agent_runtimes":{"provider":"GEMINI","command_template":%q}}]`, command)), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	mgr, err := newManager(config{
		WebBaseURL:       "http://localhost:3000",
		APIBaseURL:       "http://local.test",
		UserJWT:          "user-jwt",
		MaxConcurrent:    1,
		LogFlushInterval: 100 * time.Millisecond,
		StateFile:        statePath,
		CPUThreshold:     80,
		SystemReserveMB:  1024,
		MBPerAgent:       1024,
		LessonsBaseDir:   filepath.Join(tempDir, "agents"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.client = &http.Client{Transport: transport}
	mgr.state.ManagerID = "manager-1"
	mgr.previousManagerPID = os.Getpid() + 1
	mgr.state.Instances = map[string]persistedWorker{
		"instance-1": {
			PID:            oldCmd.Process.Pid,
			TaskID:         "task-1",
			AgentID:        "550e8400-e29b-41d4-a716-446655440000",
			SessionID:      "session-1",
			Status:         "working",
			WorkingDir:     tempDir,
			RuntimeCommand: command,
		},
	}
	if err := writeState(statePath, mgr.state); err != nil {
		t.Fatalf("write state: %v", err)
	}

	if err := mgr.recoverOrphanedWorkers(context.Background()); err != nil {
		t.Fatalf("recoverOrphanedWorkers error: %v", err)
	}
	if !isProcessRunning(oldCmd.Process.Pid) {
		t.Fatalf("expected working orphaned process to remain running")
	}
	if _, err := os.Stat(markerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no restart marker, got err=%v", err)
	}
	persisted := mgr.state.Instances["instance-1"]
	if persisted.Status != "working" {
		t.Fatalf("expected persisted status to stay working, got %q", persisted.Status)
	}
}

func TestRecoverOrphanedWorkersRestartsIdleProcessInSavedWorkingDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell helper process setup is unix-specific")
	}

	oldCmd := exec.Command("sh", "-lc", "trap 'exit 0' TERM; while true; do sleep 1; done")
	if err := oldCmd.Start(); err != nil {
		t.Fatalf("start orphaned worker: %v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- oldCmd.Wait()
	}()
	t.Cleanup(func() {
		if oldCmd.Process != nil && oldCmd.ProcessState == nil {
			_ = killManagedPID(oldCmd.Process.Pid)
		}
		select {
		case <-waitDone:
		default:
		}
	})

	tempDir, err := os.MkdirTemp("", "pa-orphaned-workers-*")
	if err != nil {
		t.Fatalf("mkdir tempDir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	savedWorkingDir := filepath.Join(tempDir, "saved-worktree")
	if err := os.MkdirAll(savedWorkingDir, 0o755); err != nil {
		t.Fatalf("mkdir savedWorkingDir: %v", err)
	}
	markerPath := filepath.Join(tempDir, "restarted-pwd.txt")
	command := fmt.Sprintf("pwd > %q; printf '› '; sleep 1", markerPath)

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/agents/instances/instance-1":
			return jsonHTTPResponse(200, `{"id":"instance-1","status":"IDLE","current_task_id":null}`), nil
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
			return jsonHTTPResponse(200, fmt.Sprintf(`[{"id":"550e8400-e29b-41d4-a716-446655440000","runtime_id":"runtime-1","agent_runtimes":{"provider":"GEMINI","command_template":%q}}]`, command)), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/prepare-command-route":
			return jsonHTTPResponse(200, `{"agentId":"550e8400-e29b-41d4-a716-446655440000","task":{"id":"task-1","name":"Resume task","description":"continue","eligible_agent_ids":["550e8400-e29b-41d4-a716-446655440000"],"selected_folder_id":"folder-1","status":"READY","assigned_agent_instance_id":null}}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			return jsonHTTPResponse(200, `{"claimed":true}`), nil
		case r.Method == http.MethodGet && r.URL.Path == "/task-checkpoints":
			return jsonHTTPResponse(200, `[]`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	statePath := filepath.Join(tempDir, "manager-state.json")
	mgr, err := newManager(config{
		WebBaseURL:       "http://localhost:3000",
		APIBaseURL:       "http://local.test",
		UserJWT:          "user-jwt",
		MaxConcurrent:    1,
		LogFlushInterval: 100 * time.Millisecond,
		StateFile:        statePath,
		CPUThreshold:     80,
		SystemReserveMB:  1024,
		MBPerAgent:       1024,
		LessonsBaseDir:   filepath.Join(tempDir, "agents"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.client = &http.Client{Transport: transport}
	mgr.state.ManagerID = "manager-1"
	mgr.previousManagerPID = os.Getpid() + 1
	mgr.state.Instances = map[string]persistedWorker{
		"instance-1": {
			PID:            oldCmd.Process.Pid,
			TaskID:         "task-1",
			AgentID:        "550e8400-e29b-41d4-a716-446655440000",
			SessionID:      "session-1",
			Status:         "working",
			WorkingDir:     savedWorkingDir,
			RuntimeCommand: command,
		},
	}
	if err := writeState(statePath, mgr.state); err != nil {
		t.Fatalf("write state: %v", err)
	}
	t.Cleanup(func() {
		mgr.mu.Lock()
		workers := make([]*worker, 0, len(mgr.workers))
		for _, w := range mgr.workers {
			if w != nil {
				workers = append(workers, w)
			}
		}
		mgr.mu.Unlock()
		for _, w := range workers {
			if w.cmd != nil && w.cmd.Process != nil {
				_ = killManagedPID(w.cmd.Process.Pid)
				<-w.done
			}
		}
	})

	if err := mgr.recoverOrphanedWorkers(context.Background()); err != nil {
		t.Fatalf("first recoverOrphanedWorkers error: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	stopped := false
	for time.Now().Before(deadline) {
		select {
		case err := <-waitDone:
			if err != nil && !strings.Contains(err.Error(), "terminated") {
				t.Fatalf("wait orphaned idle process: %v", err)
			}
			stopped = true
		default:
		}
		if stopped {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !stopped {
		t.Fatalf("expected orphaned idle process to stop")
	}

	if err := mgr.recoverOrphanedWorkers(context.Background()); err != nil {
		t.Fatalf("second recoverOrphanedWorkers error: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(markerPath)
		if readErr == nil {
			if got := strings.TrimSpace(string(raw)); got != savedWorkingDir {
				t.Fatalf("expected restart in saved working dir %q, got %q", savedWorkingDir, got)
			}
			return
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatalf("read marker: %v", readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for orphaned idle worker restart")
}

func TestRecoverOrphanedWorkersRestartsPersistedIdleProcessWhenBackendStatusIsStaleWorking(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell helper process setup is unix-specific")
	}

	oldCmd := exec.Command("sh", "-lc", "trap 'exit 0' TERM; while true; do sleep 1; done")
	if err := oldCmd.Start(); err != nil {
		t.Fatalf("start orphaned worker: %v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- oldCmd.Wait()
	}()
	t.Cleanup(func() {
		if oldCmd.Process != nil && oldCmd.ProcessState == nil {
			_ = killManagedPID(oldCmd.Process.Pid)
		}
		waitForManagedProcessExit(t, waitDone, "orphaned worker")
	})

	tempDir := t.TempDir()
	savedWorkingDir := filepath.Join(tempDir, "saved-worktree")
	if err := os.MkdirAll(savedWorkingDir, 0o755); err != nil {
		t.Fatalf("mkdir savedWorkingDir: %v", err)
	}
	markerPath := filepath.Join(tempDir, "stale-working-restarted-pwd.txt")
	command := fmt.Sprintf("pwd > %q; printf '› '; sleep 1", markerPath)

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/agents/instances/instance-1":
			return jsonHTTPResponse(200, `{"id":"instance-1","status":"WORKING","current_task_id":"task-1"}`), nil
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
			return jsonHTTPResponse(200, fmt.Sprintf(`[{"id":"550e8400-e29b-41d4-a716-446655440000","runtime_id":"runtime-1","agent_runtimes":{"provider":"GEMINI","command_template":%q}}]`, command)), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/prepare-command-route":
			return jsonHTTPResponse(200, `{"agentId":"550e8400-e29b-41d4-a716-446655440000","task":{"id":"task-1","name":"Resume task","description":"continue","eligible_agent_ids":["550e8400-e29b-41d4-a716-446655440000"],"selected_folder_id":"folder-1","status":"READY","assigned_agent_instance_id":null}}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			return jsonHTTPResponse(200, `{"claimed":true}`), nil
		case r.Method == http.MethodGet && r.URL.Path == "/task-checkpoints":
			return jsonHTTPResponse(200, `[]`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	statePath := filepath.Join(tempDir, "manager-state.json")
	mgr, err := newManager(config{
		WebBaseURL:       "http://localhost:3000",
		APIBaseURL:       "http://local.test",
		UserJWT:          "user-jwt",
		MaxConcurrent:    1,
		LogFlushInterval: 100 * time.Millisecond,
		StateFile:        statePath,
		CPUThreshold:     80,
		SystemReserveMB:  1024,
		MBPerAgent:       1024,
		LessonsBaseDir:   filepath.Join(tempDir, "agents"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.client = &http.Client{Transport: transport}
	mgr.state.ManagerID = "manager-1"
	mgr.previousManagerPID = os.Getpid() + 1
	mgr.state.Instances = map[string]persistedWorker{
		"instance-1": {
			PID:            oldCmd.Process.Pid,
			TaskID:         "task-1",
			AgentID:        "550e8400-e29b-41d4-a716-446655440000",
			SessionID:      "session-1",
			Status:         "idle",
			WorkingDir:     savedWorkingDir,
			RuntimeCommand: command,
		},
	}
	if err := writeState(statePath, mgr.state); err != nil {
		t.Fatalf("write state: %v", err)
	}
	t.Cleanup(func() {
		mgr.mu.Lock()
		workers := make([]*worker, 0, len(mgr.workers))
		for _, w := range mgr.workers {
			if w != nil {
				workers = append(workers, w)
			}
		}
		mgr.mu.Unlock()
		for _, w := range workers {
			if w.cmd != nil && w.cmd.Process != nil {
				_ = killManagedPID(w.cmd.Process.Pid)
				waitForManagedPIDExit(t, w.cmd.Process.Pid, "restarted orphaned worker")
			}
		}
	})

	if err := mgr.recoverOrphanedWorkers(context.Background()); err != nil {
		t.Fatalf("recoverOrphanedWorkers error: %v", err)
	}
	if isProcessRunning(oldCmd.Process.Pid) {
		t.Fatalf("expected stale idle orphaned process to be terminated")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		content, err := os.ReadFile(markerPath)
		if err == nil {
			if got := strings.TrimSpace(string(content)); got != savedWorkingDir {
				t.Fatalf("expected restarted worker to run in %q, got %q", savedWorkingDir, got)
			}
			return
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read marker: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for stale-working orphan recovery restart marker")
}

func waitForManagedProcessExit(t *testing.T, waitDone <-chan error, label string) {
	t.Helper()
	select {
	case <-waitDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s to exit", label)
	}
}

func waitForManagedPIDExit(t *testing.T, pid int, label string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !isProcessRunning(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s pid %d to exit", label, pid)
}

func TestRecoverOrphanedWorkersSkipsInstancesAlreadyOwnedByCurrentManager(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell helper process setup is unix-specific")
	}

	ownedCmd := exec.Command("sh", "-lc", "trap 'exit 0' TERM; while true; do sleep 1; done")
	if err := ownedCmd.Start(); err != nil {
		t.Fatalf("start owned worker: %v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- ownedCmd.Wait()
	}()
	t.Cleanup(func() {
		if ownedCmd.Process != nil && ownedCmd.ProcessState == nil {
			_ = killManagedPID(ownedCmd.Process.Pid)
		}
		select {
		case <-waitDone:
		default:
		}
	})

	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, "manager-state.json")
	mgr, err := newManager(config{
		WebBaseURL:       "http://localhost:3000",
		APIBaseURL:       "http://local.test",
		UserJWT:          "user-jwt",
		MaxConcurrent:    1,
		LogFlushInterval: 100 * time.Millisecond,
		StateFile:        statePath,
		CPUThreshold:     80,
		SystemReserveMB:  1024,
		MBPerAgent:       1024,
		LessonsBaseDir:   filepath.Join(tempDir, "agents"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	mgr.previousManagerPID = os.Getpid() + 1
	mgr.state.Instances = map[string]persistedWorker{
		"instance-1": {
			PID:        ownedCmd.Process.Pid,
			TaskID:     "task-1",
			AgentID:    "agent-1",
			SessionID:  "session-1",
			Status:     "working",
			WorkingDir: tempDir,
		},
	}
	mgr.workers["instance-1"] = &worker{
		instanceID: "instance-1",
		cmd:        ownedCmd,
	}

	if err := mgr.recoverOrphanedWorkers(context.Background()); err != nil {
		t.Fatalf("recoverOrphanedWorkers error: %v", err)
	}
	if !isProcessRunning(ownedCmd.Process.Pid) {
		t.Fatalf("expected current-manager-owned worker process to remain running")
	}
}

func TestShouldTerminateOrphanedWorkerForReadyPromptSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "logs.raw")
	if err := os.WriteFile(logPath, []byte("Gemini CLI\nType your message\n› "), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	mgr := &manager{cfg: config{StateFile: filepath.Join(tempDir, "manager-state.json")}}
	shouldTerminate := mgr.shouldTerminateOrphanedWorker("instance-1", persistedWorker{
		Status:         "working",
		RuntimeCommand: "gemini --model auto",
		LocalLogFile:   logPath,
	}, "working")

	if !shouldTerminate {
		t.Fatal("expected ready-empty prompt snapshot to terminate orphaned worker")
	}
}

func TestShouldNotTerminateOrphanedWorkerForGeminiConfirmationSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "logs.raw")
	snapshot := strings.Join([]string{
		"Gemini CLI",
		"Allow execution?",
		"› 1. Allow once",
		"2. Allow always",
		"3. Deny",
		"Press enter to continue",
	}, "\n")
	if err := os.WriteFile(logPath, []byte(snapshot), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	mgr := &manager{cfg: config{StateFile: filepath.Join(tempDir, "manager-state.json")}}
	shouldTerminate := mgr.shouldTerminateOrphanedWorker("instance-1", persistedWorker{
		Status:         "working",
		RuntimeCommand: "gemini --model auto",
		LocalLogFile:   logPath,
	}, "working")

	if shouldTerminate {
		t.Fatal("expected Gemini confirmation snapshot to match other interactive prompts and stay running")
	}
}

func TestShouldNotTerminateOrphanedWorkerForBusyGeminiSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "logs.raw")
	if err := os.WriteFile(logPath, []byte("Gemini CLI\nWorking...\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	mgr := &manager{cfg: config{StateFile: filepath.Join(tempDir, "manager-state.json")}}
	shouldTerminate := mgr.shouldTerminateOrphanedWorker("instance-1", persistedWorker{
		Status:         "working",
		RuntimeCommand: "gemini --model auto",
		LocalLogFile:   logPath,
	}, "working")

	if shouldTerminate {
		t.Fatal("expected busy Gemini snapshot not to terminate orphaned worker")
	}
}

func testJSONResponse(t *testing.T, status int, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal test response: %v", err)
	}
	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(raw)),
	}
}

func mustTestJWT(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign test jwt: %v", err)
	}
	return token
}

func TestReadStateMissingFileReturnsError(t *testing.T) {
	tempDir := t.TempDir()
	_, err := readState(filepath.Join(tempDir, "does-not-exist.json"))
	if err == nil {
		t.Fatalf("expected error for missing state file")
	}
}

func TestGetenvReturnsFallbackWhenUnset(t *testing.T) {
	const key = "PASSIVEAGENTS_TEST_ENV"
	_ = os.Unsetenv(key)

	got := getenv(key, "fallback")
	if got != "fallback" {
		t.Fatalf("expected fallback, got %q", got)
	}
}

func TestGetenvReturnsValueWhenSet(t *testing.T) {
	const key = "PASSIVEAGENTS_TEST_ENV"
	const value = "set-value"
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("setenv failed: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Unsetenv(key)
	})

	got := getenv(key, "fallback")
	if got != value {
		t.Fatalf("expected %q, got %q", value, got)
	}
}

func TestHasPollingCapacity(t *testing.T) {
	t.Run("allows polling when below all thresholds", func(t *testing.T) {
		ok := hasPollingCapacity(0, 2, 30, 80, 4096, 25, 0)
		if !ok {
			t.Fatalf("expected polling capacity")
		}
	})

	t.Run("blocks when max concurrency reached", func(t *testing.T) {
		ok := hasPollingCapacity(2, 2, 30, 80, 4096, 25, 0)
		if ok {
			t.Fatalf("expected no capacity")
		}
	})

	t.Run("blocks when cpu threshold exceeded for sustained streak", func(t *testing.T) {
		ok := hasPollingCapacity(0, 2, 85, 80, 4096, 25, 3)
		if ok {
			t.Fatalf("expected no capacity")
		}
	})

	t.Run("allows transient cpu spike before sustained streak", func(t *testing.T) {
		ok := hasPollingCapacity(0, 2, 85, 80, 4096, 25, 2)
		if !ok {
			t.Fatalf("expected capacity during transient cpu spike")
		}
	})

	t.Run("blocks when free memory is below percent and absolute threshold", func(t *testing.T) {
		ok := hasPollingCapacity(0, 2, 30, 80, 512, 8, 0)
		if ok {
			t.Fatalf("expected no capacity")
		}
	})

	t.Run("allows when absolute memory threshold is met and RAM utilization stays below cap", func(t *testing.T) {
		ok := hasPollingCapacity(0, 2, 30, 80, 2048, 21, 0)
		if !ok {
			t.Fatalf("expected capacity with sufficient absolute memory")
		}
	})

	t.Run("blocks when ram utilization reaches eighty percent and free memory is low", func(t *testing.T) {
		ok := hasPollingCapacity(0, 2, 30, 80, 1024, 20, 0)
		if ok {
			t.Fatalf("expected no capacity once RAM utilization reaches 80 percent")
		}
	})
}

func TestCurrentWakeCapacityUsesRunningWorkersAndDynamicMax(t *testing.T) {
	mgr := &manager{
		cfg: config{
			MaxConcurrent:   4,
			SystemReserveMB: 512,
			MBPerAgent:      512,
		},
		workers: map[string]*worker{
			"instance-1": {},
			"instance-2": {},
		},
		cpu:    10,
		freeMB: 4096,
	}

	runningCount, maxConcurrency := mgr.currentWakeCapacity()
	if runningCount != 2 {
		t.Fatalf("expected running count 2, got %d", runningCount)
	}
	if maxConcurrency < 2 {
		t.Fatalf("expected dynamic max concurrency to be at least 2, got %d", maxConcurrency)
	}
}

func TestCalculateAutomaticMaxConcurrentLeavesDesktopCPUHeadroom(t *testing.T) {
	got := calculateAutomaticMaxConcurrentForSystem(8, 16*1024, 1024, 1024)
	if got != 2 {
		t.Fatalf("expected automatic max concurrency 2, got %d", got)
	}
}

func TestCalculateAutomaticMaxConcurrentUsesRamLimitWhenLower(t *testing.T) {
	got := calculateAutomaticMaxConcurrentForSystem(16, 4*1024, 1024, 1024)
	if got != 3 {
		t.Fatalf("expected automatic max concurrency 3, got %d", got)
	}
}

func TestRefreshResourceMetricsKeepsMemoryWhenCPUSamplingFails(t *testing.T) {
	origCPUTimes := cpuTimesFunc
	origVirtualMemory := virtualMemoryFunc
	cpuTimesFunc = func(bool) ([]cpu.TimesStat, error) {
		return nil, errors.New("cpu backend offline")
	}
	virtualMemoryFunc = func() (*mem.VirtualMemoryStat, error) {
		return &mem.VirtualMemoryStat{
			Total:     8 * 1024 * 1024 * 1024,
			Available: 3 * 1024 * 1024 * 1024,
		}, nil
	}
	t.Cleanup(func() {
		cpuTimesFunc = origCPUTimes
		virtualMemoryFunc = origVirtualMemory
	})

	mgr := &manager{cfg: config{CPUThreshold: 80}}
	err := mgr.refreshResourceMetrics()
	if err == nil {
		t.Fatal("expected refreshResourceMetrics to report cpu sampling failure")
	}
	if !strings.Contains(err.Error(), "cpu metrics unavailable") {
		t.Fatalf("expected cpu failure context, got %v", err)
	}
	if !strings.Contains(err.Error(), "memory metrics updated") {
		t.Fatalf("expected memory update context, got %v", err)
	}
	if !strings.Contains(err.Error(), "cpu backend offline") {
		t.Fatalf("expected original cpu error in message, got %v", err)
	}
	if mgr.cpu != 0 {
		t.Fatalf("expected cpu to fall back to zero, got %.2f", mgr.cpu)
	}
	if mgr.cpuHighStreak != 0 {
		t.Fatalf("expected cpu high streak 0, got %d", mgr.cpuHighStreak)
	}
	if mgr.totalMB != 8192 {
		t.Fatalf("expected totalMB 8192, got %.2f", mgr.totalMB)
	}
	if mgr.freeMB != 3072 {
		t.Fatalf("expected freeMB 3072, got %.2f", mgr.freeMB)
	}
	if mgr.freePct != 37.5 {
		t.Fatalf("expected freePct 37.5, got %.2f", mgr.freePct)
	}
}

func TestRefreshResourceMetricsSamplesCPUPercentFromTimes(t *testing.T) {
	origCPUTimes := cpuTimesFunc
	origVirtualMemory := virtualMemoryFunc
	samples := [][]cpu.TimesStat{
		{{
			User:   100,
			System: 50,
			Idle:   50,
		}},
		{{
			User:   130,
			System: 70,
			Idle:   100,
		}},
	}
	call := 0
	cpuTimesFunc = func(bool) ([]cpu.TimesStat, error) {
		if call >= len(samples) {
			return samples[len(samples)-1], nil
		}
		sample := samples[call]
		call++
		return sample, nil
	}
	virtualMemoryFunc = func() (*mem.VirtualMemoryStat, error) {
		return &mem.VirtualMemoryStat{
			Total:     8 * 1024 * 1024 * 1024,
			Available: 3 * 1024 * 1024 * 1024,
		}, nil
	}
	t.Cleanup(func() {
		cpuTimesFunc = origCPUTimes
		virtualMemoryFunc = origVirtualMemory
	})

	mgr := &manager{cfg: config{CPUThreshold: 40}}
	if err := mgr.refreshResourceMetrics(); err != nil {
		t.Fatalf("first refreshResourceMetrics error: %v", err)
	}
	if mgr.cpu != 0 {
		t.Fatalf("expected cpu 0 on baseline sample, got %.2f", mgr.cpu)
	}
	if mgr.cpuHighStreak != 0 {
		t.Fatalf("expected cpu high streak 0 after baseline, got %d", mgr.cpuHighStreak)
	}

	if err := mgr.refreshResourceMetrics(); err != nil {
		t.Fatalf("second refreshResourceMetrics error: %v", err)
	}
	if mgr.cpu != 50 {
		t.Fatalf("expected cpu 50, got %.2f", mgr.cpu)
	}
	if mgr.cpuHighStreak != 1 {
		t.Fatalf("expected cpu high streak 1, got %d", mgr.cpuHighStreak)
	}
	if mgr.totalMB != 8192 {
		t.Fatalf("expected totalMB 8192, got %.2f", mgr.totalMB)
	}
	if mgr.freeMB != 3072 {
		t.Fatalf("expected freeMB 3072, got %.2f", mgr.freeMB)
	}
	if mgr.freePct != 37.5 {
		t.Fatalf("expected freePct 37.5, got %.2f", mgr.freePct)
	}
}

func TestCalculateCapacitySnapshotUsesObservedWorkerResources(t *testing.T) {
	snapshot := calculateCapacitySnapshot(capacityInputs{
		Cores:              8,
		CPUPercent:         20,
		TotalMB:            16 * 1024,
		FreeMB:             6 * 1024,
		SystemReserveMB:    1024,
		FallbackAgentRAMMB: 1024,
		HardMaxConcurrent:  8,
		WorkerCosts: []workerCapacityCost{
			{CPUCores: 0.1, RAMMB: 2048},
			{CPUCores: 0.1, RAMMB: 2048},
		},
	})

	if snapshot.CurrentRunningAgents != 2 {
		t.Fatalf("expected running agents 2, got %d", snapshot.CurrentRunningAgents)
	}
	if snapshot.MaxParallelAgents != 2 {
		t.Fatalf("expected parallel agents 2, got %d", snapshot.MaxParallelAgents)
	}
}

func TestNewRuntimeCommandRunsAgentsAtLowerPriorityByDefault(t *testing.T) {
	_ = os.Unsetenv("PASSIVEAGENTS_AGENT_NICE")
	t.Cleanup(func() {
		_ = os.Unsetenv("PASSIVEAGENTS_AGENT_NICE")
	})

	cmd := newRuntimeCommand("echo hello")
	if len(cmd.Args) < 5 {
		t.Fatalf("expected nice-wrapped command, got args=%v", cmd.Args)
	}
	if cmd.Args[0] != "nice" || cmd.Args[1] != "-n" || cmd.Args[2] != strconv.Itoa(defaultAgentNiceValue) {
		t.Fatalf("expected default nice wrapper, got args=%v", cmd.Args)
	}
	if cmd.Args[3] != "sh" || cmd.Args[4] != "-lc" {
		t.Fatalf("expected shell under nice wrapper, got args=%v", cmd.Args)
	}
}

func TestNewRuntimeCommandCanDisablePriorityWrapper(t *testing.T) {
	if err := os.Setenv("PASSIVEAGENTS_AGENT_NICE", "0"); err != nil {
		t.Fatalf("set env failed: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Unsetenv("PASSIVEAGENTS_AGENT_NICE")
	})

	cmd := newRuntimeCommand("echo hello")
	if len(cmd.Args) < 3 {
		t.Fatalf("expected shell command, got args=%v", cmd.Args)
	}
	if cmd.Args[0] != "sh" || cmd.Args[1] != "-lc" {
		t.Fatalf("expected direct shell command when nice wrapper is disabled, got args=%v", cmd.Args)
	}
}

func TestWorkerEffectiveCapacityCostAppliesStartupWeighting(t *testing.T) {
	now := time.Now()
	w := &worker{
		startedAt:        now.Add(-10 * time.Second),
		observedRSSMB:    512,
		observedCPUCores: 0.15,
	}

	cost := w.effectiveCapacityCost(now, 1024)
	if cost.RAMMB < 1024 {
		t.Fatalf("expected startup RAM cost to be at least fallback 1024MB, got %.2f", cost.RAMMB)
	}
	if cost.CPUCores < defaultStartupAgentCPUCores {
		t.Fatalf("expected startup CPU cost to be at least %.2f cores, got %.2f", defaultStartupAgentCPUCores, cost.CPUCores)
	}
}

func TestRequestGracefulWorkerShutdownWritesPersistPrompt(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		instanceID: "instance-1",
		input:      nopWriteCloser{Writer: &input},
		logs:       make(chan logEntry, 1),
	}
	mgr := &manager{}

	if err := mgr.requestGracefulWorkerShutdown(w); err != nil {
		t.Fatalf("requestGracefulWorkerShutdown error: %v", err)
	}
	if got := input.String(); !strings.Contains(got, "task_checkpoints.jsonl") {
		t.Fatalf("expected shutdown prompt to mention task_checkpoints.jsonl, got %q", got)
	}
	if got := input.String(); !strings.Contains(got, "lessons.jsonl") {
		t.Fatalf("expected shutdown prompt to mention lessons.jsonl, got %q", got)
	}
	if got := input.String(); !strings.Contains(got, "Do not ask the user for permission or confirmation") {
		t.Fatalf("expected shutdown prompt to tell agents not to ask permission, got %q", got)
	}
	shutdownRequested, shutdownPrompt, shutdownMode := w.shutdownRequestDetails()
	if !shutdownRequested {
		t.Fatalf("expected worker shutdown to be marked as requested")
	}
	if shutdownMode != shutdownModeUser {
		t.Fatalf("unexpected shutdown mode %q", shutdownMode)
	}
	if !strings.Contains(shutdownPrompt, "TASK_CONTEXT.md") {
		t.Fatalf("unexpected shutdown prompt %q", shutdownPrompt)
	}
}

func TestRequestManagerRestartWorkerShutdownMarksRestartMode(t *testing.T) {
	var input bytes.Buffer
	w := &worker{
		instanceID: "instance-1",
		input:      nopWriteCloser{Writer: &input},
		logs:       make(chan logEntry, 1),
	}
	mgr := &manager{}

	if err := mgr.requestManagerRestartWorkerShutdown(w); err != nil {
		t.Fatalf("requestManagerRestartWorkerShutdown error: %v", err)
	}
	if got := input.String(); !strings.Contains(got, "task_checkpoints.jsonl") {
		t.Fatalf("expected shutdown prompt to mention task_checkpoints.jsonl, got %q", got)
	}
	shutdownRequested, shutdownPrompt, shutdownMode := w.shutdownRequestDetails()
	if !shutdownRequested {
		t.Fatalf("expected worker shutdown to be marked as requested")
	}
	if shutdownMode != shutdownModeManagerRestart {
		t.Fatalf("unexpected shutdown mode %q", shutdownMode)
	}
	if !strings.Contains(shutdownPrompt, "lessons.jsonl") {
		t.Fatalf("unexpected shutdown prompt %q", shutdownPrompt)
	}
}

func TestLoadPersonaLessonsFileUsesStablePersonaIDPath(t *testing.T) {
	tempDir := t.TempDir()
	globalPath := filepath.Join(tempDir, "lessons.jsonl")
	mgr := &manager{
		cfg: config{
			LessonsBaseDir:   filepath.Join(tempDir, "agents"),
			LessonsJSONLFile: globalPath,
		},
	}

	content, hash, err := mgr.loadPersonaLessonsFile("550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("loadPersonaLessonsFile error: %v", err)
	}
	if content != "" {
		t.Fatalf("expected empty initialized lessons jsonl content, got %q", content)
	}
	if hash == "" {
		t.Fatalf("expected content hash")
	}

	if _, err := os.Stat(globalPath); err != nil {
		t.Fatalf("expected global lessons jsonl file: %v", err)
	}
}

func TestMonitorWorkerExitPreservesRestartableStateForManagerStop(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell helper process setup is unix-specific")
	}

	cmd := exec.Command("sh", "-lc", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	_ = writer.Close()

	statePath := filepath.Join(t.TempDir(), "manager-state.json")
	mgr := &manager{
		cfg: config{StateFile: statePath},
		state: persistedState{
			Instances: map[string]persistedWorker{
				"instance-1": {
					PID:            cmd.Process.Pid,
					TaskID:         "task-1",
					AgentID:        "agent-1",
					SessionID:      "session-1",
					Status:         "working",
					WorkingDir:     "/tmp/worktree",
					RuntimeCommand: "gemini --model auto",
				},
			},
		},
		logger:                slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		instanceSpawnLocks:    map[string]*sync.Mutex{"instance-1": &sync.Mutex{}},
		browserPreferredSizes: map[string]terminalSize{"instance-1": {cols: 120, rows: 40}},
	}
	w := &worker{
		instanceID:       "instance-1",
		taskID:           "task-1",
		agentID:          "agent-1",
		sessionID:        "session-1",
		workingDir:       "/tmp/worktree",
		runtimeCommand:   "gemini --model auto",
		cmd:              cmd,
		input:            nopWriteCloser{Writer: io.Discard},
		output:           reader,
		logs:             make(chan logEntry, 1),
		stopBatch:        make(chan struct{}),
		outputBuffer:     &outputRingBuffer{maxSize: 1024},
		assistantStop:    make(chan struct{}),
		attachClients:    make(map[net.Conn]struct{}),
		done:             make(chan struct{}),
		localLogFilePath: "/tmp/log",
	}
	w.tracked.Store(true)
	if !w.markShutdownRequested(buildGracefulShutdownPrompt(), shutdownModeManagerRestart) {
		t.Fatal("expected shutdown request to be recorded")
	}

	go mgr.monitorWorkerExit(w)

	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker exit")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		persisted, ok := mgr.state.Instances["instance-1"]
		if ok && persisted.PID == 0 {
			if persisted.WorkingDir != "/tmp/worktree" {
				t.Fatalf("unexpected working dir %q", persisted.WorkingDir)
			}
			if persisted.RuntimeCommand != "gemini --model auto" {
				t.Fatalf("unexpected runtime command %q", persisted.RuntimeCommand)
			}
			if _, exists := mgr.instanceSpawnLocks["instance-1"]; exists {
				t.Fatalf("expected instance spawn lock to be cleared")
			}
			if size, exists := mgr.browserPreferredSizes["instance-1"]; !exists {
				t.Fatalf("expected preferred browser size to be preserved")
			} else if size != (terminalSize{cols: 120, rows: 40}) {
				t.Fatalf("unexpected preferred browser size: %#v", size)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	persisted, ok := mgr.state.Instances["instance-1"]
	if !ok {
		t.Fatalf("expected restartable state to be preserved, got %#v", mgr.state.Instances)
	}
	t.Fatalf("expected PID to be cleared, got %d", persisted.PID)
}

func TestLoadPersonaLessonsFileRejectsInvalidPersonaID(t *testing.T) {
	mgr := &manager{
		cfg: config{
			LessonsBaseDir: t.TempDir(),
		},
	}

	if _, _, err := mgr.loadPersonaLessonsFile("../persona-name"); err == nil {
		t.Fatalf("expected invalid persona id error")
	}
}

func TestPersistWorkerLessonsToPersonaMergesWorktreeJSONLIntoGlobalFile(t *testing.T) {
	tempDir := t.TempDir()
	personaID := "550e8400-e29b-41d4-a716-446655440000"
	globalPath := filepath.Join(tempDir, "lessons.jsonl")
	mgr := &manager{
		cfg: config{
			LessonsBaseDir:   filepath.Join(tempDir, "agents"),
			LessonsJSONLFile: globalPath,
		},
	}

	initial := lessonJSONLRecord{
		AgentPersonaID: personaID,
		Lesson:         "Keep fixes small.",
		Source:         "AGENT",
		CreatedAt:      "2026-04-22T00:00:00Z",
	}
	initialRaw, err := json.Marshal(initial)
	if err != nil {
		t.Fatalf("marshal initial lesson: %v", err)
	}
	if err := writeFileAtomic(globalPath, append(initialRaw, '\n'), 0o600, 0o700); err != nil {
		t.Fatalf("write canonical lessons: %v", err)
	}

	workingDir := filepath.Join(tempDir, "worktree")
	workspaceLessonsPath := filepath.Join(workingDir, "lessons.jsonl")
	workspaceRaw := strings.Join([]string{
		`{"lesson":"Keep fixes small."}`,
		`{"lesson":"Copy worktree lessons back before sync."}`,
		"",
	}, "\n")
	if err := writeFileAtomic(workspaceLessonsPath, []byte(workspaceRaw), 0o600, 0o700); err != nil {
		t.Fatalf("write workspace lessons: %v", err)
	}

	if err := mgr.persistWorkerLessonsToPersona(personaID, workingDir); err != nil {
		t.Fatalf("persistWorkerLessonsToPersona error: %v", err)
	}

	records, err := mgr.loadGlobalLessonRecords()
	if err != nil {
		t.Fatalf("read canonical lessons: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d: %#v", len(records), records)
	}
	if records[0].Lesson != "Keep fixes small." {
		t.Fatalf("expected first lesson to be preserved, got %q", records[0].Lesson)
	}
	if records[1].AgentPersonaID != personaID || records[1].Lesson != "Copy worktree lessons back before sync." {
		t.Fatalf("unexpected appended lesson: %#v", records[1])
	}
}

func TestPersistWorkerLessonsToPersonaIgnoresLegacyMarkdown(t *testing.T) {
	tempDir := t.TempDir()
	personaID := "550e8400-e29b-41d4-a716-446655440000"
	globalPath := filepath.Join(tempDir, "lessons.jsonl")
	mgr := &manager{
		cfg: config{
			LessonsBaseDir:   filepath.Join(tempDir, "agents"),
			LessonsJSONLFile: globalPath,
		},
	}

	workingDir := filepath.Join(tempDir, "worktree")
	workspaceLessonsPath := filepath.Join(workingDir, "lessons.md")
	if err := writeFileAtomic(workspaceLessonsPath, []byte(strings.Join([]string{
		"# Lessons Learned",
		"",
		"- Preserve old lessons files during migration.",
		"",
	}, "\n")), 0o600, 0o700); err != nil {
		t.Fatalf("write workspace lessons: %v", err)
	}

	if err := mgr.persistWorkerLessonsToPersona(personaID, workingDir); err != nil {
		t.Fatalf("persistWorkerLessonsToPersona error: %v", err)
	}

	records, err := mgr.loadGlobalLessonRecords()
	if err != nil {
		t.Fatalf("load global lessons: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected legacy markdown file to be ignored, got %#v", records)
	}
}

func TestPersistWorkerLessonsToPersonaRejectsInvalidJSONL(t *testing.T) {
	tempDir := t.TempDir()
	personaID := "550e8400-e29b-41d4-a716-446655440000"
	mgr := &manager{
		cfg: config{
			LessonsBaseDir:   filepath.Join(tempDir, "agents"),
			LessonsJSONLFile: filepath.Join(tempDir, "lessons.jsonl"),
		},
	}

	workingDir := filepath.Join(tempDir, "worktree")
	workspaceLessonsPath := filepath.Join(workingDir, "lessons.jsonl")
	if err := writeFileAtomic(workspaceLessonsPath, []byte("- Preserve old lessons files during migration.\n"), 0o600, 0o700); err != nil {
		t.Fatalf("write workspace lessons: %v", err)
	}

	if err := mgr.persistWorkerLessonsToPersona(personaID, workingDir); err == nil {
		t.Fatal("expected invalid jsonl error")
	}
}

func TestClampPTYDimensionsEnforcesMinimumBrowserSafeSize(t *testing.T) {
	cols, rows, clipped := clampPTYDimensions(1, 2)
	if !clipped {
		t.Fatal("expected tiny PTY dimensions to be clamped")
	}
	if cols < 80 {
		t.Fatalf("expected cols to be clamped to a safe minimum, got %d", cols)
	}
	if rows < 8 {
		t.Fatalf("expected rows to be clamped to a safe minimum, got %d", rows)
	}
}

func TestWritePersonaPromptFileUsesStablePersonaIDPath(t *testing.T) {
	tempDir := t.TempDir()
	mgr := &manager{
		cfg: config{
			LessonsBaseDir: tempDir,
		},
	}

	path, err := mgr.writePersonaPromptFile(
		apiAgentPersona{
			ID:           "550e8400-e29b-41d4-a716-446655440000",
			Name:         "Billy",
			Role:         "Backend Engineer",
			Instructions: "Implement backend changes carefully",
		},
		filepath.Join(tempDir, "550e8400-e29b-41d4-a716-446655440000", "lessons.jsonl"),
	)
	if err != nil {
		t.Fatalf("writePersonaPromptFile error: %v", err)
	}
	if !strings.HasSuffix(path, "AGENT_PERSONA.md") {
		t.Fatalf("expected AGENT_PERSONA.md path, got %q", path)
	}
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read persona file error: %v", readErr)
	}
	if !strings.Contains(string(raw), "# Agent Persona: Billy") {
		t.Fatalf("unexpected persona file content: %q", string(raw))
	}
}

func TestWritePollSnapshotPersistsTasksAndPersonas(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "snapshot", "polled-task-batch.json")
	personas := []apiAgentPersona{
		{ID: "agent-1", Name: "Triangle Agent", Personality: "Precise"},
	}
	tasks := []apiTask{
		{ID: "task-1", Name: "Print triangle", EligibleAgentIDs: []string{"agent-1"}},
	}

	if err := writePollSnapshot(outPath, personas, tasks); err != nil {
		t.Fatalf("writePollSnapshot error: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read snapshot error: %v", err)
	}
	var snap pollSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatalf("unmarshal snapshot error: %v", err)
	}
	if len(snap.Tasks) != 1 || snap.Tasks[0].Name != "Print triangle" {
		t.Fatalf("unexpected tasks payload: %#v", snap.Tasks)
	}
	if len(snap.Personas) != 1 || snap.Personas[0].ID != "agent-1" {
		t.Fatalf("unexpected personas payload: %#v", snap.Personas)
	}
}

func TestChoosePersonaForTaskUsesRunnablePersonaForOpenTask(t *testing.T) {
	personas := []apiAgentPersona{
		{ID: "agent-without-runtime", Name: "Draft Agent"},
		{ID: "agent-runnable", Name: "Runnable Agent", RuntimeID: "runtime-1"},
	}

	got := choosePersonaForTask(personas, apiTask{ID: "task-1"})

	if got == nil || got.ID != "agent-runnable" {
		t.Fatalf("expected runnable persona, got %#v", got)
	}
}

func TestChoosePersonaForTaskUsesRunnableEligiblePersona(t *testing.T) {
	personas := []apiAgentPersona{
		{ID: "agent-without-runtime", Name: "Draft Agent"},
		{ID: "agent-runnable", Name: "Runnable Agent", RuntimeID: "runtime-1"},
	}

	got := choosePersonaForTask(personas, apiTask{
		ID:               "task-1",
		EligibleAgentIDs: []string{"agent-without-runtime", "agent-runnable"},
	})

	if got == nil || got.ID != "agent-runnable" {
		t.Fatalf("expected runnable eligible persona, got %#v", got)
	}
}

func TestPollOnceSpawnsAgentStreamsLogsAndCompletesTaskLocally(t *testing.T) {
	var mu sync.Mutex
	updated := false
	syncedLessons := false
	var registeredAgentID string
	var registeredRuntimeID string
	var claimedTaskID string
	var claimedAgentID string
	triangleOutPath := filepath.Join(t.TempDir(), "triangle.txt")
	triangleCommand := fmt.Sprintf(
		"sleep 6; printf '*\\n**\\n***\\n' | tee %q; echo '[TASK_COMPLETED] rendered triangle'",
		triangleOutPath,
	)

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
			return jsonHTTPResponse(200, fmt.Sprintf(`[
				{"id":"550e8400-e29b-41d4-a716-446655440001","name":"JSON Agent","role":"Formatter","personality":"Strict JSON","instructions":"Return JSON only","examples":"{\"ok\":true}","guardrails":"Do: output valid JSON. Do not: print plain text.","runtime_id":"runtime-json","agent_runtimes":{"provider":"OPENCODE","command_template":"printf '{\"ok\":true}\n'"}},
				{"id":"550e8400-e29b-41d4-a716-446655440002","name":"Triangle Agent","role":"Renderer","personality":"Visual and precise","instructions":"Print a 3-row triangle exactly","examples":"*\n**\n***","guardrails":"Do: keep exact row count. Do not: add extra commentary.","runtime_id":"runtime-triangle","agent_runtimes":{"provider":"OPENCODE","command_template":%q}}
			]`, triangleCommand)), nil
		case r.Method == http.MethodPost && r.URL.Path == "/managers/tasks/eligible":
			return jsonHTTPResponse(200, `[{"id":"task-1","name":"Print triangle","description":"Render exactly three rows: *, **, ***","eligible_agent_ids":["550e8400-e29b-41d4-a716-446655440002"]}]`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			var payload struct {
				AgentID   string `json:"agentId"`
				RuntimeID string `json:"runtimeId"`
			}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			mu.Lock()
			registeredAgentID = payload.AgentID
			registeredRuntimeID = payload.RuntimeID
			mu.Unlock()
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			var payload struct {
				TaskID  string `json:"taskId"`
				AgentID string `json:"agentId"`
			}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			mu.Lock()
			claimedTaskID = payload.TaskID
			claimedAgentID = payload.AgentID
			mu.Unlock()
			return jsonHTTPResponse(200, `{"claimed":true}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/update-task":
			mu.Lock()
			updated = true
			mu.Unlock()
			return jsonHTTPResponse(200, `{}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-lessons/sync":
			mu.Lock()
			syncedLessons = true
			mu.Unlock()
			return jsonHTTPResponse(200, `{"results":[{"agentPersonaId":"550e8400-e29b-41d4-a716-446655440002","status":"synced","inserted":1}]}`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	tempDir := t.TempDir()
	m, err := newManager(config{
		WebBaseURL:        "http://localhost:3000",
		APIBaseURL:        "http://local.test",
		UserJWT:           "user-jwt",
		MaxConcurrent:     1,
		StateFile:         filepath.Join(tempDir, "manager-state.json"),
		CPUThreshold:      80,
		SystemReserveMB:   1024,
		MBPerAgent:        1024,
		LessonsBaseDir:    filepath.Join(tempDir, "agents"),
		PollSnapshotFile:  filepath.Join(tempDir, "polled-task-batch.json"),
		LogFlushInterval:  10 * time.Millisecond,
		LogFlushBatchSize: 1,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: transport}
	m.state.ManagerID = "manager-1"
	m.cpu = 10
	m.freeMB = 4096

	if err := m.pollOnce(context.Background(), false); err != nil {
		t.Fatalf("pollOnce error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		activeInstances := len(m.state.Instances)
		m.mu.Unlock()

		mu.Lock()
		done := updated
		sawLessonSync := syncedLessons
		currentRegisteredAgentID := registeredAgentID
		currentRegisteredRuntimeID := registeredRuntimeID
		currentClaimedTaskID := claimedTaskID
		currentClaimedAgentID := claimedAgentID
		mu.Unlock()

		if activeInstances == 0 && done && sawLessonSync {
			if currentRegisteredAgentID != "550e8400-e29b-41d4-a716-446655440002" {
				t.Fatalf("expected register payload agentId=triangle-agent, got %q", currentRegisteredAgentID)
			}
			if currentRegisteredRuntimeID != "runtime-triangle" {
				t.Fatalf("expected register payload runtimeId=runtime-triangle, got %q", currentRegisteredRuntimeID)
			}
			if currentClaimedTaskID != "task-1" {
				t.Fatalf("expected claim-task payload taskId=task-1, got %q", currentClaimedTaskID)
			}
			if currentClaimedAgentID != "550e8400-e29b-41d4-a716-446655440002" {
				t.Fatalf("expected claim-task payload agentId=triangle-agent, got %q", currentClaimedAgentID)
			}
			_, statErr := os.Stat(m.cfg.PollSnapshotFile)
			if statErr != nil {
				t.Fatalf("expected poll snapshot file: %v", statErr)
			}
			rawLogs, readLogsErr := os.ReadFile(filepath.Join(tempDir, "instances", "instance-1", "logs.raw"))
			if readLogsErr == nil && strings.TrimSpace(string(rawLogs)) != "" {
				rawTriangle, readErr := os.ReadFile(triangleOutPath)
				if readErr != nil {
					t.Fatalf("expected triangle output file: %v", readErr)
				}
				if strings.TrimSpace(string(rawTriangle)) != "*\n**\n***" {
					t.Fatalf("unexpected triangle output: %q", string(rawTriangle))
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for spawned instance completion")
}

func TestSpawnWorkerLeavesTaskReadyWhenAgentClaimFails(t *testing.T) {
	claimAttempted := false

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			claimAttempted = true
			return jsonHTTPResponse(200, `{"claimed":false}`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	tempDir := t.TempDir()
	m, err := newManager(config{
		WebBaseURL:      "http://localhost:3000",
		APIBaseURL:      "http://local.test",
		UserJWT:         "user-jwt",
		MaxConcurrent:   1,
		StateFile:       filepath.Join(tempDir, "manager-state.json"),
		CPUThreshold:    80,
		SystemReserveMB: 1024,
		MBPerAgent:      1024,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: transport}
	m.state.ManagerID = "manager-1"

	err = m.spawnWorker(
		context.Background(),
		apiAgentPersona{
			ID:        "agent-1",
			RuntimeID: "runtime-1",
			AgentRuntime: struct {
				Provider        string `json:"provider"`
				CommandTemplate string `json:"command_template"`
			}{
				CommandTemplate: "printf 'hello\\n'",
			},
		},
		apiTask{ID: "task-1", Name: "Queued task"},
		1,
		"",
	)
	if err == nil || !strings.Contains(err.Error(), "task claim was not granted") {
		t.Fatalf("expected agent claim failure, got %v", err)
	}
	if !claimAttempted {
		t.Fatalf("expected agent claim attempt")
	}
}

func TestSpawnWorkerPersistsTaskWorkingDirectoryImmediatelyAfterClaim(t *testing.T) {
	claimAttempted := false
	claimedWorkingDir := ""
	persistedWorkingDir := ""
	sawClaimStatePersist := false
	persistedTaskID := ""
	persistedCurrentTaskID := ""
	persistedAssignedInstanceID := ""
	persistedManagerID := ""
	tempDir := t.TempDir()
	var logBuf bytes.Buffer

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			var payload struct {
				WorkingDirectory string `json:"workingDirectory"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode claim-task payload: %v", err)
			}
			claimAttempted = true
			claimedWorkingDir = payload.WorkingDirectory
			return jsonHTTPResponse(200, `{"claimed":true}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/update-task":
			var payload struct {
				TaskID                  string `json:"taskId"`
				CurrentTaskID           string `json:"currentTaskId"`
				AssignedAgentInstanceID string `json:"assignedAgentInstanceId"`
				LocalAgentManagerID     string `json:"localAgentManagerId"`
				WorkingDirectory        string `json:"workingDirectory"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode update-task payload: %v", err)
			}
			persistedTaskID = payload.TaskID
			persistedCurrentTaskID = payload.CurrentTaskID
			persistedAssignedInstanceID = payload.AssignedAgentInstanceID
			persistedManagerID = payload.LocalAgentManagerID
			persistedWorkingDir = payload.WorkingDirectory
			sawClaimStatePersist = true
			return jsonHTTPResponse(200, `{}`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	m, err := newManager(config{
		WebBaseURL:        "http://localhost:3000",
		APIBaseURL:        "http://local.test",
		UserJWT:           "user-jwt",
		MaxConcurrent:     1,
		StateFile:         filepath.Join(tempDir, "manager-state.json"),
		CPUThreshold:      80,
		SystemReserveMB:   1024,
		MBPerAgent:        1024,
		LogFlushInterval:  10 * time.Millisecond,
		LogFlushBatchSize: 1,
		LessonsBaseDir:    filepath.Join(tempDir, "agents"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: transport}
	m.logger = slog.New(slog.NewTextHandler(&logBuf, nil))
	m.state.ManagerID = "manager-1"

	err = m.spawnWorkerWithWorkingDir(
		context.Background(),
		apiAgentPersona{
			ID:        "550e8400-e29b-41d4-a716-446655440020",
			RuntimeID: "runtime-1",
			AgentRuntime: struct {
				Provider        string `json:"provider"`
				CommandTemplate string `json:"command_template"`
			}{
				CommandTemplate: "printf 'hello\\n'",
			},
		},
		apiTask{ID: "task-1", Name: "Queued task"},
		1,
		"",
		tempDir,
	)
	if !claimAttempted {
		t.Fatalf("expected agent claim attempt")
	}
	if claimedWorkingDir != tempDir {
		t.Fatalf("expected claim working directory %q, got %q", tempDir, claimedWorkingDir)
	}
	if !sawClaimStatePersist {
		t.Fatalf("expected a claim-state persistence call")
	}
	if persistedTaskID != "task-1" {
		t.Fatalf("expected persisted task id %q, got %q", "task-1", persistedTaskID)
	}
	if persistedCurrentTaskID != "task-1" {
		t.Fatalf("expected persisted current task id %q, got %q", "task-1", persistedCurrentTaskID)
	}
	if persistedAssignedInstanceID != "instance-1" {
		t.Fatalf("expected persisted assigned instance id %q, got %q", "instance-1", persistedAssignedInstanceID)
	}
	if persistedManagerID != "manager-1" {
		t.Fatalf("expected persisted manager id %q, got %q", "manager-1", persistedManagerID)
	}
	if persistedWorkingDir != tempDir {
		t.Fatalf("expected persisted working directory %q, got %q", tempDir, persistedWorkingDir)
	}
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "task_claim_state_persisted") {
		t.Fatalf("expected success log for claim-state persistence, got %q", logOutput)
	}
	if !strings.Contains(logOutput, "working_dir="+tempDir) {
		t.Fatalf("expected success log to include working directory %q, got %q", tempDir, logOutput)
	}
	if !strings.Contains(logOutput, "manager_id=manager-1") {
		t.Fatalf("expected success log to include manager id, got %q", logOutput)
	}
	if err == nil {
		t.Fatalf("expected the test worker to fail later in startup so the test stays scoped to working-directory persistence")
	}
}

func TestSpawnWorkerLogsTaskWorkingDirectoryPersistenceFailure(t *testing.T) {
	claimAttempted := false
	tempDir := t.TempDir()
	var logBuf bytes.Buffer

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/register":
			return jsonHTTPResponse(200, `{"id":"instance-1"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/claim-task":
			claimAttempted = true
			return jsonHTTPResponse(200, `{"claimed":true}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/agent-instances/instance-1/update-task":
			return jsonHTTPResponse(400, `{"message":"Unable to update task"}`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	m, err := newManager(config{
		WebBaseURL:        "http://localhost:3000",
		APIBaseURL:        "http://local.test",
		UserJWT:           "user-jwt",
		MaxConcurrent:     1,
		StateFile:         filepath.Join(tempDir, "manager-state.json"),
		CPUThreshold:      80,
		SystemReserveMB:   1024,
		MBPerAgent:        1024,
		LogFlushInterval:  10 * time.Millisecond,
		LogFlushBatchSize: 1,
		LessonsBaseDir:    filepath.Join(tempDir, "agents"),
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: transport}
	m.logger = slog.New(slog.NewTextHandler(&logBuf, nil))
	m.state.ManagerID = "manager-1"

	err = m.spawnWorkerWithWorkingDir(
		context.Background(),
		apiAgentPersona{
			ID:        "550e8400-e29b-41d4-a716-446655440020",
			RuntimeID: "runtime-1",
			AgentRuntime: struct {
				Provider        string `json:"provider"`
				CommandTemplate string `json:"command_template"`
			}{
				CommandTemplate: "printf 'hello\\n'",
			},
		},
		apiTask{ID: "task-1", Name: "Queued task"},
		1,
		"",
		tempDir,
	)
	if !claimAttempted {
		t.Fatalf("expected agent claim attempt")
	}
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "task_claim_state_persist_failed") {
		t.Fatalf("expected failure log for claim-state persistence, got %q", logOutput)
	}
	if strings.Contains(logOutput, "task_claim_state_persisted") {
		t.Fatalf("did not expect success log when persistence failed, got %q", logOutput)
	}
	if err == nil {
		t.Fatalf("expected the test worker to fail later in startup so the test stays scoped to working-directory persistence logging")
	}
}

func TestPollLoopRunsImmediatelyWhenRequested(t *testing.T) {
	var mu sync.Mutex
	personaPolls := 0

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/managers/tasks/agent-personas":
			mu.Lock()
			personaPolls++
			mu.Unlock()
			return jsonHTTPResponse(200, `[]`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"not found"}`), nil
		}
	})

	tempDir := t.TempDir()
	m, err := newManager(config{
		WebBaseURL:      "http://localhost:3000",
		APIBaseURL:      "http://local.test",
		UserJWT:         "user-jwt",
		MaxConcurrent:   1,
		StateFile:       filepath.Join(tempDir, "manager-state.json"),
		CPUThreshold:    80,
		SystemReserveMB: 1024,
		MBPerAgent:      1024,
		PollInterval:    time.Hour,
	})
	if err != nil {
		t.Fatalf("newManager error: %v", err)
	}
	m.client = &http.Client{Transport: transport}
	m.state.ManagerID = "manager-1"
	m.cpu = 10
	m.freeMB = 4096

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.pollLoop(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		currentPolls := personaPolls
		mu.Unlock()
		if currentPolls >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	m.requestImmediatePoll()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		currentPolls := personaPolls
		mu.Unlock()
		if currentPolls >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected immediate poll request to trigger another poll")
}

func executeCLITestCommand(t *testing.T, cfg config, args ...string) (string, error) {
	t.Helper()
	root := newRootCommand(cfg)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs(args)

	capturedStdout, err := captureStdout(t, func() error {
		return root.ExecuteContext(context.Background())
	})
	output := stdout.String() + capturedStdout
	if stderr.Len() > 0 {
		output += stderr.String()
	}
	return output, err
}

func useIsolatedServiceHomeDir(t *testing.T) {
	t.Helper()
	originalHomeDir := serviceUserHomeDir
	originalGOOS := serviceCurrentGOOS
	tempHome := t.TempDir()
	serviceUserHomeDir = func() (string, error) {
		return tempHome, nil
	}
	serviceCurrentGOOS = func() string {
		return runtime.GOOS
	}
	t.Cleanup(func() {
		serviceUserHomeDir = originalHomeDir
		serviceCurrentGOOS = originalGOOS
	})
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe error: %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = originalStdout
	}()

	runErr := fn()
	_ = writer.Close()

	output, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil {
		t.Fatalf("reading captured stdout: %v", readErr)
	}
	return string(output), runErr
}

func disableBrowserOpenForTest(t *testing.T) {
	t.Helper()
	originalShouldAttempt := shouldAttemptBrowserOpenFunc
	originalTryOpen := tryOpenBrowserFunc
	shouldAttemptBrowserOpenFunc = func() bool { return false }
	tryOpenBrowserFunc = func(targetURL string) error {
		t.Fatalf("login test must not open browser URL %q", targetURL)
		return nil
	}
	t.Cleanup(func() {
		shouldAttemptBrowserOpenFunc = originalShouldAttempt
		tryOpenBrowserFunc = originalTryOpen
	})
}

func startPassiveAgentsTestProcess(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestPassiveAgentsHelperProcess", "--", "sleep")
	cmd.Env = append(os.Environ(), "PASSIVEAGENTS_TEST_HELPER_PROCESS=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	return cmd
}

func stopPassiveAgentsTestProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal helper process: %v", err)
	}
	waitForPassiveAgentsTestProcessExit(t, cmd)
}

func waitForPassiveAgentsTestProcessExit(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for helper process %d to exit", cmd.Process.Pid)
	case err := <-done:
		if err != nil && cmd.ProcessState != nil && !cmd.ProcessState.Success() {
			// A signaled process is the expected stop path here.
			return
		}
	}
}

func TestPassiveAgentsHelperProcess(t *testing.T) {
	if os.Getenv("PASSIVEAGENTS_TEST_HELPER_PROCESS") != "1" {
		return
	}
	select {}
}

func TestTerminateProcessStopsManagedProcessGroup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process group signaling test is Unix-specific")
	}

	tempDir := t.TempDir()
	childPath := filepath.Join(tempDir, "child.pid")
	scriptPath := filepath.Join(tempDir, "spawn-child.sh")
	script := fmt.Sprintf(`#!/bin/sh
sleep 30 &
echo $! > %q
wait
`, childPath)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	cmd := newRuntimeCommand(scriptPath)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start managed process: %v", err)
	}
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()
	defer func() {
		if cmd.Process != nil {
			_ = killManagedPID(cmd.Process.Pid)
		}
	}()

	var childPID int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(childPath)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if convErr != nil {
				t.Fatalf("parse child pid: %v", convErr)
			}
			childPID = pid
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read child pid: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if childPID == 0 {
		t.Fatal("timed out waiting for child pid")
	}
	defer func() {
		if proc, err := os.FindProcess(childPID); err == nil {
			_ = proc.Kill()
		}
	}()

	if err := terminateProcess(cmd.Process.Pid); err != nil {
		t.Fatalf("terminate process group: %v", err)
	}
	select {
	case err := <-waitCh:
		if err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("wait parent process: %v", err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected parent process to exit after terminateProcess")
	}
	if !waitForProcessExit(childPID, 2*time.Second) {
		t.Fatalf("expected child process %d to exit with managed process group", childPID)
	}
}

func newTestBrowserClient(t *testing.T) (*browserClient, func(time.Duration) (map[string]any, error)) {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	serverConnCh := make(chan *websocket.Conn, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		serverConnCh <- conn
	})}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("tcp4 loopback listener unavailable: %v", err)
	}
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
	})

	clientConn, _, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String(), nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() {
		_ = clientConn.Close()
	})

	serverConn := <-serverConnCh
	t.Cleanup(func() {
		_ = serverConn.Close()
	})

	client := &browserClient{
		conn:               serverConn,
		authenticated:      true,
		authenticatedUser:  "user-1",
		subscribedInstance: map[string]struct{}{},
	}
	readEvent := func(timeout time.Duration) (map[string]any, error) {
		if timeout <= 0 {
			timeout = time.Second
		}
		_ = clientConn.SetReadDeadline(time.Now().Add(timeout))
		defer clientConn.SetReadDeadline(time.Time{})
		var payload map[string]any
		if err := clientConn.ReadJSON(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	return client, readEvent
}

type roundTripFunc func(r *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// Bug: looksLikeCodexReadySnapshot returns true for "esc to interrupt" even
// when Codex is actively processing (no › visible). This causes
// workerAgentStatusForWorker to return "idle" instead of "working".
func TestWorkerAgentStatusIsWorkingWhenCodexProcessingShowsEscToInterrupt(t *testing.T) {
	w := &worker{
		usesPTY:        true,
		runtimeCommand: "codex --profile default",
		outputBuffer:   &outputRingBuffer{maxSize: 4096},
	}
	w.bootstrapObserved.Store(true)
	w.bootstrapReadyEmptySeen.Store(true)
	// Codex processing state: generating a response, no › input box visible.
	w.outputBuffer.Write(strings.Join([]string{
		"Codex",
		"Let me help you with that...",
		"esc to interrupt",
	}, "\n"))
	got := workerAgentStatusForWorker(w)
	if got != "working" {
		t.Fatalf("expected working when Codex is processing, got %q", got)
	}
}

func readySnapshotForRuntime(runtimeCommand string) string {
	switch {
	case isGeminiRuntimeCommand(runtimeCommand):
		return "Gemini CLI\nType your message or @path\n› "
	case isOpenCodeRuntimeCommand(runtimeCommand):
		return "OpenCode\n› "
	case isClaudeRuntimeCommand(runtimeCommand):
		return "Claude Code\n› "
	default:
		return "Codex\n› "
	}
}

// Bug: codexPromptDraft scans bottom-to-top and breaks on "esc to interrupt",
// so when the status bar appears below the › input line, the typed text is
// never detected. State stays ReadyEmpty → "idle" even when text is in the box.
func TestCodexPromptDraftDetectsTextWhenStatusBarAppearsBelow(t *testing.T) {
	snapshot := strings.Join([]string{
		"Codex",
		"› hello world",
		"esc to interrupt | for shortcuts",
	}, "\n")
	got := codexPromptDraft(snapshot)
	if got != "hello world" {
		t.Fatalf("expected %q from codexPromptDraft with status bar below input, got %q", "hello world", got)
	}
}

func jsonHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}
