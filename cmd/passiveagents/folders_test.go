//go:build unix
// +build unix

package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureDefaultTaskWorkingDirCreatesTaskFolder(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state", "manager-state.json")

	dir, err := ensureDefaultTaskWorkingDir(stateFile, "task-123")
	if err != nil {
		t.Fatalf("ensureDefaultTaskWorkingDir error: %v", err)
	}
	if want := filepath.Join(filepath.Dir(stateFile), "tasks", "task-123"); dir != want {
		t.Fatalf("unexpected task dir: got %s want %s", dir, want)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("expected created task dir, err=%v", err)
	}
}

func TestResolveTaskWorkingDirUsesAllowlistedFolder(t *testing.T) {
	allowedPath := t.TempDir()
	mgr := &manager{
		cfg: config{
			StateFile: filepath.Join(t.TempDir(), "manager-state.json"),
		},
		state: persistedState{
			AllowedFolders: []allowedFolder{
				{
					ID:           "folder-1",
					Label:        "Docs",
					DisplayPath:  allowedPath,
					AbsolutePath: allowedPath,
					IsGitRepo:    false,
				},
			},
		},
	}

	dir, err := mgr.resolveTaskWorkingDir(context.Background(), apiTask{
		ID:               "task-1",
		SelectedFolderID: "folder-1",
	})
	if err != nil {
		t.Fatalf("resolveTaskWorkingDir error: %v", err)
	}
	if dir != allowedPath {
		t.Fatalf("unexpected working dir: got %s want %s", dir, allowedPath)
	}
}

func TestResolveTaskWorkingDirRefreshesMissingSelectedFolderFromBackend(t *testing.T) {
	allowedDir := t.TempDir()
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			StateFile:  stateFile,
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID:      "manager-1",
			AllowedFolders: []allowedFolder{},
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					t.Fatalf("unexpected method: %s", req.Method)
				}
				if got := req.URL.String(); got != "http://example.test/manager-folders?managerId=manager-1" {
					t.Fatalf("unexpected refresh url: %s", got)
				}
				return testJSONResponse(t, http.StatusOK, []map[string]any{
					{
						"id":                     "550e8400-e29b-41d4-a716-446655440010",
						"label":                  "PassiveAgents",
						"display_path":           allowedDir,
						"is_git_repo":            false,
						"local_agent_manager_id": "manager-1",
					},
				}), nil
			}),
		},
	}
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	dir, err := mgr.resolveTaskWorkingDir(context.Background(), apiTask{
		ID:               "task-1",
		SelectedFolderID: "550e8400-e29b-41d4-a716-446655440010",
	})
	if err != nil {
		t.Fatalf("resolveTaskWorkingDir error: %v", err)
	}
	if dir != allowedDir {
		t.Fatalf("unexpected working dir: got %s want %s", dir, allowedDir)
	}
	if len(mgr.state.AllowedFolders) != 1 {
		t.Fatalf("expected refreshed folder cache, got %d entries", len(mgr.state.AllowedFolders))
	}
}

func TestResolveTaskWorkingDirReportsMissingLocalFolderAfterRefresh(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing-folder")
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			StateFile:  stateFile,
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID:      "manager-1",
			AllowedFolders: []allowedFolder{},
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					t.Fatalf("unexpected method: %s", req.Method)
				}
				return testJSONResponse(t, http.StatusOK, []map[string]any{
					{
						"id":                     "550e8400-e29b-41d4-a716-446655440010",
						"label":                  "Missing",
						"display_path":           missingDir,
						"is_git_repo":            false,
						"local_agent_manager_id": "manager-1",
					},
				}), nil
			}),
		},
	}
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	_, err := mgr.resolveTaskWorkingDir(context.Background(), apiTask{
		ID:               "task-1",
		SelectedFolderID: "550e8400-e29b-41d4-a716-446655440010",
	})
	if err == nil {
		t.Fatalf("expected missing local folder error")
	}
	if !strings.Contains(err.Error(), "does not exist on this computer") {
		t.Fatalf("expected missing local folder message, got %v", err)
	}
}

func TestRefreshAllowedFoldersFromBackendHydratesLocalState(t *testing.T) {
	allowedDir := t.TempDir()
	stateFile := filepath.Join(t.TempDir(), "manager-state.json")
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			StateFile:  stateFile,
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID:      "manager-1",
			AllowedFolders: []allowedFolder{},
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					t.Fatalf("unexpected method: %s", req.Method)
				}
				if got := req.URL.String(); got != "http://example.test/manager-folders?managerId=manager-1" {
					t.Fatalf("unexpected refresh url: %s", got)
				}
				return testJSONResponse(t, http.StatusOK, []map[string]any{
					{
						"id":                     "550e8400-e29b-41d4-a716-446655440010",
						"label":                  "PassiveAgents",
						"display_path":           allowedDir,
						"is_git_repo":            false,
						"local_agent_manager_id": "manager-1",
					},
				}), nil
			}),
		},
	}
	if err := writeState(stateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	if err := mgr.refreshAllowedFoldersFromBackend(context.Background()); err != nil {
		t.Fatalf("refreshAllowedFoldersFromBackend error: %v", err)
	}
	if len(mgr.state.AllowedFolders) != 1 {
		t.Fatalf("expected 1 allowlisted folder, got %d", len(mgr.state.AllowedFolders))
	}
	if mgr.state.AllowedFolders[0].AbsolutePath != allowedDir {
		t.Fatalf("unexpected absolute path: %s", mgr.state.AllowedFolders[0].AbsolutePath)
	}
	reloaded, err := readState(mgr.cfg.StateFile)
	if err != nil {
		t.Fatalf("readState error: %v", err)
	}
	if len(reloaded.AllowedFolders) != 1 {
		t.Fatalf("expected 1 persisted folder, got %d", len(reloaded.AllowedFolders))
	}
}

func TestAddAllowedFolderCreatesBackendRowAndRefreshesLocalState(t *testing.T) {
	allowedDir := t.TempDir()
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			StateFile:  filepath.Join(t.TempDir(), "manager-state.json"),
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID:      "manager-1",
			AllowedFolders: []allowedFolder{},
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == http.MethodPost && req.URL.Path == "/local-agent-managers/register":
					return testJSONResponse(t, http.StatusOK, map[string]any{
						"machineId":        "manager-1",
						"managerSubdomain": "manager-1",
						"tunnelId":         "",
						"tunnelToken":      "",
					}), nil
				case req.Method == http.MethodPost && req.URL.Path == "/api/managers/manager-1/folders":
					body, _ := io.ReadAll(req.Body)
					if !strings.Contains(string(body), "\"displayPath\":\""+allowedDir+"\"") {
						t.Fatalf("expected create payload to include displayPath, got %s", string(body))
					}
					return testJSONResponse(t, http.StatusOK, map[string]any{
						"id":                     "550e8400-e29b-41d4-a716-446655440010",
						"label":                  filepath.Base(allowedDir),
						"display_path":           allowedDir,
						"is_git_repo":            false,
						"local_agent_manager_id": "manager-1",
					}), nil
				case req.Method == http.MethodGet && req.URL.String() == "http://example.test/manager-folders?managerId=manager-1":
					return testJSONResponse(t, http.StatusOK, []map[string]any{
						{
							"id":                     "550e8400-e29b-41d4-a716-446655440010",
							"label":                  filepath.Base(allowedDir),
							"display_path":           allowedDir,
							"is_git_repo":            false,
							"local_agent_manager_id": "manager-1",
						},
					}), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
					return nil, nil
				}
			}),
		},
	}
	if err := writeState(mgr.cfg.StateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	folder, err := mgr.addAllowedFolder(context.Background(), allowedDir, "")
	if err != nil {
		t.Fatalf("addAllowedFolder error: %v", err)
	}
	if folder.AbsolutePath != allowedDir {
		t.Fatalf("unexpected absolute path: %s", folder.AbsolutePath)
	}
	if len(mgr.state.AllowedFolders) != 1 {
		t.Fatalf("expected 1 local folder after refresh, got %d", len(mgr.state.AllowedFolders))
	}
}

func TestAddAllowedFolderReturnsFriendlyDuplicateMessage(t *testing.T) {
	allowedDir := t.TempDir()
	mgr := &manager{
		cfg: config{
			APIBaseURL: "http://example.test",
			StateFile:  filepath.Join(t.TempDir(), "manager-state.json"),
			UserJWT:    "token",
		},
		state: persistedState{
			ManagerID:      "manager-1",
			AllowedFolders: []allowedFolder{},
		},
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == http.MethodPost && req.URL.Path == "/local-agent-managers/register":
					return testJSONResponse(t, http.StatusOK, map[string]any{
						"machineId":        "manager-1",
						"managerSubdomain": "manager-1",
						"tunnelId":         "",
						"tunnelToken":      "",
					}), nil
				case req.Method == http.MethodPost && req.URL.Path == "/api/managers/manager-1/folders":
					return testJSONResponse(t, http.StatusBadRequest, map[string]any{
						"message":    "This folder has already been allowlisted",
						"error":      "Bad Request",
						"statusCode": 400,
					}), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
					return nil, nil
				}
			}),
		},
	}
	if err := writeState(mgr.cfg.StateFile, mgr.state); err != nil {
		t.Fatalf("writeState error: %v", err)
	}

	_, err := mgr.addAllowedFolder(context.Background(), allowedDir, "")
	if err == nil {
		t.Fatalf("expected duplicate add failure")
	}
	if !strings.Contains(err.Error(), "already been allowlisted") {
		t.Fatalf("expected friendly duplicate error, got %v", err)
	}
	if len(mgr.state.AllowedFolders) != 0 {
		t.Fatalf("expected local folders to remain empty on duplicate, got %d", len(mgr.state.AllowedFolders))
	}
}

func TestResolveTaskWorkingDirCreatesGitWorktree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "test@example.com")
	runGit(t, repoDir, "config", "user.name", "PassiveAgents Test")
	if err := os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write repo file: %v", err)
	}
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	mgr := &manager{
		cfg: config{
			StateFile: filepath.Join(t.TempDir(), "manager-state.json"),
		},
		state: persistedState{
			AllowedFolders: []allowedFolder{
				{
					ID:           "folder-1",
					Label:        "Repo",
					DisplayPath:  repoDir,
					AbsolutePath: repoDir,
					IsGitRepo:    true,
				},
			},
		},
	}

	dir, err := mgr.resolveTaskWorkingDir(context.Background(), apiTask{
		ID:               "task-1",
		Name:             "Fix queue status",
		SelectedFolderID: "folder-1",
	})
	if err != nil {
		t.Fatalf("resolveTaskWorkingDir error: %v", err)
	}
	if !strings.HasPrefix(dir, filepath.Join(repoDir, ".passiveagents-worktrees")) {
		t.Fatalf("expected repo-local worktree, got %s", dir)
	}
	if !strings.Contains(dir, filepath.Join(repoDir, ".passiveagents-worktrees", "fix-queue-status-task-1")) {
		t.Fatalf("expected descriptive worktree name, got %s", dir)
	}
	if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
		t.Fatalf("expected worktree metadata, err=%v", err)
	}
	currentBranch := strings.TrimSpace(gitOutputForTest(t, dir, "branch", "--show-current"))
	if currentBranch != "feature/task-1-fix-queue-status" {
		t.Fatalf("expected standard worktree branch name, got %s", currentBranch)
	}
	rawExclude, err := os.ReadFile(filepath.Join(repoDir, ".git", "info", "exclude"))
	if err != nil {
		t.Fatalf("read exclude: %v", err)
	}
	if !strings.Contains(string(rawExclude), "/.passiveagents-worktrees/") {
		t.Fatalf("expected worktree exclude entry, got %q", string(rawExclude))
	}
}

func TestResolveTaskWorkingDirReusesExistingTaskBranchWorktree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "test@example.com")
	runGit(t, repoDir, "config", "user.name", "PassiveAgents Test")
	if err := os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write repo file: %v", err)
	}
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	task := apiTask{
		ID:               "1cc5e843-6a05-45ee-b662-a20f8b88d69e",
		Name:             "Manager handles pre-created task worktree issue",
		SelectedFolderID: "folder-1",
	}
	legacyWorktreePath := filepath.Join(repoDir, ".passiveagents-worktrees", safeTaskToken(task.ID))
	runGit(t, repoDir, "worktree", "add", "-B", "pa/"+shortTaskToken(task.ID), legacyWorktreePath, "HEAD")

	mgr := &manager{
		cfg: config{
			StateFile: filepath.Join(t.TempDir(), "manager-state.json"),
		},
		state: persistedState{
			AllowedFolders: []allowedFolder{
				{
					ID:           "folder-1",
					Label:        "Repo",
					DisplayPath:  repoDir,
					AbsolutePath: repoDir,
					IsGitRepo:    true,
				},
			},
		},
	}

	dir, err := mgr.resolveTaskWorkingDir(context.Background(), task)
	if err != nil {
		t.Fatalf("resolveTaskWorkingDir error: %v", err)
	}
	if dir != legacyWorktreePath {
		t.Fatalf("expected existing task branch worktree to be reused, got %s want %s", dir, legacyWorktreePath)
	}
}

func TestResolveTaskWorkingDirReusesExistingStandardTaskBranchWorktree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "test@example.com")
	runGit(t, repoDir, "config", "user.name", "PassiveAgents Test")
	if err := os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write repo file: %v", err)
	}
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	task := apiTask{
		ID:               "task-7",
		Name:             "Docs cleanup",
		SelectedFolderID: "folder-1",
	}
	worktreePath := filepath.Join(repoDir, ".passiveagents-worktrees", "task-7-docs-cleanup")
	runGit(t, repoDir, "worktree", "add", "-B", worktreeBranchNameForTask(task), worktreePath, "HEAD")

	mgr := &manager{
		cfg: config{
			StateFile: filepath.Join(t.TempDir(), "manager-state.json"),
		},
		state: persistedState{
			AllowedFolders: []allowedFolder{
				{
					ID:           "folder-1",
					Label:        "Repo",
					DisplayPath:  repoDir,
					AbsolutePath: repoDir,
					IsGitRepo:    true,
				},
			},
		},
	}

	dir, err := mgr.resolveTaskWorkingDir(context.Background(), task)
	if err != nil {
		t.Fatalf("resolveTaskWorkingDir error: %v", err)
	}
	if dir != worktreePath {
		t.Fatalf("expected existing task branch worktree to be reused, got %s want %s", dir, worktreePath)
	}
}

func TestResolveTaskWorkingDirUsesRepoSubdirectoryWithinTaskWorktree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "test@example.com")
	runGit(t, repoDir, "config", "user.name", "PassiveAgents Test")
	frontendDir := filepath.Join(repoDir, "apps", "web", "frontend")
	if err := os.MkdirAll(frontendDir, 0o700); err != nil {
		t.Fatalf("mkdir frontend dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(frontendDir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write repo file: %v", err)
	}
	runGit(t, repoDir, "add", ".")
	runGit(t, repoDir, "commit", "-m", "init")

	mgr := &manager{
		cfg: config{
			StateFile: filepath.Join(t.TempDir(), "manager-state.json"),
		},
		state: persistedState{
			AllowedFolders: []allowedFolder{
				{
					ID:           "folder-1",
					Label:        "Frontend",
					DisplayPath:  frontendDir,
					AbsolutePath: frontendDir,
					IsGitRepo:    false,
				},
			},
		},
	}

	dir, err := mgr.resolveTaskWorkingDir(context.Background(), apiTask{
		ID:               "task-1234567890abcdef",
		Name:             "Frontend polish",
		SelectedFolderID: "folder-1",
	})
	if err != nil {
		t.Fatalf("resolveTaskWorkingDir error: %v", err)
	}

	worktreeRoot := filepath.Join(repoDir, ".passiveagents-worktrees", "frontend-polish-task-1234567")
	want := filepath.Join(worktreeRoot, "apps", "web", "frontend")
	if dir != want {
		t.Fatalf("unexpected working dir: got %s want %s", dir, want)
	}
	if _, err := os.Stat(filepath.Join(worktreeRoot, ".git")); err != nil {
		t.Fatalf("expected worktree metadata, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "README.md")); err != nil {
		t.Fatalf("expected selected subdirectory contents in worktree, err=%v", err)
	}
}

func TestResolveTaskWorkingDirFallsBackToWorktreeRootWhenSelectedRepoSubdirectoryIsUntracked(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "test@example.com")
	runGit(t, repoDir, "config", "user.name", "PassiveAgents Test")
	if err := os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write repo file: %v", err)
	}
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	untrackedDir := filepath.Join(repoDir, "worktrees")
	if err := os.MkdirAll(untrackedDir, 0o700); err != nil {
		t.Fatalf("mkdir untracked dir: %v", err)
	}

	mgr := &manager{
		cfg: config{
			StateFile: filepath.Join(t.TempDir(), "manager-state.json"),
		},
		state: persistedState{
			AllowedFolders: []allowedFolder{
				{
					ID:           "folder-1",
					Label:        "Worktrees",
					DisplayPath:  untrackedDir,
					AbsolutePath: untrackedDir,
					IsGitRepo:    false,
				},
			},
		},
	}

	dir, err := mgr.resolveTaskWorkingDir(context.Background(), apiTask{
		ID:               "f6f2cc3e-60b9-44b3-a15b-c6f17d1d9609",
		Name:             "Task Detail Modal should open w/ worktrees",
		SelectedFolderID: "folder-1",
	})
	if err != nil {
		t.Fatalf("resolveTaskWorkingDir error: %v", err)
	}

	worktreeRoot := filepath.Join(
		repoDir,
		".passiveagents-worktrees",
		worktreeNameForTask(apiTask{
			ID:   "f6f2cc3e-60b9-44b3-a15b-c6f17d1d9609",
			Name: "Task Detail Modal should open w/ worktrees",
		}),
	)
	if dir != worktreeRoot {
		t.Fatalf("expected fallback to worktree root for untracked repo subdir: got %s want %s", dir, worktreeRoot)
	}
	if _, err := os.Stat(filepath.Join(worktreeRoot, ".git")); err != nil {
		t.Fatalf("expected worktree metadata, err=%v", err)
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(output))
	}
}

func gitOutputForTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(output))
	}
	return string(output)
}
