//go:build unix
// +build unix

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/google/uuid"
)

func looksLikeFolderPathQuery(query string) bool {
	if strings.ContainsAny(query, string(os.PathSeparator)+"/") {
		return true
	}
	if strings.HasPrefix(query, ".") || strings.HasPrefix(query, "~") || strings.HasPrefix(query, `\\`) {
		return true
	}
	return len(query) >= 2 && ((query[0] >= 'A' && query[0] <= 'Z') || (query[0] >= 'a' && query[0] <= 'z')) && query[1] == ':'
}

type allowedFolder struct {
	ID           string `json:"id"`
	Label        string `json:"label"`
	DisplayPath  string `json:"display_path"`
	AbsolutePath string `json:"absolute_path"`
	IsGitRepo    bool   `json:"is_git_repo"`
}

type apiManagerFolderRecord struct {
	ID                  string `json:"id"`
	Label               string `json:"label"`
	DisplayPath         string `json:"display_path"`
	IsGitRepo           bool   `json:"is_git_repo"`
	LocalAgentManagerID string `json:"local_agent_manager_id"`
}

func (m *manager) listAllowedFolders(out io.Writer) error {
	folders := m.snapshotAllowedFolders()
	if len(folders) == 0 {
		_, err := fmt.Fprintln(out, "No allowlisted folders configured.")
		return err
	}

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tLABEL\tPATH\tTYPE")
	for _, folder := range folders {
		folderType := "folder"
		if folder.IsGitRepo {
			folderType = "git-repo"
		}
		_, _ = fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\n",
			folder.ID,
			folder.Label,
			folder.DisplayPath,
			folderType,
		)
	}
	return w.Flush()
}

func (m *manager) addAllowedFolder(ctx context.Context, inputPath, label string) (allowedFolder, error) {
	normalized, err := buildAllowedFolder(inputPath, label)
	if err != nil {
		return allowedFolder{}, err
	}
	if err := m.ensureRegistered(ctx); err != nil {
		return allowedFolder{}, err
	}
	managerID := m.snapshotManagerID()
	if managerID == "" {
		return allowedFolder{}, fmt.Errorf("manager id is not initialized")
	}
	payload := map[string]any{
		"displayPath": strings.TrimSpace(inputPath),
		"label":       normalized.Label,
		"isGitRepo":   normalized.IsGitRepo,
	}
	var created apiManagerFolderRecord
	if err := m.managerRequestJSON(
		ctx,
		http.MethodPost,
		"/api/managers/"+managerID+"/folders",
		payload,
		&created,
	); err != nil {
		return allowedFolder{}, friendlyFolderAPIError(err)
	}
	if refreshErr := m.refreshAllowedFoldersFromBackend(ctx); refreshErr != nil {
		return normalized, fmt.Errorf(
			"saved on server but could not refresh local folders: %w",
			friendlyFolderAPIError(refreshErr),
		)
	}
	if refreshed, ok := m.findAllowedFolderByID(created.ID); ok {
		return refreshed, nil
	}
	return normalized, nil
}

func (m *manager) removeAllowedFolder(ctx context.Context, query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return fmt.Errorf("folder id or path is required")
	}

	normalizedPath := ""
	if looksLikeFolderPathQuery(query) {
		if pathValue, err := canonicalizeDirectoryPath(query); err == nil {
			normalizedPath = pathValue
		}
	}

	err := func() error {
		if err := m.refreshAllowedFoldersFromBackend(ctx); err != nil {
			return friendlyFolderAPIError(err)
		}
		managerID := m.snapshotManagerID()
		if managerID == "" {
			return fmt.Errorf("manager id is not initialized")
		}
		folders := m.snapshotAllowedFolders()
		var folderID string
		for _, folder := range folders {
			if folder.ID == query || (normalizedPath != "" && folder.AbsolutePath == normalizedPath) {
				folderID = folder.ID
				break
			}
		}
		if folderID == "" {
			return fmt.Errorf("allowlisted folder not found")
		}
		return friendlyFolderAPIError(m.managerRequestJSON(
			ctx,
			http.MethodDelete,
			"/api/managers/"+managerID+"/folders/"+url.PathEscape(folderID),
			nil,
			nil,
		))
	}()
	if err != nil {
		return err
	}
	if refreshErr := m.refreshAllowedFoldersFromBackend(ctx); refreshErr != nil {
		return fmt.Errorf(
			"removed on server but could not refresh local folders: %w",
			friendlyFolderAPIError(refreshErr),
		)
	}
	return nil
}

func (m *manager) snapshotAllowedFolders() []allowedFolder {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]allowedFolder, len(m.state.AllowedFolders))
	copy(out, m.state.AllowedFolders)
	return out
}

func (m *manager) snapshotManagerID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return strings.TrimSpace(m.state.ManagerID)
}

func (m *manager) handleBrowserAddFolder(
	ctx context.Context,
	client *browserClient,
	clientMessageID,
	path,
	label string,
) error {
	if _, err := m.addAllowedFolder(ctx, path, label); err != nil {
		return err
	}
	return m.writeBrowserEvent(client, map[string]any{
		"type":              "folder_action_complete",
		"client_message_id": strings.TrimSpace(clientMessageID),
	})
}

func (m *manager) refreshAllowedFoldersFromBackend(ctx context.Context) error {
	_, err := m.refreshAllowedFoldersFromBackendDetailed(ctx)
	return err
}

func (m *manager) refreshAllowedFoldersFromBackendDetailed(ctx context.Context) (map[string]error, error) {
	managerID := m.snapshotManagerID()
	if managerID == "" {
		return nil, fmt.Errorf("manager id is not initialized")
	}
	var records []apiManagerFolderRecord
	if err := m.managerRequestJSON(
		ctx,
		http.MethodGet,
		"/manager-folders?managerId="+url.QueryEscape(managerID),
		nil,
		&records,
	); err != nil {
		return nil, err
	}

	next := make([]allowedFolder, 0, len(records))
	issues := make(map[string]error)
	for _, record := range records {
		folder, buildErr := buildAllowedFolderFromRecord(record)
		if buildErr != nil {
			issues[strings.TrimSpace(record.ID)] = describeAllowedFolderRecordError(
				record,
				buildErr,
			)
			m.logWarn(
				"allowed_folder_refresh_skip folder_id=%s path=%q err=%v",
				strings.TrimSpace(record.ID),
				strings.TrimSpace(record.DisplayPath),
				buildErr,
			)
			continue
		}
		next = append(next, folder)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.AllowedFolders = next
	return issues, writeState(m.cfg.StateFile, m.state)
}

func (m *manager) handleBrowserRemoveFolder(
	ctx context.Context,
	client *browserClient,
	clientMessageID,
	folderID string,
) error {
	if err := m.removeAllowedFolder(ctx, folderID); err != nil {
		return err
	}
	return m.writeBrowserEvent(client, map[string]any{
		"type":              "folder_action_complete",
		"client_message_id": strings.TrimSpace(clientMessageID),
	})
}

func (m *manager) resolveTaskWorkingDir(ctx context.Context, task apiTask) (string, error) {
	if strings.TrimSpace(task.SelectedFolderID) == "" {
		return ensureDefaultTaskWorkingDir(m.cfg.StateFile, task.ID)
	}

	folder, err := m.resolveSelectedAllowedFolder(ctx, task.SelectedFolderID)
	if err != nil {
		return "", err
	}

	canonicalPath, err := canonicalizeDirectoryPath(folder.AbsolutePath)
	if err != nil {
		return "", describeLocalFolderError(folder.DisplayPath, err)
	}
	if canonicalPath != folder.AbsolutePath {
		return "", fmt.Errorf(
			"selected folder %q changed on this computer and must be re-allowlisted",
			folder.DisplayPath,
		)
	}

	repoRoot, repoSubpath, inRepo, err := gitRepoContext(canonicalPath)
	if err != nil {
		return "", describeLocalFolderError(folder.DisplayPath, err)
	}
	if inRepo {
		return ensureTaskWorktree(task, repoRoot, repoSubpath)
	}
	return canonicalPath, nil
}

func (m *manager) resolveSelectedAllowedFolder(ctx context.Context, folderID string) (allowedFolder, error) {
	if folder, ok := m.findAllowedFolderByID(folderID); ok {
		return folder, nil
	}
	issues, err := m.refreshAllowedFoldersFromBackendDetailed(ctx)
	if err != nil {
		return allowedFolder{}, err
	}
	if folder, ok := m.findAllowedFolderByID(folderID); ok {
		return folder, nil
	}
	if issue, ok := issues[strings.TrimSpace(folderID)]; ok {
		return allowedFolder{}, issue
	}
	return allowedFolder{}, fmt.Errorf("selected folder is not allowlisted on this manager")
}

func buildAllowedFolderFromRecord(record apiManagerFolderRecord) (allowedFolder, error) {
	canonicalPath, err := canonicalizeDirectoryPath(record.DisplayPath)
	if err != nil {
		return allowedFolder{}, err
	}
	isGitRepo, err := detectGitRepo(canonicalPath)
	if err != nil {
		return allowedFolder{}, err
	}
	label := strings.TrimSpace(record.Label)
	if label == "" {
		label = filepath.Base(canonicalPath)
	}
	if label == "" || label == "." || label == string(os.PathSeparator) {
		label = canonicalPath
	}
	return allowedFolder{
		ID:           strings.TrimSpace(record.ID),
		Label:        label,
		DisplayPath:  displayPathForHome(canonicalPath),
		AbsolutePath: canonicalPath,
		IsGitRepo:    isGitRepo,
	}, nil
}

func describeAllowedFolderRecordError(record apiManagerFolderRecord, err error) error {
	return describeLocalFolderError(strings.TrimSpace(record.DisplayPath), err)
}

func describeLocalFolderError(displayPath string, err error) error {
	displayPath = strings.TrimSpace(displayPath)
	if displayPath == "" {
		displayPath = "selected folder"
	}
	if os.IsNotExist(err) {
		return fmt.Errorf("selected folder %q does not exist on this computer", displayPath)
	}
	return fmt.Errorf("selected folder %q is unavailable on this manager: %w", displayPath, err)
}

func buildAllowedFolder(inputPath, label string) (allowedFolder, error) {
	canonicalPath, err := canonicalizeDirectoryPath(inputPath)
	if err != nil {
		return allowedFolder{}, err
	}
	if strings.TrimSpace(label) == "" {
		label = filepath.Base(canonicalPath)
	}
	if strings.TrimSpace(label) == "" || label == "." || label == string(os.PathSeparator) {
		label = canonicalPath
	}

	isGitRepo, err := detectGitRepo(canonicalPath)
	if err != nil {
		return allowedFolder{}, err
	}

	return allowedFolder{
		ID:           uuid.NewString(),
		Label:        strings.TrimSpace(label),
		DisplayPath:  displayPathForHome(canonicalPath),
		AbsolutePath: canonicalPath,
		IsGitRepo:    isGitRepo,
	}, nil
}

func (m *manager) findAllowedFolderByID(folderID string) (allowedFolder, bool) {
	for _, folder := range m.snapshotAllowedFolders() {
		if folder.ID == strings.TrimSpace(folderID) {
			return folder, true
		}
	}
	return allowedFolder{}, false
}

func friendlyFolderAPIError(err error) error {
	if err == nil {
		return nil
	}
	message := apiErrorMessage(err)
	if message == "" {
		return err
	}
	return fmt.Errorf("%s", message)
}

func apiErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	raw := err.Error()
	idx := strings.Index(raw, "body=")
	if idx < 0 {
		return ""
	}
	body := strings.TrimSpace(raw[idx+len("body="):])
	var payload struct {
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(body), &payload) != nil {
		return ""
	}
	return strings.TrimSpace(payload.Message)
}

func ensureDefaultTaskWorkingDir(stateFilePath, taskID string) (string, error) {
	taskDir := filepath.Join(filepath.Dir(stateFilePath), "tasks", safeTaskToken(taskID))
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		return "", err
	}
	return taskDir, nil
}

func ensureTaskWorktree(task apiTask, repoPath, repoSubpath string) (string, error) {
	repoRoot, err := gitOutput(repoPath, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("selected folder is marked as a git repo but could not be verified: %w", err)
	}
	repoRoot, err = canonicalizeDirectoryPath(strings.TrimSpace(repoRoot))
	if err != nil {
		return "", err
	}
	if repoRoot != repoPath {
		return "", fmt.Errorf("selected folder must be the root of the git repository")
	}

	if err := ensureGitExcludeRule(repoPath, "/.passiveagents-worktrees/"); err != nil {
		return "", err
	}

	worktreePath := filepath.Join(repoPath, ".passiveagents-worktrees", worktreeNameForTask(task))
	if _, err := os.Stat(filepath.Join(worktreePath, ".git")); err == nil {
		return taskWorkingDirWithinWorktree(worktreePath, repoSubpath)
	} else if !os.IsNotExist(err) {
		return "", err
	}

	branchName := worktreeBranchNameForTask(task)
	if existingPath, err := existingWorktreePathForTask(repoPath, task); err != nil {
		return "", err
	} else if existingPath != "" {
		return taskWorkingDirWithinWorktree(existingPath, repoSubpath)
	}

	if err := os.MkdirAll(filepath.Dir(worktreePath), 0o700); err != nil {
		return "", err
	}

	cmd := exec.Command("git", "-C", repoPath, "worktree", "add", "-B", branchName, worktreePath, "HEAD")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git worktree add failed: %s", strings.TrimSpace(string(output)))
	}
	return taskWorkingDirWithinWorktree(worktreePath, repoSubpath)
}

func existingWorktreePathForTask(repoPath string, task apiTask) (string, error) {
	branchNames := []string{worktreeBranchNameForTask(task)}
	if legacy := legacyWorktreeBranchNameForTask(task); legacy != "" {
		branchNames = append(branchNames, legacy)
	}
	for _, branchName := range branchNames {
		existingPath, err := existingWorktreePathForBranch(repoPath, branchName)
		if err != nil {
			return "", err
		}
		if existingPath != "" {
			return existingPath, nil
		}
	}
	return "", nil
}

func taskWorkingDirWithinWorktree(worktreePath, repoSubpath string) (string, error) {
	repoSubpath = strings.TrimSpace(repoSubpath)
	if repoSubpath == "" || repoSubpath == "." {
		return worktreePath, nil
	}
	workingDir := filepath.Join(worktreePath, filepath.FromSlash(repoSubpath))
	info, err := os.Stat(workingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return worktreePath, nil
		}
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("expected %q inside task worktree to be a directory", repoSubpath)
	}
	return workingDir, nil
}

func ensureGitExcludeRule(repoPath, pattern string) error {
	gitCommonDir, err := gitOutput(repoPath, "rev-parse", "--git-common-dir")
	if err != nil {
		return err
	}
	gitCommonDir = strings.TrimSpace(gitCommonDir)
	if !filepath.IsAbs(gitCommonDir) {
		gitCommonDir = filepath.Join(repoPath, gitCommonDir)
	}
	excludePath := filepath.Join(gitCommonDir, "info", "exclude")
	if err := os.MkdirAll(filepath.Dir(excludePath), 0o700); err != nil {
		return err
	}

	raw, err := os.ReadFile(excludePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == pattern {
			return nil
		}
	}

	f, err := os.OpenFile(excludePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if len(raw) > 0 && !strings.HasSuffix(string(raw), "\n") {
		if _, err := f.WriteString("\n"); err != nil {
			return err
		}
	}
	_, err = f.WriteString(pattern + "\n")
	return err
}

func existingWorktreePathForBranch(repoPath, branchName string) (string, error) {
	output, err := gitOutput(repoPath, "worktree", "list", "--porcelain")
	if err != nil {
		return "", err
	}

	var currentPath string
	var currentBranch string
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "worktree "):
			if currentPath != "" && currentBranch == "refs/heads/"+branchName {
				return currentPath, nil
			}
			currentPath = strings.TrimSpace(strings.TrimPrefix(line, "worktree "))
			currentBranch = ""
		case strings.HasPrefix(line, "branch "):
			currentBranch = strings.TrimSpace(strings.TrimPrefix(line, "branch "))
		case line == "":
			if currentPath != "" && currentBranch == "refs/heads/"+branchName {
				return currentPath, nil
			}
			currentPath = ""
			currentBranch = ""
		}
	}
	if currentPath != "" && currentBranch == "refs/heads/"+branchName {
		return currentPath, nil
	}
	return "", nil
}

func canonicalizeDirectoryPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("folder path is required")
	}
	if strings.HasPrefix(path, "~"+string(os.PathSeparator)) || path == "~" {
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			if path == "~" {
				path = home
			} else {
				path = filepath.Join(home, strings.TrimPrefix(path, "~"+string(os.PathSeparator)))
			}
		}
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	cleaned := filepath.Clean(absPath)
	resolved, err := filepath.EvalSymlinks(cleaned)
	if err == nil {
		cleaned = resolved
	} else if !os.IsNotExist(err) {
		return "", err
	}
	info, err := os.Stat(cleaned)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("folder path must be a directory")
	}
	return cleaned, nil
}

func detectGitRepo(path string) (bool, error) {
	root, _, inRepo, err := gitRepoContext(path)
	if err != nil {
		return false, err
	}
	if !inRepo {
		return false, nil
	}
	return root == path, nil
}

func gitRepoContext(path string) (string, string, bool, error) {
	root, err := gitOutput(path, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", "", false, nil
	}
	root, err = canonicalizeDirectoryPath(strings.TrimSpace(root))
	if err != nil {
		return "", "", false, err
	}
	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return "", "", false, err
	}
	relPath = filepath.Clean(relPath)
	if relPath == ".." || strings.HasPrefix(relPath, ".."+string(os.PathSeparator)) {
		return "", "", false, fmt.Errorf("selected folder must be within the git repository")
	}
	return root, filepath.ToSlash(relPath), true, nil
}

func gitOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s", strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func displayPathForHome(path string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	if path == home {
		return "~"
	}
	prefix := home + string(os.PathSeparator)
	if strings.HasPrefix(path, prefix) {
		return "~" + string(os.PathSeparator) + strings.TrimPrefix(path, prefix)
	}
	return path
}

func safeTaskToken(taskID string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(taskID) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "task"
	}
	return b.String()
}

func worktreeNameForTask(task apiTask) string {
	base := safeDescriptiveToken(task.Name)
	if base == "" {
		base = "task"
	}
	if len(base) > 32 {
		base = strings.Trim(base[:32], "-")
	}
	token := shortTaskToken(task.ID)
	if token == "" {
		return base
	}
	return base + "-" + token
}

func worktreeBranchNameForTask(task apiTask) string {
	const prefix = "feature/"

	taskID := safeTaskToken(task.ID)
	description := safeDescriptiveToken(task.Name)
	switch {
	case taskID == "" && description == "":
		return prefix + "task"
	case taskID == "":
		return prefix + description
	case description == "":
		return prefix + taskID
	default:
		return prefix + taskID + "-" + description
	}
}

func legacyWorktreeBranchNameForTask(task apiTask) string {
	token := shortTaskToken(task.ID)
	if token == "" {
		return ""
	}
	return "pa/" + token
}

func safeDescriptiveToken(value string) string {
	var b strings.Builder
	lastHyphen := false
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastHyphen = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastHyphen = false
		case r == ' ' || r == '-' || r == '_' || r == '/' || r == '.':
			if b.Len() > 0 && !lastHyphen {
				b.WriteByte('-')
				lastHyphen = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func shortTaskToken(taskID string) string {
	token := safeTaskToken(taskID)
	if len(token) <= 12 {
		return token
	}
	return token[:12]
}
