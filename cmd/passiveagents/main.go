//go:build unix
// +build unix

package main

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/creack/pty"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/spf13/cobra"
	auth "github.com/supabase-community/auth-go"
	"github.com/zalando/go-keyring"
	"golang.org/x/term"
)

var (
	ansiCSIRegex               = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)
	ansiOSCRegex               = regexp.MustCompile(`\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)`)
	splitCSIResidueRegex       = regexp.MustCompile(`\[[0-9;?]*[ -/]*[@-~]`)
	taskCompletedRegex         = regexp.MustCompile(`\[TASK_COMPLETED\]\s*(.*)`)
	needsInputRegex            = regexp.MustCompile(`\[NEEDS_USER_INPUT\]\s*(.*)`)
	openCodeResumeRegex        = regexp.MustCompile(`(?i)\b(opencode\s+-s\s+ses_[A-Za-z0-9_-]+)\b`)
	csiParamResidueRegex       = regexp.MustCompile(`^[0-9;]*[mK]`)
	keyringSet                 = keyring.Set
	keyringGet                 = keyring.Get
	ptyStartWithSize           = pty.StartWithSize
	ptyOpen                    = pty.Open
	ptySetSize                 = pty.Setsize
	cpuTimesFunc               = cpu.Times
	virtualMemoryFunc          = mem.VirtualMemory
	bootstrapPromptDelay       = 5 * time.Second
	geminiBootstrapDelay       = 7 * time.Second
	postBootstrapWorkingGrace  = 7 * time.Second
	shutdownKillRetryBaseDelay = 250 * time.Millisecond
	shutdownKillRetryMaxDelay  = 2 * time.Second
	cmdStart                   = func(cmd *exec.Cmd) error { return cmd.Start() }
	killManagedPIDFunc         = killManagedPID
	managerRuntimeGOOS         = func() string { return runtime.GOOS }
)

const (
	maxTerminalReplayBytes      = 8 * 1024 * 1024
	maxPTYDimension             = 65535
	taskCompletedGrace          = 750 * time.Millisecond
	gracefulShutdownWait        = 60 * time.Second
	openCodeResumeWait          = 1500 * time.Millisecond
	openCodeResumeHintWait      = 5 * time.Second
	openCodeResumeForceKill     = 3500 * time.Millisecond
	openCodeResumeStopWait      = 1500 * time.Millisecond
	orphanRecoveryPollInterval  = 5 * time.Second
	shutdownModeUser            = "user"
	shutdownModeManagerRestart  = "manager_restart"
	shutdownModeIdleTimeout     = "idle_timeout"
	workerIdleTimeout           = 10 * time.Minute // timer check interval
	workerIdleDisplayThreshold  = 10 * time.Minute // show IDLE to frontend after this long idle
	workerIdleShutdownThreshold = 20 * time.Minute // graceful shutdown after this long idle
	shutdownKillRetryAttempts   = 6
)

var refreshTokenMessagePrinted bool

func printRefreshTokenMessage() {
	if refreshTokenMessagePrinted {
		return
	}
	refreshTokenMessagePrinted = true
	fmt.Println("Refresh token invalid or already used. Run 'passiveagents login'.")
}

func missingRecoveryTokenInstructionError() error {
	return fmt.Errorf("manager session recovery is not configured; run 'passiveagents login'")
}

type config struct {
	WebBaseURL           string
	APIBaseURL           string
	UserJWT              string
	MachineName          string
	ExecutionMode        string
	MaxConcurrent        int
	StateFile            string
	DatabaseURL          string
	CPUThreshold         float64
	SystemReserveMB      float64
	MBPerAgent           float64
	PollInterval         time.Duration
	ResourceInterval     time.Duration
	HeartbeatInterval    time.Duration
	LogFlushInterval     time.Duration
	LogFlushBatchSize    int
	PollSnapshotFile     string
	StreamPort           int
	CloudflareTunnelName string
	CloudflaredBinary    string
	SupabaseURL          string
	SupabaseAnonKey      string
	ManagerLogFile       string
	LessonsBaseDir       string
}

type persistedState struct {
	ManagerID            string                     `json:"manager_id"`
	ManagerPID           int                        `json:"manager_pid,omitempty"`
	LegacyDaemonPID      int                        `json:"daemon_pid,omitempty"`
	ManagerRecoveryToken string                     `json:"manager_recovery_token,omitempty"`
	SupabaseURL          string                     `json:"supabase_url,omitempty"`
	SupabaseAnonKey      string                     `json:"supabase_anon_key,omitempty"`
	ManagerSubdomain     string                     `json:"manager_subdomain,omitempty"`
	TunnelToken          string                     `json:"tunnel_token,omitempty"`
	ExpiresAt            string                     `json:"expires_at"`
	Instances            map[string]persistedWorker `json:"instances"`
	AllowedFolders       []allowedFolder            `json:"allowed_folders,omitempty"`
	PersonaLessonHashes  map[string]string          `json:"persona_lesson_hashes,omitempty"`
	LastLessonsPullAt    string                     `json:"last_lessons_pull_at,omitempty"`
}

type storedSession struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	UserID       string `json:"user_id,omitempty"`
	UserEmail    string `json:"user_email,omitempty"`
}

type persistedWorker struct {
	PID                 int    `json:"pid"`
	TaskID              string `json:"task_id"`
	AgentID             string `json:"agent_id"`
	SessionID           string `json:"session_id"`
	Status              string `json:"status,omitempty"`
	LastWorkingAt       string `json:"last_working_at,omitempty"`
	LastIdleAt          string `json:"last_idle_at,omitempty"`
	WorkingDir          string `json:"working_dir,omitempty"`
	RuntimeCommand      string `json:"runtime_command,omitempty"`
	LocalLogFile        string `json:"local_log_file,omitempty"` // raw PTY output
	LocalTranscriptFile string `json:"local_transcript_file,omitempty"`
	LocalCommandFile    string `json:"local_command_file,omitempty"`
	AttachAddr          string `json:"attach_addr,omitempty"`
	ResumeCommand       string `json:"resume_command,omitempty"`
}

type apiAgentInstanceStatus struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	CurrentTaskID string `json:"current_task_id"`
}

type manager struct {
	cfg                     config
	state                   persistedState
	previousManagerPID      int
	client                  *http.Client
	logger                  *slog.Logger
	mu                      sync.Mutex
	cpu                     float64
	lastCPUTimes            *cpu.TimesStat
	freeMB                  float64
	freePct                 float64
	totalMB                 float64
	cpuHighStreak           int
	lastSpawnAt             time.Time
	pollNow                 chan struct{}
	workers                 map[string]*worker
	sessionRefreshMu        sync.Mutex
	sessionRefreshRetryAt   time.Time
	browserClients          map[*browserClient]struct{}
	browserMu               sync.RWMutex
	streamServer            *http.Server
	streamListener          net.Listener
	tunnelCmd               *exec.Cmd
	tunnelMu                sync.Mutex
	tunnelID                string
	tunnelReady             atomic.Bool
	instanceSpawnLocks      map[string]*sync.Mutex
	managerSubdomain        string
	tunnelToken             string
	userID                  string
	supabaseBootstrapSynced bool
	browserPreferredSizes   map[string]terminalSize
	refreshSessionHook      func(context.Context, string) (storedSession, error)
}

type worker struct {
	instanceID              string
	taskID                  string
	agentID                 string
	sessionID               string
	workingDir              string
	runtimeCommand          string
	cmd                     *exec.Cmd
	input                   io.WriteCloser
	output                  io.ReadCloser
	logs                    chan logEntry
	stopBatch               chan struct{}
	outputBuffer            *outputRingBuffer
	terminalState           *vtScreenState
	terminalStateMu         sync.Mutex
	terminalOutputSeq       uint64
	terminalReplayEventSeq  uint64
	terminalReplayBuffer    *terminalReplayBuffer
	terminalCols            int
	terminalRows            int
	terminalSizeLocked      bool
	sawTaskCompleted        bool
	sawNeedsUserInput       bool
	pendingTaskCompleted    string
	pendingTaskCompletedAt  time.Time
	localLogFilePath        string
	localTranscriptFilePath string
	localCommandFilePath    string
	usesPTY                 bool
	assistantMu             sync.Mutex
	assistantBuf            strings.Builder
	assistantLastFragmentAt time.Time
	assistantStop           chan struct{}
	attachListener          net.Listener
	attachClients           map[net.Conn]struct{}
	attachMu                sync.Mutex
	deliveredCommandIDs     map[string]struct{}
	deliveredCommandMu      sync.Mutex
	bootstrapPromptPending  bool
	bootstrapPromptMu       sync.Mutex
	shutdownRequested       bool
	shutdownPrompt          string
	shutdownMode            string
	shutdownMu              sync.Mutex
	resourceMu              sync.Mutex
	startedAt               time.Time
	observedRSSMB           float64
	observedCPUCores        float64
	lastResourceSampleAt    time.Time
	idleTimer               *time.Timer
	idleStartedAt           time.Time // first moment agent entered ready-empty; zero when busy
	idleTimerMu             sync.Mutex
	done                    chan struct{}
	tracked                 atomic.Bool
	livestreamReady         atomic.Bool
	bootstrapObserved       atomic.Bool
	bootstrapObservedAt     atomic.Int64
	bootstrapReadyEmptySeen atomic.Bool
}

type browserClient struct {
	conn               *websocket.Conn
	authenticated      bool
	authenticatedUser  string
	subscribedInstance map[string]struct{}
	subscriptionMu     sync.RWMutex
	lifecycleMu        sync.RWMutex
	writeMu            sync.Mutex
	outbound           chan map[string]any
	closed             bool
	closeOnce          sync.Once
}

const (
	browserStreamPongWait     = 70 * time.Second
	browserStreamPingInterval = 25 * time.Second
	browserStreamWriteWait    = 10 * time.Second
)

type outputRingBuffer struct {
	mu      sync.Mutex
	maxSize int
	buf     []byte
}

type terminalSize struct {
	cols int
	rows int
}

type terminalReplayChunk struct {
	eventID   uint64
	sequence  uint64
	kind      string
	data      []byte
	cols      int
	rows      int
	sizeBytes int
}

type terminalReplayBuffer struct {
	maxBytes   int
	totalBytes int
	chunks     []terminalReplayChunk
}

type workerPromptState string

const (
	workerPromptStateBusy           workerPromptState = "busy"
	workerPromptStateReadyEmpty     workerPromptState = "ready-empty"
	workerPromptStateReadyWithDraft workerPromptState = "ready-with-draft"
)

type workerPromptStatus struct {
	State    workerPromptState
	Snapshot string
	Draft    string
}

func (b *terminalReplayBuffer) AppendOutput(eventID, sequence uint64, data []byte) {
	if b == nil || len(data) == 0 {
		return
	}
	b.appendChunk(terminalReplayChunk{
		eventID:   eventID,
		sequence:  sequence,
		kind:      "output",
		data:      append([]byte(nil), data...),
		sizeBytes: len(data),
	})
}

func (b *terminalReplayBuffer) AppendResize(eventID uint64, cols, rows int) {
	if b == nil || cols <= 0 || rows <= 0 {
		return
	}
	b.appendChunk(terminalReplayChunk{
		eventID:   eventID,
		kind:      "resize",
		cols:      cols,
		rows:      rows,
		sizeBytes: 16,
	})
}

func (b *terminalReplayBuffer) appendChunk(chunk terminalReplayChunk) {
	b.chunks = append(b.chunks, chunk)
	b.totalBytes += chunk.sizeBytes
	for b.totalBytes > b.maxBytes && len(b.chunks) > 1 {
		b.totalBytes -= b.chunks[0].sizeBytes
		b.chunks = b.chunks[1:]
	}
}

func (b *terminalReplayBuffer) Snapshot(endEventID uint64) []terminalReplayChunk {
	if b == nil || len(b.chunks) == 0 {
		return nil
	}
	chunks := make([]terminalReplayChunk, 0, len(b.chunks))
	for _, chunk := range b.chunks {
		if endEventID > 0 && chunk.eventID > endEventID {
			break
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}

func (r *outputRingBuffer) Write(p string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	if len(r.buf) > r.maxSize {
		r.buf = r.buf[len(r.buf)-r.maxSize:]
	}
}

func (r *outputRingBuffer) Last(n int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n <= 0 || len(r.buf) == 0 {
		return ""
	}
	if n >= len(r.buf) {
		return string(r.buf)
	}
	return string(r.buf[len(r.buf)-n:])
}

func normalizePromptDraftText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func normalizePromptDraftComparison(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), ""))
}

func (w *worker) markBootstrapPromptPending(prompt string) {
	if w == nil || !w.usesPTY {
		return
	}
	w.bootstrapPromptMu.Lock()
	defer w.bootstrapPromptMu.Unlock()
	w.bootstrapPromptPending = true
}

func (w *worker) clearBootstrapPromptPending() {
	if w == nil {
		return
	}
	w.bootstrapPromptMu.Lock()
	defer w.bootstrapPromptMu.Unlock()
	w.bootstrapPromptPending = false
}

func (w *worker) markShutdownRequested(prompt, mode string) bool {
	if w == nil {
		return false
	}
	w.shutdownMu.Lock()
	defer w.shutdownMu.Unlock()
	if w.shutdownRequested {
		return false
	}
	w.shutdownRequested = true
	w.shutdownPrompt = strings.TrimSpace(prompt)
	w.shutdownMode = strings.TrimSpace(mode)
	return true
}

func (w *worker) shutdownRequestDetails() (bool, string, string) {
	if w == nil {
		return false, "", ""
	}
	w.shutdownMu.Lock()
	defer w.shutdownMu.Unlock()
	return w.shutdownRequested, w.shutdownPrompt, w.shutdownMode
}

func (w *worker) clearShutdownRequested() {
	if w == nil {
		return
	}
	w.shutdownMu.Lock()
	defer w.shutdownMu.Unlock()
	w.shutdownRequested = false
	w.shutdownPrompt = ""
	w.shutdownMode = ""
}

func (w *worker) resetIdleTimer() {
	w.idleTimerMu.Lock()
	defer w.idleTimerMu.Unlock()
	if w.idleTimer != nil {
		w.idleTimer.Reset(workerIdleTimeout)
	}
}

func (w *worker) stopIdleTimer() {
	w.idleTimerMu.Lock()
	defer w.idleTimerMu.Unlock()
	if w.idleTimer != nil {
		w.idleTimer.Stop()
	}
}

func (w *worker) markCommandDelivered(commandID string) {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return
	}
	w.deliveredCommandMu.Lock()
	w.deliveredCommandIDs[commandID] = struct{}{}
	w.deliveredCommandMu.Unlock()
}

func (w *worker) hasDeliveredCommand(commandID string) bool {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return false
	}
	w.deliveredCommandMu.Lock()
	_, ok := w.deliveredCommandIDs[commandID]
	w.deliveredCommandMu.Unlock()
	return ok
}

type logEntry struct {
	Line      string `json:"line"`
	LogType   string `json:"log_type"`
	Timestamp string `json:"timestamp"`
}

func appendRawLocalLogChunk(path string, chunk []byte) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(chunk); err != nil {
		return err
	}
	return nil
}

func appendTranscriptLocalLogEntry(path string, entry logEntry) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return nil
}

type apiAgentPersona struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Role         string   `json:"role"`
	Personality  string   `json:"personality"`
	Instructions string   `json:"instructions"`
	Examples     string   `json:"examples"`
	Guardrails   string   `json:"guardrails"`
	RuntimeID    string   `json:"runtime_id"`
	Lessons      []string `json:"lessons"`
	AgentRuntime struct {
		Provider        string `json:"provider"`
		CommandTemplate string `json:"command_template"`
	} `json:"agent_runtimes"`
}

type apiTask struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Description      string   `json:"description"`
	EligibleAgentIDs []string `json:"eligible_agent_ids"`
	SelectedFolderID string   `json:"selected_folder_id"`
	Status           string   `json:"status"`
	AssignedInstance string   `json:"assigned_agent_instance_id"`
}

type apiTaskCheckpoint struct {
	ID             string `json:"id"`
	CheckpointType string `json:"checkpoint_type"`
	CheckpointText string `json:"checkpoint_text"`
	CreatedAt      string `json:"created_at"`
}

type clientBootstrapConfig struct {
	SupabaseURL     string `json:"supabaseUrl"`
	SupabaseAnonKey string `json:"supabaseAnonKey"`
}

const (
	defaultWebBaseURL              = "https://passiveagents.com"
	defaultAPIBaseURL              = "https://api.passiveagents.com"
	keyringService                 = "passiveagents"
	keyringUser                    = "default-session"
	keyringRecoveryTokenUser       = "manager-recovery-token"
	tokenRefreshSkew               = 5 * time.Minute
	sessionRefreshRateLimitBackoff = 30 * time.Second
	authFileName                   = "auth.json"
	pollSnapshotFileName           = "polled-task-batch.json"
	cpuHighStreakThreshold         = 3
	ramUtilizationCapPct           = 80
	defaultSteadyAgentCPUCores     = 0.25
	defaultStartupAgentCPUCores    = 0.5
	workerStartupObservationWindow = 45 * time.Second
	startupResourceCostMultiplier  = 1.25
	spawnCooldown                  = 5 * time.Second
)

var (
	managerVersion        = "dev"
	managerInstallChannel = "dev-source"
)

func main() {
	cfg := loadConfig()
	root := newRootCommand(cfg)
	if err := root.Execute(); err != nil {
		fmt.Println(cleanCLIError(err))
		os.Exit(1)
	}
}

func formattedManagerVersion() string {
	return fmt.Sprintf(
		"passiveagents %s (%s/%s, %s)",
		currentManagerVersion(),
		runtime.GOOS,
		runtime.GOARCH,
		currentInstallChannel(),
	)
}

func newRootCommand(cfg config) *cobra.Command {
	root := &cobra.Command{Use: "passiveagents"}
	root.SilenceUsage = true
	root.Version = strings.TrimPrefix(formattedManagerVersion(), "passiveagents ")
	root.SetVersionTemplate("passiveagents {{.Version}}\n")
	root.SetHelpTemplate(`Usage:
  {{.UseLine}}

Available Commands:
{{range .Commands}}{{if (and (not .Hidden) .IsAvailableCommand)}}  {{rpad .Use 34 }} {{.Short}}
{{end}}{{end}}{{if .HasAvailableLocalFlags}}
Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}

Use "{{.CommandPath}} [command] --help" for more information about a command.
`)
	root.PersistentFlags().StringVar(
		&cfg.WebBaseURL,
		"web-url",
		cfg.WebBaseURL,
		"PassiveAgents base URL (web login)",
	)
	root.PersistentFlags().StringVar(
		&cfg.APIBaseURL,
		"api-url",
		cfg.APIBaseURL,
		"PassiveAgents API base URL",
	)

	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Show manager build version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", formattedManagerVersion())
			return nil
		},
	})

	root.AddCommand(&cobra.Command{
		Use:   "login",
		Short: "Authenticate via browser and store session locally",
		RunE: func(cmd *cobra.Command, args []string) error {
			m, err := newManager(cfg)
			if err != nil {
				return err
			}
			return m.login(cmd.Context())
		},
	})

	root.AddCommand(&cobra.Command{
		Use:   "start",
		Short: "Start PassiveAgents local manager",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := ensurePersistedManagerRecoveryToken(cfg.StateFile); err != nil {
				return err
			}
			return startManagerProcess(cfg)
		},
	})

	root.AddCommand(&cobra.Command{
		Use:    "run-manager",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runManager(cmd.Context(), cfg)
		},
	})

	tasksCmd := &cobra.Command{
		Use:   "tasks",
		Short: "List tasks being worked on and READY tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			m, err := newManager(cfg)
			if err != nil {
				return err
			}
			watch, _ := cmd.Flags().GetBool("watch")
			if !watch {
				return m.listWorkingTasks(cmd.Context())
			}
			lastOutput := ""
			lastLines := 0
			for {
				output, err := m.renderWorkingTasks(cmd.Context())
				if err != nil {
					return err
				}
				if output != lastOutput {
					if lastOutput != "" {
						fmt.Printf("\033[%dA\033[J", lastLines)
					}
					fmt.Print(output)
					lastOutput = output
					lastLines = strings.Count(output, "\n")
					if lastLines == 0 {
						lastLines = 1
					}
				}
				select {
				case <-cmd.Context().Done():
					return nil
				case <-time.After(2 * time.Second):
				}
			}
		},
	}
	tasksCmd.Flags().Bool("watch", false, "watch tasks")
	tasksCmd.AddCommand(&cobra.Command{
		Use:   "reset <task-id>",
		Short: "Detach working agents from task and reset task to READY",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			m, err := newManager(cfg)
			if err != nil {
				return err
			}
			return m.resetTaskForTesting(cmd.Context(), args[0])
		},
	})
	root.AddCommand(tasksCmd)

	root.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show manager status",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := ensurePersistedManagerRecoveryToken(cfg.StateFile); err != nil {
				return err
			}
			supervisor := installedManagerSupervisor(cfg)
			pid, err := readManagerPIDFromState(cfg.StateFile)
			if err != nil {
				if supervisor != "" {
					fmt.Printf("Status: stopped (service installed: %s)\n", supervisor)
					return nil
				}
				fmt.Println("Status: stopped")
				return nil
			}
			if !isProcessRunning(pid) {
				if supervisor != "" {
					fmt.Printf("Status: stopped (stale pid %d, service installed: %s)\n", pid, supervisor)
					return nil
				}
				fmt.Printf("Status: stopped (stale pid %d)\n", pid)
				return nil
			}
			if supervisor != "" {
				fmt.Printf("Status: manager running (PID: %d, managed by %s)\n", pid, supervisor)
				return nil
			}
			fmt.Printf("Status: manager running (PID: %d)\n", pid)
			return nil
		},
	})

	chatCmd := &cobra.Command{
		Use:   "chat agent <AGENT_INSTANCE_ID>",
		Short: "Chat with a running agent instance and stream its history",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(strings.ToLower(args[0])) != "agent" {
				return fmt.Errorf("unknown chat target: %s (supported: agent)", args[0])
			}
			m, err := newManager(cfg)
			if err != nil {
				return err
			}
			return m.chatWithInstance(cmd.Context(), args[1])
		},
	}
	root.AddCommand(chatCmd)

	listCmd := &cobra.Command{
		Use:   "list agents",
		Short: "List local agent instances",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(strings.ToLower(args[0])) != "agents" {
				return fmt.Errorf("unknown list target: %s (supported: agents)", args[0])
			}
			m, err := newManager(cfg)
			if err != nil {
				return err
			}
			return m.listAgentInstances(cmd.Context())
		},
	}
	root.AddCommand(listCmd)

	managerCmd := &cobra.Command{
		Use:   "manager",
		Short: "Manager operations",
	}
	managerLogsCmd := &cobra.Command{
		Use:   "logs",
		Short: "Show local manager logs",
		RunE: func(cmd *cobra.Command, args []string) error {
			watchLogs, _ := cmd.Flags().GetBool("watch")
			return tailManagerLogs(cfg.ManagerLogFile, watchLogs)
		},
	}
	managerLogsCmd.Flags().Bool("watch", false, "watch logs")
	managerCmd.AddCommand(managerLogsCmd)
	root.AddCommand(managerCmd)

	foldersAddLabel := ""
	runAddFolder := func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(strings.ToLower(args[0])) != "folder" {
			return fmt.Errorf("unknown add target: %s (supported: folder)", args[0])
		}
		m, err := newManager(cfg)
		if err != nil {
			return err
		}
		folder, addErr := m.addAllowedFolder(cmd.Context(), args[1], foldersAddLabel)
		if addErr != nil && isRefreshTokenRotationError(addErr) {
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			if root := cmd.Root(); root != nil {
				root.SilenceErrors = true
				root.SilenceUsage = true
			}
			printRefreshTokenMessage()
			os.Exit(1)
		}
		if addErr != nil && !strings.Contains(addErr.Error(), "saved on server but could not refresh local folders") {
			return addErr
		}
		fmt.Printf("Allowlisted folder: %s (%s)\n", folder.Label, folder.DisplayPath)
		if addErr != nil {
			fmt.Printf("Warning: %s\n", cleanCLIError(addErr))
		}
		return nil
	}
	addCmd := &cobra.Command{
		Use:   "add folder <folder-path>",
		Short: "Allowlist a folder for task execution",
		Args:  cobra.ExactArgs(2),
		RunE:  runAddFolder,
	}
	addCmd.SetUsageTemplate(`Usage:
  {{.UseLine}}

{{if .HasAvailableLocalFlags}}Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}
Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}
`)
	addCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
	})
	addCmd.Flags().StringVar(&foldersAddLabel, "label", "", "optional folder label")
	runRemoveFolder := func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(strings.ToLower(args[0])) != "folder" {
			return fmt.Errorf("unknown remove target: %s (supported: folder)", args[0])
		}
		m, err := newManager(cfg)
		if err != nil {
			return err
		}
		folderID := args[1]
		removeErr := m.removeAllowedFolder(cmd.Context(), folderID)
		if removeErr != nil && !strings.Contains(removeErr.Error(), "removed on server but could not refresh local folders") {
			return removeErr
		}
		fmt.Println("Removed allowlisted folder.")
		if removeErr != nil {
			fmt.Printf("Warning: %s\n", cleanCLIError(removeErr))
		}
		return nil
	}
	removeFolderRootCmd := &cobra.Command{
		Use:   "remove folder <folder-id-or-path>",
		Short: "Remove an allowlisted folder",
		Args:  cobra.ExactArgs(2),
		RunE:  runRemoveFolder,
	}

	root.AddCommand(addCmd, removeFolderRootCmd)

	agentLogsWatch := false
	agentLogsRaw := false
	agentLogsCmd := &cobra.Command{
		Use:   "agent logs <agent-instance-id>",
		Short: "Show logs for a local agent instance",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(strings.ToLower(args[0])) != "logs" {
				return fmt.Errorf("unknown agent action: %s (supported: logs)", args[0])
			}
			return tailLocalAgentLogs(cfg.StateFile, args[1], agentLogsWatch, agentLogsRaw)
		},
	}
	agentLogsCmd.Flags().BoolVar(&agentLogsWatch, "watch", false, "watch logs")
	agentLogsCmd.Flags().BoolVar(&agentLogsRaw, "raw", false, "show raw PTY output")
	root.AddCommand(agentLogsCmd)

	root.AddCommand(&cobra.Command{
		Use:   "stop",
		Short: "Stop PassiveAgents local manager",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := ensurePersistedManagerRecoveryToken(cfg.StateFile); err != nil {
				return err
			}
			return stopManagerProcess(cfg)
		},
	})
	return root
}

func loadConfig() config {
	home, _ := os.UserHomeDir()
	stateFile := filepath.Join(home, ".passiveagents", "manager-state.json")
	lessonsBaseDir := filepath.Join(home, ".passiveagents", "agents")
	managerLogFile := filepath.Join(home, ".passiveagents", "manager.log")
	pollSnapshotFile := filepath.Join(home, ".passiveagents", pollSnapshotFileName)
	systemReserveMB, mbPerAgent := estimateMemoryTuning()
	maxConcurrent := 1
	maxConcurrentFromEnv := false
	if raw := os.Getenv("PASSIVEAGENTS_MAX_CONCURRENT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			maxConcurrent = parsed
			maxConcurrentFromEnv = true
		}
	}

	machineName, _ := os.Hostname()
	if machineName == "" {
		machineName = "unknown"
	}

	cfg := config{
		WebBaseURL:           strings.TrimSuffix(defaultWebBaseURL, "/"),
		APIBaseURL:           strings.TrimSuffix(defaultAPIBaseURL, "/"),
		UserJWT:              os.Getenv("PASSIVEAGENTS_USER_JWT"),
		MachineName:          machineName,
		ExecutionMode:        getenv("PASSIVEAGENTS_EXECUTION_MODE", "LOCAL"),
		MaxConcurrent:        maxConcurrent,
		StateFile:            getenv("PASSIVEAGENTS_STATE_FILE", stateFile),
		DatabaseURL:          os.Getenv("PASSIVEAGENTS_DATABASE_URL"),
		CPUThreshold:         80,
		SystemReserveMB:      systemReserveMB,
		MBPerAgent:           mbPerAgent,
		PollInterval:         60 * time.Second,
		ResourceInterval:     1 * time.Second,
		HeartbeatInterval:    30 * time.Second,
		LogFlushInterval:     100 * time.Millisecond,
		LogFlushBatchSize:    50,
		PollSnapshotFile:     pollSnapshotFile,
		SupabaseURL:          strings.TrimSuffix(strings.TrimSpace(os.Getenv("PASSIVEAGENTS_SUPABASE_URL")), "/"),
		SupabaseAnonKey:      strings.TrimSpace(os.Getenv("PASSIVEAGENTS_SUPABASE_ANON_KEY")),
		CloudflaredBinary:    strings.TrimSpace(os.Getenv("PASSIVEAGENTS_CLOUDFLARED_PATH")),
		ManagerLogFile:       getenv("PASSIVEAGENTS_MANAGER_LOG_FILE", managerLogFile),
		StreamPort:           getenvInt("PASSIVEAGENTS_STREAM_PORT", 0),
		CloudflareTunnelName: getenv("PASSIVEAGENTS_CLOUDFLARE_TUNNEL_NAME", "passiveagents"),
		LessonsBaseDir:       getenv("PASSIVEAGENTS_LESSONS_BASE_DIR", lessonsBaseDir),
	}

	if state, err := readState(stateFile); err == nil {
		if cfg.SupabaseURL == "" && strings.TrimSpace(state.SupabaseURL) != "" {
			cfg.SupabaseURL = strings.TrimRight(strings.TrimSpace(state.SupabaseURL), "/")
		}
		if cfg.SupabaseAnonKey == "" && strings.TrimSpace(state.SupabaseAnonKey) != "" {
			cfg.SupabaseAnonKey = strings.TrimSpace(state.SupabaseAnonKey)
		}
	}
	if !maxConcurrentFromEnv {
		cfg.MaxConcurrent = calculateDynamicMaxConcurrent(
			cfg.SystemReserveMB,
			cfg.MBPerAgent,
		)
	}

	return cfg
}

func currentManagerVersion() string {
	version := strings.TrimSpace(managerVersion)
	if version == "" {
		return "dev"
	}
	return version
}

func currentInstallChannel() string {
	channel := strings.TrimSpace(managerInstallChannel)
	if channel == "" {
		return "dev-source"
	}
	return channel
}

func estimateMemoryTuning() (systemReserveMB, mbPerAgent float64) {
	const (
		defaultReserveMB = 1024.0
		defaultPerAgent  = 1024.0
		minReserveMB     = 512.0
		minPerAgentMB    = 512.0
	)

	vm, err := mem.VirtualMemory()
	if err != nil || vm.Total == 0 {
		return defaultReserveMB, defaultPerAgent
	}

	totalMB := float64(vm.Total) / 1024 / 1024
	reserve := totalMB * 0.10
	if reserve < defaultReserveMB {
		reserve = defaultReserveMB
	}
	if reserve < minReserveMB {
		reserve = minReserveMB
	}
	if reserve > totalMB*0.35 {
		reserve = totalMB * 0.35
	}

	perAgent := totalMB * 0.06
	proc, procErr := process.NewProcess(int32(os.Getpid()))
	if procErr == nil {
		if memInfo, memErr := proc.MemoryInfo(); memErr == nil && memInfo != nil {
			rssMB := float64(memInfo.RSS) / 1024 / 1024
			if rssMB > 0 && rssMB*2 > perAgent {
				perAgent = rssMB * 2
			}
		}
	}
	if perAgent < minPerAgentMB {
		perAgent = minPerAgentMB
	}

	availableForAgents := totalMB - reserve
	if availableForAgents <= minPerAgentMB {
		return reserve, minPerAgentMB
	}
	if perAgent >= availableForAgents {
		perAgent = availableForAgents / 2
	}
	if perAgent < minPerAgentMB {
		perAgent = minPerAgentMB
	}

	return reserve, perAgent
}

func newManager(cfg config) (*manager, error) {
	state, err := readState(cfg.StateFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if state.Instances == nil {
		state.Instances = map[string]persistedWorker{}
	}
	if state.AllowedFolders == nil {
		state.AllowedFolders = []allowedFolder{}
	}
	if state.PersonaLessonHashes == nil {
		state.PersonaLessonHashes = map[string]string{}
	}
	return &manager{
		cfg:    cfg,
		state:  state,
		client: &http.Client{Timeout: 20 * time.Second},
		logger: slog.New(
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		),
		pollNow:               make(chan struct{}, 1),
		workers:               map[string]*worker{},
		browserClients:        map[*browserClient]struct{}{},
		instanceSpawnLocks:    map[string]*sync.Mutex{},
		browserPreferredSizes: map[string]terminalSize{},
		managerSubdomain:      state.ManagerSubdomain,
		tunnelToken:           state.TunnelToken,
	}, nil
}

func (m *manager) ensureSupabaseBootstrap(ctx context.Context) error {
	m.mu.Lock()
	cachedURL := strings.TrimSpace(m.cfg.SupabaseURL)
	cachedKey := strings.TrimSpace(m.cfg.SupabaseAnonKey)
	apiBaseURL := strings.TrimRight(strings.TrimSpace(m.cfg.APIBaseURL), "/")
	bootstrapSynced := m.supabaseBootstrapSynced
	if bootstrapSynced && cachedURL != "" && cachedKey != "" {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	if apiBaseURL == "" {
		if cachedURL != "" && cachedKey != "" {
			m.mu.Lock()
			m.supabaseBootstrapSynced = true
			m.mu.Unlock()
			return nil
		}
		return fmt.Errorf("api base url is required to fetch client bootstrap config")
	}

	bootstrap, err := m.fetchClientBootstrapConfig(ctx, apiBaseURL)
	if err != nil {
		if cachedURL != "" && cachedKey != "" {
			m.mu.Lock()
			m.supabaseBootstrapSynced = true
			m.mu.Unlock()
			return nil
		}
		return err
	}

	bootstrap.SupabaseURL = strings.TrimRight(strings.TrimSpace(bootstrap.SupabaseURL), "/")
	bootstrap.SupabaseAnonKey = strings.TrimSpace(bootstrap.SupabaseAnonKey)
	if bootstrap.SupabaseURL == "" || bootstrap.SupabaseAnonKey == "" {
		return fmt.Errorf("client bootstrap config is incomplete")
	}

	m.mu.Lock()
	m.cfg.SupabaseURL = bootstrap.SupabaseURL
	m.cfg.SupabaseAnonKey = bootstrap.SupabaseAnonKey
	m.state.SupabaseURL = bootstrap.SupabaseURL
	m.state.SupabaseAnonKey = bootstrap.SupabaseAnonKey
	m.supabaseBootstrapSynced = true
	m.mu.Unlock()

	if err := persistSupabaseConfigInState(
		m.cfg.StateFile,
		bootstrap.SupabaseURL,
		bootstrap.SupabaseAnonKey,
	); err != nil {
		return err
	}

	return nil
}

func (m *manager) fetchClientBootstrapConfig(ctx context.Context, apiBaseURL string) (clientBootstrapConfig, error) {
	bootstrap, err := m.fetchClientBootstrapConfigFromPath(ctx, apiBaseURL, "/version/client-config")
	if err == nil {
		return bootstrap, nil
	}

	return m.fetchClientBootstrapConfigFromPath(ctx, apiBaseURL, "/api/version/client-config")
}

func (m *manager) fetchClientBootstrapConfigFromPath(
	ctx context.Context,
	apiBaseURL, path string,
) (clientBootstrapConfig, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		apiBaseURL+path,
		nil,
	)
	if err != nil {
		return clientBootstrapConfig{}, err
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return clientBootstrapConfig{}, fmt.Errorf("fetch client bootstrap config failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return clientBootstrapConfig{}, fmt.Errorf(
			"fetch client bootstrap config failed: status=%d body=%s",
			resp.StatusCode,
			strings.TrimSpace(buf.String()),
		)
	}

	var bootstrap clientBootstrapConfig
	if err := json.NewDecoder(resp.Body).Decode(&bootstrap); err != nil {
		return clientBootstrapConfig{}, fmt.Errorf("decode client bootstrap config failed: %w", err)
	}

	return bootstrap, nil
}

func (m *manager) login(ctx context.Context) error {
	if err := m.ensureSupabaseBootstrap(ctx); err != nil {
		return err
	}
	state, err := generateLoginState()
	if err != nil {
		return err
	}
	verifier, challenge, err := generatePKCEPair()
	if err != nil {
		return err
	}

	var initResp struct {
		ShortID string `json:"shortId"`
	}
	if err := m.requestJSON(
		ctx,
		http.MethodPost,
		"/auth/init",
		map[string]any{
			"challenge": challenge,
			"state":     state,
			"port":      54321, // Dummy port for backward compatibility
		},
		&initResp,
		"",
	); err != nil {
		return err
	}
	if strings.TrimSpace(initResp.ShortID) == "" {
		return fmt.Errorf("auth init failed: missing short id")
	}
	loginURL := strings.TrimSuffix(m.cfg.WebBaseURL, "/") + "/login?id=" + url.QueryEscape(initResp.ShortID)

	fmt.Printf("? Press Enter to open %s in your browser... ", strings.TrimSuffix(m.cfg.WebBaseURL, "/"))
	_, _ = bufio.NewReader(os.Stdin).ReadBytes('\n')
	if shouldAttemptBrowserOpen() {
		if err := tryOpenBrowser(loginURL); err != nil {
			fmt.Println("Please open this URL in your browser:")
			fmt.Printf("%s\n", loginURL)
		}
	} else {
		fmt.Println("Manual browser mode detected.")
		fmt.Println("Open this URL in your browser:")
		fmt.Printf("%s\n", loginURL)
	}

	fmt.Println("Waiting for authentication in browser...")

	pollTicker := time.NewTicker(2 * time.Second)
	defer pollTicker.Stop()

	timeout := time.After(10 * time.Minute)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("login timed out after 10 minutes")
		case <-pollTicker.C:
			var pollResp struct {
				Pending      bool   `json:"pending"`
				AccessToken  string `json:"accessToken"`
				RefreshToken string `json:"refreshToken"`
				ExpiresAt    int64  `json:"expiresAt"`
				UserID       string `json:"userId"`
			}
			err := m.requestJSON(
				ctx,
				http.MethodGet,
				"/auth/sessions/"+initResp.ShortID+"/poll?codeVerifier="+url.QueryEscape(verifier),
				nil,
				&pollResp,
				"",
			)
			if err != nil {
				if strings.Contains(err.Error(), "404") {
					return fmt.Errorf("login session expired or invalid")
				}
				continue
			}

			if pollResp.Pending {
				continue
			}

			// Extract email directly from the JWT claims (avoids storing it in DB).
			userEmail := ""
			if claims, err := m.validateSupabaseJWT(pollResp.AccessToken); err == nil {
				userEmail = claimString(claims, "email")
			}
			session := storedSession{
				AccessToken:  pollResp.AccessToken,
				RefreshToken: pollResp.RefreshToken,
				ExpiresAt:    pollResp.ExpiresAt,
				UserID:       pollResp.UserID,
				UserEmail:    userEmail,
			}

			if err := m.saveSession(session); err != nil {
				return err
			}
			if session.UserEmail != "" {
				fmt.Printf("Authentication complete. Logged in as: %s\n", maskEmail(session.UserEmail))
			} else {
				fmt.Println("Authentication complete.")
			}
			if err := m.registerManager(ctx); err != nil {
				if isTunnelRegistrationError(err) {
					m.logWarn("register_manager_after_login_warning err=%v", err)
					fmt.Printf("Warning: manager registration failed: %s\n", cleanCLIError(err))
					fmt.Println("Your login session was saved. Retry manager startup later with: passiveagents start")
					return nil
				}
				return err
			}
			fmt.Println("To start your passiveagents manager in the background, run: passiveagents start")
			return nil
		}
	}
}

func isTunnelRegistrationError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(cleanCLIError(err), "Unable to create Cloudflare tunnel")
}

func runManager(ctx context.Context, cfg config) error {
	previousManagerPID := 0
	if pid, err := readManagerPIDFromState(cfg.StateFile); err == nil {
		previousManagerPID = pid
		if pid != os.Getpid() && isProcessRunning(pid) {
			return fmt.Errorf("another manager instance is already running (PID %d)", pid)
		}
	}
	if err := writeManagerPIDToState(cfg.StateFile, os.Getpid()); err != nil {
		return err
	}
	defer func() {
		_ = clearManagerPIDInState(cfg.StateFile, os.Getpid())
	}()

	m, err := newManager(cfg)
	if err != nil {
		return err
	}
	m.previousManagerPID = previousManagerPID
	return m.startForeground(ctx)
}

func (m *manager) startForeground(ctx context.Context) error {
	if err := m.ensureRegistered(ctx); err != nil {
		return err
	}
	if err := m.refreshManagerUserIdentity(ctx); err != nil {
		m.logWarn(
			"manager_user_identity_refresh_startup_warning user_jwt_configured=%t err=%v",
			strings.TrimSpace(m.cfg.UserJWT) != "",
			err,
		)
	}
	if err := m.refreshAllowedFoldersFromBackend(ctx); err != nil {
		m.logError("allowed_folders_refresh_error err=%v", err)
	}
	m.reconcileLocalState()
	if err := m.recoverRestartableWorkers(ctx); err != nil {
		m.logError("restartable_worker_recovery_error err=%v", err)
	}
	if err := m.refreshResourceMetrics(); err != nil {
		m.logError("resource_monitor_error err=%v", err)
	}
	m.logInfo(
		"manager_start manager_id=%s api_url=%s web_url=%s max_concurrent=%d",
		strings.TrimSpace(m.state.ManagerID),
		strings.TrimSpace(m.cfg.APIBaseURL),
		strings.TrimSpace(m.cfg.WebBaseURL),
		m.cfg.MaxConcurrent,
	)
	if err := m.startStreamServer(ctx); err != nil {
		return err
	}
	m.setTunnelReady(false)
	if err := m.sendHeartbeat(ctx); err != nil {
		m.logError("initial_heartbeat_error err=%v", err)
	}
	if err := m.startCloudflareTunnel(ctx); err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		m.stopStreamServer()
		m.stopCloudflareTunnel()
	}()

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go m.resourceLoop(ctx)
	go m.heartbeatLoop(ctx)
	go m.instanceHeartbeatLoop(ctx)
	go m.pollLoop(ctx)
	go m.userSessionRefreshLoop(ctx)
	go m.lessonsSyncLoop(ctx)
	go m.lessonsPullLoop(ctx)
	go m.tunnelWatchLoop(ctx)
	go m.orphanRecoveryLoop(ctx)

	<-ctx.Done()
	m.shutdownWorkersForManagerStop()
	return nil
}

func (m *manager) startStreamServer(ctx context.Context) error {
	addr := fmt.Sprintf("127.0.0.1:%d", m.cfg.StreamPort)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", m.streamHealthHandler)
	mux.HandleFunc("/ws/manager", m.managerStreamHandler)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		m.logError("stream_listen_error err=%v", err)
		return err
	}
	if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok {
		m.cfg.StreamPort = tcpAddr.Port
	}
	m.streamServer = srv
	m.streamListener = ln

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			m.logError("stream_server_error err=%v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	m.logInfo("stream_server_started addr=%s", ln.Addr().String())
	return nil
}

func (m *manager) streamHealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":           true,
		"tunnel_ready": m.tunnelReady.Load(),
		"stream_port":  m.cfg.StreamPort,
	})
}

func (m *manager) managerStreamHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := streamUpgrader.Upgrade(w, r, nil)
	if err != nil {
		m.logError("manager_stream_upgrade_error err=%v", err)
		return
	}
	client := &browserClient{
		conn:               conn,
		subscribedInstance: map[string]struct{}{},
	}
	m.addBrowserClient(client)
	defer m.removeBrowserClient(client)

	_ = m.writeBrowserEvent(client, map[string]any{"type": "connection_ready"})
	m.handleManagerConnection(r.Context(), client)
}

var streamUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (m *manager) handleManagerConnection(ctx context.Context, client *browserClient) {
	client.conn.SetReadLimit(64 * 1024)
	_ = client.conn.SetReadDeadline(time.Now().Add(browserStreamPongWait))
	client.conn.SetPongHandler(func(string) error {
		return client.conn.SetReadDeadline(time.Now().Add(browserStreamPongWait))
	})
	stopHeartbeat := m.startBrowserStreamHeartbeat(client)
	defer stopHeartbeat()
	for {
		messageType, payload, err := client.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err,
				websocket.CloseNormalClosure,
				websocket.CloseNoStatusReceived,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
				m.logError("manager_stream_read_error err=%v", err)
			}
			return
		}
		_ = client.conn.SetReadDeadline(time.Now().Add(browserStreamPongWait))
		if messageType != websocket.TextMessage {
			continue
		}

		var envelope struct {
			Type            string `json:"type"`
			AccessToken     string `json:"access_token"`
			InstanceID      string `json:"instance_id"`
			TaskID          string `json:"task_id"`
			FolderID        string `json:"folder_id"`
			Path            string `json:"path"`
			Label           string `json:"label"`
			ReplyTo         string `json:"reply_to"`
			Data            string `json:"data"`
			MessageType     string `json:"message_type"`
			Text            string `json:"text"`
			ClientMessageID string `json:"client_message_id"`
			Cols            int    `json:"cols"`
			Rows            int    `json:"rows"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil {
			_ = m.writeBrowserEvent(client, map[string]any{
				"type":    "error",
				"message": "invalid websocket payload",
			})
			continue
		}

		switch envelope.Type {
		case "authenticate":
			if err := m.authenticateBrowserClient(ctx, client, strings.TrimSpace(envelope.AccessToken)); err != nil {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":    "error",
					"message": cleanCLIError(err),
				})
				return
			}
			_ = m.writeBrowserEvent(client, map[string]any{"type": "authenticated"})
		case "subscribe_instance":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":    "error",
					"message": "manager websocket is not authenticated",
				})
				return
			}
			instanceID := strings.TrimSpace(envelope.InstanceID)
			if instanceID == "" {
				continue
			}
			m.rememberBrowserPreferredSize(instanceID, envelope.Cols, envelope.Rows)
			client.subscriptionMu.Lock()
			client.subscribedInstance[instanceID] = struct{}{}
			client.subscriptionMu.Unlock()
			m.sendInitialStatesForInstance(client, instanceID)
			m.sendTerminalReplayForInstance(client, instanceID, envelope.Cols, envelope.Rows)
		case "unsubscribe_instance":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":    "error",
					"message": "manager websocket is not authenticated",
				})
				return
			}
			client.subscriptionMu.Lock()
			delete(client.subscribedInstance, strings.TrimSpace(envelope.InstanceID))
			client.subscriptionMu.Unlock()
		case "add_folder":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           "manager websocket is not authenticated",
				})
				return
			}
			if err := m.handleBrowserAddFolder(ctx, client, envelope.ClientMessageID, envelope.Path, envelope.Label); err != nil {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           cleanCLIError(err),
				})
			}
		case "remove_folder":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           "manager websocket is not authenticated",
				})
				return
			}
			if err := m.handleBrowserRemoveFolder(ctx, client, envelope.ClientMessageID, envelope.FolderID); err != nil {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           cleanCLIError(err),
				})
			}
		case "refresh_folders":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           "manager websocket is not authenticated",
				})
				return
			}
			if err := m.refreshAllowedFoldersFromBackend(ctx); err != nil {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           cleanCLIError(err),
				})
				return
			}
			_ = m.writeBrowserEvent(client, map[string]any{
				"type":              "folder_action_complete",
				"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
			})
		case "pickup_ready_tasks":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":    "error",
					"message": "manager websocket is not authenticated",
				})
				return
			}
			m.requestImmediatePoll()
		case "terminal_resize":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":    "error",
					"message": "manager websocket is not authenticated",
				})
				return
			}
			m.applyBrowserTerminalResize(strings.TrimSpace(envelope.InstanceID), envelope.Cols, envelope.Rows)
		case "terminal_input":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":    "error",
					"message": "manager websocket is not authenticated",
				})
				return
			}
			m.applyBrowserTerminalInput(strings.TrimSpace(envelope.InstanceID), envelope.Data)
		case "send_message":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           "manager websocket is not authenticated",
				})
				return
			}
			if err := m.handleBrowserSendMessage(ctx, client, envelope.ClientMessageID, strings.TrimSpace(envelope.InstanceID), strings.TrimSpace(envelope.TaskID), strings.TrimSpace(envelope.ReplyTo), strings.TrimSpace(envelope.MessageType), strings.TrimSpace(envelope.Text)); err != nil {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           cleanCLIError(err),
				})
			}
		case "wake_instance":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           "manager websocket is not authenticated",
				})
				return
			}
			clientMessageID := strings.TrimSpace(envelope.ClientMessageID)
			instanceID := strings.TrimSpace(envelope.InstanceID)
			taskID := strings.TrimSpace(envelope.TaskID)
			go func() {
				if err := m.handleBrowserWakeInstance(ctx, client, clientMessageID, instanceID, taskID); err != nil {
					_ = m.writeBrowserEvent(client, map[string]any{
						"type":              "error",
						"client_message_id": clientMessageID,
						"message":           cleanCLIError(err),
					})
				}
			}()
		case "shutdown_instance":
			if !browserClientAuthenticated(client) {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           "manager websocket is not authenticated",
				})
				return
			}
			if err := m.handleBrowserShutdownInstance(client, envelope.ClientMessageID, strings.TrimSpace(envelope.InstanceID)); err != nil {
				_ = m.writeBrowserEvent(client, map[string]any{
					"type":              "error",
					"client_message_id": strings.TrimSpace(envelope.ClientMessageID),
					"message":           cleanCLIError(err),
				})
			}
		default:
			_ = m.writeBrowserEvent(client, map[string]any{
				"type":    "error",
				"message": "unsupported websocket event",
			})
		}
	}
}

func (m *manager) startBrowserStreamHeartbeat(client *browserClient) func() {
	stop := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(browserStreamPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				client.writeMu.Lock()
				err := client.conn.WriteControl(
					websocket.PingMessage,
					nil,
					time.Now().Add(browserStreamWriteWait),
				)
				client.writeMu.Unlock()
				if err != nil {
					_ = client.conn.Close()
					return
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(stop)
			<-stopped
		})
	}
}

func browserClientAuthenticated(client *browserClient) bool {
	client.subscriptionMu.RLock()
	defer client.subscriptionMu.RUnlock()
	return client.authenticated
}

func (m *manager) authenticateBrowserClient(ctx context.Context, client *browserClient, accessToken string) error {
	if accessToken == "" {
		return fmt.Errorf("missing browser access token")
	}
	claims, err := m.validateSupabaseJWT(accessToken)
	if err != nil {
		return fmt.Errorf("invalid browser access token")
	}
	browserUserID := strings.TrimSpace(claimString(claims, "sub"))
	if browserUserID == "" {
		return fmt.Errorf("browser session missing user id")
	}
	managerUserID, err := m.currentManagerUserID(ctx)
	if err != nil {
		return err
	}
	if browserUserID != managerUserID {
		return fmt.Errorf("browser session does not match the manager account")
	}
	client.subscriptionMu.Lock()
	client.authenticated = true
	client.authenticatedUser = browserUserID
	client.subscriptionMu.Unlock()
	return nil
}

func (m *manager) currentManagerUserID(ctx context.Context) (string, error) {
	m.mu.Lock()
	userID := strings.TrimSpace(m.userID)
	m.mu.Unlock()
	if userID != "" {
		return userID, nil
	}
	if err := m.refreshManagerUserIdentity(ctx); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	userID = strings.TrimSpace(m.userID)
	if userID == "" {
		return "", fmt.Errorf("manager session missing user id")
	}
	return userID, nil
}

func (m *manager) addBrowserClient(client *browserClient) {
	client.lifecycleMu.Lock()
	if client.outbound == nil {
		client.outbound = make(chan map[string]any, 256)
	}
	client.closed = false
	client.lifecycleMu.Unlock()
	m.browserMu.Lock()
	m.browserClients[client] = struct{}{}
	m.browserMu.Unlock()
	go m.runBrowserClientWriter(client)
}

func (m *manager) removeBrowserClient(client *browserClient) {
	if client == nil {
		return
	}
	client.closeOnce.Do(func() {
		client.lifecycleMu.Lock()
		client.closed = true
		outbound := client.outbound
		client.outbound = nil
		client.lifecycleMu.Unlock()
		if outbound != nil {
			close(outbound)
		}
		_ = client.conn.Close()
		m.browserMu.Lock()
		delete(m.browserClients, client)
		m.browserMu.Unlock()
	})
}

func (m *manager) runBrowserClientWriter(client *browserClient) {
	client.lifecycleMu.RLock()
	outbound := client.outbound
	client.lifecycleMu.RUnlock()
	if outbound == nil {
		return
	}
	for payload := range outbound {
		if err := m.writeBrowserEventDirect(client, payload); err != nil {
			m.removeBrowserClient(client)
			return
		}
	}
}

func (m *manager) writeBrowserEvent(client *browserClient, payload any) error {
	return m.writeBrowserEventDirect(client, payload)
}

func (m *manager) writeBrowserEventDirect(client *browserClient, payload any) error {
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	client.conn.SetWriteDeadline(time.Now().Add(browserStreamWriteWait))
	defer client.conn.SetWriteDeadline(time.Time{})
	return client.conn.WriteJSON(payload)
}

func (m *manager) writeBrowserEvents(client *browserClient, payloads ...map[string]any) error {
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	client.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	defer client.conn.SetWriteDeadline(time.Time{})
	for _, payload := range payloads {
		if err := client.conn.WriteJSON(payload); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) enqueueBrowserEvent(client *browserClient, payload map[string]any) bool {
	if client == nil || payload == nil {
		return false
	}
	client.lifecycleMu.RLock()
	if client.closed || client.outbound == nil {
		client.lifecycleMu.RUnlock()
		return false
	}
	select {
	case client.outbound <- payload:
		client.lifecycleMu.RUnlock()
		return true
	default:
		client.lifecycleMu.RUnlock()
		m.removeBrowserClient(client)
		return false
	}
}

func (m *manager) sendInitialStatesForInstance(client *browserClient, instanceID string) {
	instanceID = strings.TrimSpace(instanceID)
	livestreamState := m.computeLivestreamState(instanceID)
	agentState, agentMessage := m.computeAgentState(instanceID)
	workingDir := m.workerWorkingDir(instanceID)

	livestreamPayload := map[string]any{
		"type":        "livestream_state",
		"instance_id": instanceID,
		"state":       livestreamState,
	}
	agentPayload := map[string]any{
		"type":        "agent_state",
		"instance_id": instanceID,
		"state":       agentState,
	}
	if agentMessage != "" {
		agentPayload["message"] = agentMessage
	}
	if workingDir != "" {
		agentPayload["working_dir"] = workingDir
	}
	_ = m.writeBrowserEvent(client, livestreamPayload)
	_ = m.writeBrowserEvent(client, agentPayload)
}

func (m *manager) persistedWorkerForInstance(instanceID string) (persistedWorker, bool) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return persistedWorker{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	persisted, ok := m.state.Instances[instanceID]
	return persisted, ok
}

func (m *manager) applyBrowserTerminalResize(instanceID string, cols, rows int) {
	m.rememberBrowserPreferredSize(instanceID, cols, rows)
	m.resizeWorkerTerminal(instanceID, cols, rows)
}

func (m *manager) resizeWorkerTerminal(instanceID string, cols, rows int) bool {
	if instanceID == "" || cols <= 0 || rows <= 0 {
		return false
	}
	worker := m.workerForInstanceID(instanceID)
	if worker == nil {
		return false
	}
	return m.setWorkerTerminalSize(worker, cols, rows, true, true)
}

func (m *manager) rememberBrowserPreferredSize(instanceID string, cols, rows int) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || cols <= 0 || rows <= 0 {
		return
	}
	cols, rows, _ = clampPTYDimensions(cols, rows)
	m.mu.Lock()
	m.browserPreferredSizes[instanceID] = terminalSize{cols: cols, rows: rows}
	m.mu.Unlock()
}

func (m *manager) preferredBrowserSize(instanceID string) (terminalSize, bool) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return terminalSize{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	size, ok := m.browserPreferredSizes[instanceID]
	return size, ok
}

func (m *manager) subscribedBrowserClientsForInstance(instanceID string) []*browserClient {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil
	}
	m.browserMu.RLock()
	clients := make([]*browserClient, 0, len(m.browserClients))
	for client := range m.browserClients {
		client.subscriptionMu.RLock()
		_, ok := client.subscribedInstance[instanceID]
		client.subscriptionMu.RUnlock()
		if ok {
			clients = append(clients, client)
		}
	}
	m.browserMu.RUnlock()
	return clients
}

func (m *manager) broadcastTerminalReplayToSubscribedBrowsers(instanceID string) {
	for _, client := range m.subscribedBrowserClientsForInstance(instanceID) {
		m.sendTerminalReplayForInstance(client, instanceID, 0, 0)
	}
}

func (m *manager) setWorkerTerminalSize(worker *worker, cols, rows int, applyPTY, recordReplay bool) bool {
	if worker == nil || cols <= 0 || rows <= 0 {
		return false
	}
	clampedCols, clampedRows, clipped := clampPTYDimensions(cols, rows)
	if clipped {
		m.logWarn(
			"terminal_resize_clamped instance_id=%s cols=%d rows=%d clamped_cols=%d clamped_rows=%d",
			worker.instanceID,
			cols,
			rows,
			clampedCols,
			clampedRows,
		)
	}
	cols = clampedCols
	rows = clampedRows
	worker.terminalStateMu.Lock()
	if worker.terminalCols == cols && worker.terminalRows == rows {
		worker.terminalStateMu.Unlock()
		return false
	}
	if worker.terminalState != nil {
		worker.terminalState.Resize(cols, rows)
	}
	worker.terminalCols = cols
	worker.terminalRows = rows
	if recordReplay && worker.terminalReplayBuffer != nil {
		worker.terminalReplayEventSeq++
		worker.terminalReplayBuffer.AppendResize(worker.terminalReplayEventSeq, cols, rows)
	}
	worker.terminalStateMu.Unlock()
	if applyPTY {
		if ptmx, ok := worker.input.(*os.File); ok {
			_ = pty.Setsize(ptmx, &pty.Winsize{
				Cols: uint16(cols),
				Rows: uint16(rows),
			})
		}
	}
	return true
}

func clampPTYDimensions(cols, rows int) (int, int, bool) {
	clipped := false
	if cols > maxPTYDimension {
		cols = maxPTYDimension
		clipped = true
	}
	if rows > maxPTYDimension {
		rows = maxPTYDimension
		clipped = true
	}
	return cols, rows, clipped
}

func (m *manager) sendTerminalReplayForInstance(client *browserClient, instanceID string, requestedCols, requestedRows int) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return
	}
	worker := m.workerForInstanceID(instanceID)
	if worker == nil || !worker.usesPTY || !workerTerminalVisibleToBrowser(worker) {
		return
	}

	shouldResize := false
	worker.terminalStateMu.Lock()
	if worker.terminalCols <= 0 {
		worker.terminalCols = 80
	}
	if worker.terminalRows <= 0 {
		worker.terminalRows = 24
	}
	if !worker.terminalSizeLocked {
		if requestedCols > 0 && requestedRows > 0 && worker.terminalOutputSeq == 0 {
			shouldResize = worker.terminalCols != requestedCols || worker.terminalRows != requestedRows
		}
		worker.terminalSizeLocked = true
	}
	replayID := uuid.NewString()
	endSequence := worker.terminalOutputSeq
	endEventID := worker.terminalReplayEventSeq
	cols := worker.terminalCols
	rows := worker.terminalRows
	chunks := worker.terminalReplayBuffer.Snapshot(endEventID)
	worker.terminalStateMu.Unlock()

	if shouldResize {
		m.setWorkerTerminalSize(worker, requestedCols, requestedRows, true, true)
		worker.terminalStateMu.Lock()
		cols = worker.terminalCols
		rows = worker.terminalRows
		endEventID = worker.terminalReplayEventSeq
		endSequence = worker.terminalOutputSeq
		chunks = worker.terminalReplayBuffer.Snapshot(endEventID)
		worker.terminalStateMu.Unlock()
	}

	events := make([]map[string]any, 0, len(chunks)+2)
	events = append(events, map[string]any{
		"type":         "terminal_replay_start",
		"instance_id":  instanceID,
		"replay_id":    replayID,
		"end_sequence": endSequence,
		"cols":         cols,
		"rows":         rows,
	})
	for _, chunk := range chunks {
		switch chunk.kind {
		case "resize":
			events = append(events, map[string]any{
				"type":        "terminal_replay_resize",
				"instance_id": instanceID,
				"replay_id":   replayID,
				"cols":        chunk.cols,
				"rows":        chunk.rows,
			})
		default:
			events = append(events, map[string]any{
				"type":        "terminal_replay_chunk",
				"instance_id": instanceID,
				"replay_id":   replayID,
				"sequence":    chunk.sequence,
				"data_base64": base64.StdEncoding.EncodeToString(chunk.data),
			})
		}
	}
	events = append(events, map[string]any{
		"type":         "terminal_replay_end",
		"instance_id":  instanceID,
		"replay_id":    replayID,
		"end_sequence": endSequence,
		"cols":         cols,
		"rows":         rows,
	})
	if err := m.writeBrowserEvents(client, events...); err != nil {
		m.removeBrowserClient(client)
	}
}

func (m *manager) applyBrowserTerminalInput(instanceID, data string) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || data == "" {
		return
	}
	worker := m.workerForInstanceID(instanceID)
	if worker == nil {
		return
	}
	if _, err := io.WriteString(worker.input, data); err != nil {
		m.logError("browser_terminal_input_error instance_id=%s err=%v", instanceID, err)
	}
}

func workerTerminalVisibleToBrowser(w *worker) bool {
	if w == nil {
		return false
	}
	if !w.usesPTY {
		return true
	}
	return w.livestreamReady.Load()
}

func workerLivestreamStateForWorker(w *worker) string {
	if w == nil || !w.livestreamReady.Load() {
		return "offline"
	}
	return "online"
}

func (m *manager) logWorkerStateMilestone(w *worker, milestone string) {
	if w == nil {
		return
	}
	agentState := workerAgentStatusForWorker(w)
	if agentState == "" {
		agentState = "offline"
	}
	m.logInfo(
		"worker_state_milestone instance_id=%s milestone=%s livestream_state=%s agent_state=%s runtime=%s",
		w.instanceID,
		strings.TrimSpace(milestone),
		workerLivestreamStateForWorker(w),
		agentState,
		strings.TrimSpace(w.runtimeCommand),
	)
}

func (m *manager) logPTYStartFailure(instanceID, command, workingDir string, attempt int, err error) {
	if err == nil {
		return
	}
	m.logWarn(
		"pty_start_failed instance_id=%s runtime=%s working_dir=%s attempt=%d err=%v",
		strings.TrimSpace(instanceID),
		strings.TrimSpace(command),
		strings.TrimSpace(workingDir),
		attempt,
		err,
	)
}

func (m *manager) logPTYRetryWithoutSetctty(instanceID, command, workingDir string, err error) {
	if err == nil {
		return
	}
	m.logWarn(
		"pty_setctty_blocked_retrying instance_id=%s runtime=%s working_dir=%s err=%v",
		strings.TrimSpace(instanceID),
		strings.TrimSpace(command),
		strings.TrimSpace(workingDir),
		err,
	)
}

func (m *manager) logPTYStartSuccess(instanceID, command, workingDir string, attempt int) {
	event := "pty_started_with_setctty"
	if attempt == 2 {
		event = "pty_started_without_setctty"
	}
	m.logInfo(
		"%s instance_id=%s runtime=%s working_dir=%s attempt=%d",
		event,
		strings.TrimSpace(instanceID),
		strings.TrimSpace(command),
		strings.TrimSpace(workingDir),
		attempt,
	)
}

func (m *manager) logInstanceLifecycleEvent(event, instanceID, taskID, status, detail string) {
	m.logInfo(
		"instance_lifecycle_event event=%s instance_id=%s task_id=%s status=%s detail=%q",
		strings.TrimSpace(event),
		strings.TrimSpace(instanceID),
		strings.TrimSpace(taskID),
		strings.TrimSpace(status),
		strings.TrimSpace(detail),
	)
}

func (m *manager) markWorkerLivestreamReady(w *worker, message string) {
	if w == nil {
		return
	}
	alreadyReady := w.livestreamReady.Swap(true)
	if alreadyReady {
		return
	}
	m.logWorkerStateMilestone(w, "pty_live")
	m.broadcastLivestreamState(w.instanceID, "online", message)
	agentStatus := workerAgentStatusForWorker(w)
	if agentStatus == "" {
		agentStatus = "waking_up"
	}
	m.broadcastAgentState(w.instanceID, agentStatus, "")
	m.broadcastTerminalReplayToSubscribedBrowsers(w.instanceID)
}

func (m *manager) markWorkerBootstrapObserved(w *worker) {
	if w == nil {
		return
	}
	if w.bootstrapObserved.Swap(true) {
		return
	}
	w.bootstrapObservedAt.Store(time.Now().UnixNano())
	m.logWorkerStateMilestone(w, "bootstrap_observed")
	m.persistWorkerActivityStatus(w)
}

func (m *manager) maybeForceBootstrapReadyAfterObservationTimeout(w *worker, err error) bool {
	if w == nil || err == nil {
		return false
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if !w.usesPTY || !supportsBootstrapSubmissionObservation(w.runtimeCommand) {
		return false
	}
	m.unblockWorkerAfterBootstrapTimeout(w)
	return true
}

func (m *manager) unblockWorkerAfterBootstrapTimeout(w *worker) {
	if w == nil {
		return
	}
	w.clearBootstrapPromptPending()
	w.bootstrapReadyEmptySeen.Store(true)
	m.markWorkerBootstrapObserved(w)
	livestreamState := m.computeLivestreamState(w.instanceID)
	agentState, message := m.computeAgentState(w.instanceID)
	m.broadcastBothStates(w.instanceID, livestreamState, agentState, message)
}

func (m *manager) handleBrowserSendMessage(ctx context.Context, client *browserClient, clientMessageID, instanceID, taskID, replyTo, messageType, text string) error {
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("message text is required")
	}
	switch messageType {
	case "command":
		return m.handleBrowserCommand(ctx, client, clientMessageID, instanceID, taskID, text)
	default:
		return fmt.Errorf("unsupported message type")
	}
}

func (m *manager) handleBrowserCommand(ctx context.Context, client *browserClient, clientMessageID, instanceID, taskID, text string) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return fmt.Errorf("instance id is required")
	}

	command, err := m.persistCommand(ctx, instanceID, text)
	if err != nil {
		return err
	}

	unlockSpawn := m.lockInstanceSpawn(instanceID)
	defer unlockSpawn()

	if m.workerForInstanceID(instanceID) == nil {
		m.broadcastAgentState(instanceID, "waking_up", "Recreating agent session for your command.")
		prepared, err := m.prepareCommandRoute(ctx, instanceID, taskID)
		if err != nil {
			m.broadcastBothStates(instanceID, "offline", "offline", "Unable to resume this agent right now.")
			commandID, _ := command["id"].(string)
			_ = m.markCommandFailed(ctx, instanceID, commandID, err.Error(), true)
			return err
		}
		persona, err := m.fetchPersonaByID(ctx, prepared.AgentID)
		if err != nil {
			m.broadcastBothStates(instanceID, "offline", "offline", "Unable to load agent persona.")
			commandID, _ := command["id"].(string)
			_ = m.markCommandFailed(ctx, instanceID, commandID, err.Error(), true)
			return err
		}
		if err := m.spawnWorker(ctx, persona, prepared.Task, 1, instanceID); err != nil {
			m.broadcastBothStates(instanceID, "offline", "offline", "Unable to restart this agent.")
			commandID, _ := command["id"].(string)
			_ = m.markCommandFailed(ctx, instanceID, commandID, err.Error(), true)
			return err
		}
	}

	worker := m.workerForInstanceID(instanceID)
	if worker == nil {
		m.broadcastBothStates(instanceID, "offline", "offline", "Agent session is not available.")
		commandID, _ := command["id"].(string)
		_ = m.markCommandFailed(ctx, instanceID, commandID, "agent session is not available", true)
		return fmt.Errorf("agent session is not available")
	}
	commandID, _ := command["id"].(string)
	if err := m.deliverBrowserCommand(ctx, worker, commandID, text); err != nil {
		_ = m.markCommandFailed(ctx, instanceID, commandID, err.Error(), false)
		return err
	}
	return m.writeBrowserEvent(client, map[string]any{
		"type":              "message_accepted",
		"client_message_id": strings.TrimSpace(clientMessageID),
		"command":           command,
	})
}

func (m *manager) markCommandFailed(ctx context.Context, instanceID, commandID, message string, retryable bool) error {
	if commandID == "" {
		return nil
	}
	var out map[string]any
	apiErr := m.userRequestJSON(ctx, http.MethodPost, "/api/agents/"+instanceID+"/commands/"+commandID+"/failed", map[string]any{
		"message":   message,
		"retryable": retryable,
	}, &out)
	if apiErr == nil {
		return nil
	}
	if strings.TrimSpace(m.cfg.DatabaseURL) == "" {
		return apiErr
	}
	db, err := sql.Open("postgres", m.cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	status := "FAILED"
	if retryable {
		status = "RETRYABLE"
	}
	_, err = db.Exec(`update agent_commands set status = $1, error_message = $2, updated_at = now() where id = $3`, status, message, commandID)
	return err
}

func (m *manager) currentWakeCapacity() (runningCount, maxConcurrency int) {
	snapshot := m.currentCapacitySnapshot()
	return snapshot.CurrentRunningAgents, snapshot.MaxParallelAgents
}

type workerCapacityCost struct {
	CPUCores float64
	RAMMB    float64
}

type capacityInputs struct {
	Cores              int
	CPUPercent         float64
	CPUThresholdPct    float64
	TotalMB            float64
	FreeMB             float64
	SystemReserveMB    float64
	FallbackAgentRAMMB float64
	HardMaxConcurrent  int
	WorkerCosts        []workerCapacityCost
}

type capacitySnapshot struct {
	CurrentRunningAgents int
	MaxParallelAgents    int
}

func (w *worker) effectiveCapacityCost(now time.Time, fallbackRAMMB float64) workerCapacityCost {
	w.resourceMu.Lock()
	startedAt := w.startedAt
	observedRSSMB := w.observedRSSMB
	observedCPUCores := w.observedCPUCores
	lastSampleAt := w.lastResourceSampleAt
	w.resourceMu.Unlock()

	if fallbackRAMMB <= 0 {
		fallbackRAMMB = 1024
	}

	ramMB := observedRSSMB
	if ramMB <= 0 {
		ramMB = fallbackRAMMB
	}
	cpuCores := observedCPUCores
	if cpuCores < 0 {
		cpuCores = 0
	}

	hasRecentSample := !lastSampleAt.IsZero()
	if !startedAt.IsZero() && now.Sub(startedAt) < workerStartupObservationWindow {
		if ramMB < fallbackRAMMB {
			ramMB = fallbackRAMMB
		}
		ramMB *= startupResourceCostMultiplier
		if !hasRecentSample || cpuCores < defaultStartupAgentCPUCores {
			cpuCores = defaultStartupAgentCPUCores
		}
	} else if !hasRecentSample && cpuCores <= 0 {
		cpuCores = defaultSteadyAgentCPUCores
	}

	return workerCapacityCost{
		CPUCores: maxFloat64(cpuCores, 0),
		RAMMB:    maxFloat64(ramMB, fallbackRAMMB),
	}
}

func calculateCapacitySnapshot(inputs capacityInputs) capacitySnapshot {
	cores := inputs.Cores
	if cores <= 0 {
		cores = 1
	}

	cpuThresholdPct := inputs.CPUThresholdPct
	if cpuThresholdPct <= 0 {
		cpuThresholdPct = 80
	}

	fallbackAgentRAMMB := inputs.FallbackAgentRAMMB
	if fallbackAgentRAMMB <= 0 {
		fallbackAgentRAMMB = 1024
	}

	cpuHardCap := int(math.Floor(float64(cores) * 0.5))
	if cpuHardCap < 1 {
		cpuHardCap = 1
	}

	runningAgents := len(inputs.WorkerCosts)
	totalWorkerCPUCores := 0.0
	totalWorkerRAMMB := 0.0
	for _, cost := range inputs.WorkerCosts {
		if cost.CPUCores > 0 {
			totalWorkerCPUCores += cost.CPUCores
		}
		if cost.RAMMB > 0 {
			totalWorkerRAMMB += cost.RAMMB
		}
	}

	predictedAgentCPUCores := defaultSteadyAgentCPUCores
	if runningAgents > 0 && totalWorkerCPUCores > 0 {
		predictedAgentCPUCores = totalWorkerCPUCores / float64(runningAgents)
	}
	if predictedAgentCPUCores <= 0 {
		predictedAgentCPUCores = defaultSteadyAgentCPUCores
	}

	predictedAgentRAMMB := fallbackAgentRAMMB
	if runningAgents > 0 && totalWorkerRAMMB > 0 {
		predictedAgentRAMMB = totalWorkerRAMMB / float64(runningAgents)
	}
	if predictedAgentRAMMB < fallbackAgentRAMMB {
		predictedAgentRAMMB = fallbackAgentRAMMB
	}

	systemUsedCPUCores := (maxFloat64(inputs.CPUPercent, 0) / 100.0) * float64(cores)
	nonAgentCPUCores := systemUsedCPUCores - totalWorkerCPUCores
	if nonAgentCPUCores < 0 {
		nonAgentCPUCores = 0
	}
	safeAgentCPUCores := (cpuThresholdPct / 100.0 * float64(cores)) - nonAgentCPUCores
	if safeAgentCPUCores < 0 {
		safeAgentCPUCores = 0
	}
	if safeAgentCPUCores > float64(cpuHardCap) {
		safeAgentCPUCores = float64(cpuHardCap)
	}
	cpuCapacity := int(math.Floor(safeAgentCPUCores / predictedAgentCPUCores))

	totalMB := maxFloat64(inputs.TotalMB, 0)
	freeMB := maxFloat64(inputs.FreeMB, 0)
	if totalMB > 0 && freeMB > totalMB {
		freeMB = totalMB
	}
	systemUsedMB := totalMB - freeMB
	nonAgentUsedMB := systemUsedMB - totalWorkerRAMMB
	if nonAgentUsedMB < 0 {
		nonAgentUsedMB = 0
	}
	ramBudgetByUtilization := (float64(ramUtilizationCapPct) / 100.0 * totalMB) - nonAgentUsedMB
	ramBudgetByReserve := totalMB - maxFloat64(inputs.SystemReserveMB, 0) - nonAgentUsedMB
	safeAgentRAMMB := minFloat64(ramBudgetByUtilization, ramBudgetByReserve)
	if safeAgentRAMMB < 0 {
		safeAgentRAMMB = 0
	}
	ramCapacity := int(math.Floor(safeAgentRAMMB / predictedAgentRAMMB))

	parallelAgents := minInt(cpuHardCap, minInt(cpuCapacity, ramCapacity))
	if inputs.HardMaxConcurrent > 0 {
		parallelAgents = minInt(parallelAgents, inputs.HardMaxConcurrent)
	}
	parallelAgents = maxInt(1, parallelAgents)
	if runningAgents > parallelAgents {
		parallelAgents = runningAgents
	}

	return capacitySnapshot{
		CurrentRunningAgents: runningAgents,
		MaxParallelAgents:    parallelAgents,
	}
}

func (m *manager) currentCapacitySnapshot() capacitySnapshot {
	m.mu.Lock()
	cpuVal := m.cpu
	freeMB := m.freeMB
	totalMB := m.totalMB
	workers := make([]*worker, 0, len(m.workers))
	for _, w := range m.workers {
		if w != nil {
			workers = append(workers, w)
		}
	}
	m.mu.Unlock()

	now := time.Now()
	costs := make([]workerCapacityCost, 0, len(workers))
	for _, w := range workers {
		costs = append(costs, w.effectiveCapacityCost(now, m.cfg.MBPerAgent))
	}

	return calculateCapacitySnapshot(capacityInputs{
		Cores:              runtime.NumCPU(),
		CPUPercent:         cpuVal,
		CPUThresholdPct:    m.cfg.CPUThreshold,
		TotalMB:            totalMB,
		FreeMB:             freeMB,
		SystemReserveMB:    m.cfg.SystemReserveMB,
		FallbackAgentRAMMB: m.cfg.MBPerAgent,
		HardMaxConcurrent:  m.cfg.MaxConcurrent,
		WorkerCosts:        costs,
	})
}

func buildGracefulShutdownPrompt() string {
	return "Before you shut down, read TASK_CONTEXT.md to understand previous checkpoints, write any new progress to TASK_CONTEXT.md and append a new lesson learned to lessons.md, then exit. If you didn't do much, don't append a new lesson."
}

func workerShutdownStatusMessage(mode string) string {
	switch strings.TrimSpace(mode) {
	case shutdownModeManagerRestart:
		return "Manager is stopping. Waiting for the agent to persist progress."
	case shutdownModeIdleTimeout:
		return "Agent has been idle with no activity for 10 minutes. Asking it to persist and shut down."
	default:
		return "Shutdown requested. Waiting for the agent to persist progress."
	}
}

func (m *manager) saveOpenCodeResumeCommandIfPresent(w *worker) {
	if w == nil || !isOpenCodeRuntimeCommand(w.runtimeCommand) {
		return
	}
	if resumeCmd, ok := extractOpenCodeResumeCommand(workerReadySnapshot(w)); ok {
		m.mu.Lock()
		if pw, exists := m.state.Instances[w.instanceID]; exists {
			pw.ResumeCommand = resumeCmd
			m.state.Instances[w.instanceID] = pw
			_ = writeState(m.cfg.StateFile, m.state)
		}
		m.mu.Unlock()
		m.logInfo("opencode_resume_command_saved instance_id=%s", w.instanceID)
	}
}

func (m *manager) requestGracefulWorkerShutdown(w *worker) error {
	return m.requestWorkerShutdown(w, shutdownModeUser, "Shutdown requested. Waiting for the agent to persist progress.", gracefulShutdownWait)
}

func (m *manager) requestManagerRestartWorkerShutdown(w *worker) error {
	return m.requestWorkerShutdown(w, shutdownModeManagerRestart, "Manager is stopping. Waiting for the agent to persist progress.", gracefulShutdownWait)
}

func (m *manager) requestIdleTimeoutShutdown(w *worker) error {
	return m.requestWorkerShutdown(w, shutdownModeIdleTimeout, "Agent has been idle with no activity for 10 minutes. Asking it to persist and shut down.", 5*time.Minute)
}

func (m *manager) requestWorkerShutdown(w *worker, mode, routedMessage string, gracePeriod time.Duration) error {
	if w == nil {
		return fmt.Errorf("agent session is not running")
	}
	prompt := buildGracefulShutdownPrompt()
	if !w.markShutdownRequested(prompt, mode) {
		return nil
	}
	m.logInfo("graceful_shutdown_requested instance_id=%s mode=%s grace_period=%s", w.instanceID, strings.TrimSpace(mode), gracePeriod)
	if err := m.writeAutomatedPrompt(w, prompt); err != nil {
		m.logWarn("graceful_shutdown_prompt_error instance_id=%s err=%v", w.instanceID, err)
	} else {
		m.logInfo("graceful_shutdown_prompt_written instance_id=%s mode=%s prompt=%q", w.instanceID, strings.TrimSpace(mode), prompt)
		submitCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		if awaitWorkerPromptSubmitted(submitCtx, w) {
			m.logInfo("graceful_shutdown_prompt_submitted instance_id=%s", w.instanceID)
		} else {
			m.logWarn("graceful_shutdown_prompt_submission_unconfirmed instance_id=%s err=%v", w.instanceID, submitCtx.Err())
		}
		cancel()
	}
	m.logWorkerStateMilestone(w, "shutdown_requested")
	m.broadcastAgentState(w.instanceID, "shutting_down", workerShutdownStatusMessage(mode))
	go func() {
		waitTimer := time.NewTimer(gracePeriod)
		defer waitTimer.Stop()
		select {
		case <-w.done:
			m.saveOpenCodeResumeCommandIfPresent(w)
			m.logInfo("graceful_shutdown_agent_finished instance_id=%s", w.instanceID)
			return
		case <-waitTimer.C:
		}
		m.logWarn("graceful_shutdown_agent_finish_timeout instance_id=%s", w.instanceID)
		if w.cmd == nil || w.cmd.Process == nil {
			return
		}
		if err := m.killWorkerWithRetry(w); err != nil {
			w.clearShutdownRequested()
			livestreamState := m.computeLivestreamState(w.instanceID)
			agentState, message := m.computeAgentState(w.instanceID)
			m.broadcastBothStates(w.instanceID, livestreamState, agentState, message)
			m.logWarn("graceful_shutdown_kill_failed instance_id=%s err=%v", w.instanceID, err)
			return
		}
		m.logWarn("graceful_shutdown_kill_sent instance_id=%s", w.instanceID)
	}()
	return nil
}

func (m *manager) handleBrowserWakeInstance(ctx context.Context, client *browserClient, clientMessageID, instanceID, taskID string) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return fmt.Errorf("instance id is required")
	}
	m.logInstanceLifecycleEvent("wake_instance", instanceID, taskID, "requested", "")
	wakeCtx := context.Background()

	unlockSpawn := m.lockInstanceSpawn(instanceID)
	defer unlockSpawn()

	if m.workerForInstanceID(instanceID) == nil {
		runningCount, maxConcurrency := m.currentWakeCapacity()
		if runningCount >= maxConcurrency {
			m.logInstanceLifecycleEvent("wake_instance", instanceID, taskID, "capacity_blocked", fmt.Sprintf("running_count=%d max_concurrency=%d", runningCount, maxConcurrency))
			return m.writeBrowserEvent(client, map[string]any{
				"type":              "wake_accepted",
				"client_message_id": strings.TrimSpace(clientMessageID),
				"status":            "capacity_blocked",
				"code":              "MAX_CONCURRENCY_REACHED",
				"running_count":     runningCount,
				"max_concurrency":   maxConcurrency,
			})
		}
		m.broadcastAgentState(instanceID, "waking_up", "Resuming the agent session.")
		prepared, err := m.prepareCommandRoute(wakeCtx, instanceID, taskID)
		if err != nil {
			m.logInstanceLifecycleEvent("wake_instance", instanceID, taskID, "failed", err.Error())
			m.broadcastBothStates(instanceID, "offline", "offline", "Unable to resume this agent right now.")
			return err
		}
		persona, err := m.fetchPersonaByID(wakeCtx, prepared.AgentID)
		if err != nil {
			m.logInstanceLifecycleEvent("wake_instance", instanceID, taskID, "failed", err.Error())
			m.broadcastBothStates(instanceID, "offline", "offline", "Unable to load agent persona.")
			return err
		}
		if err := m.spawnWorker(wakeCtx, persona, prepared.Task, 1, instanceID); err != nil {
			m.logInstanceLifecycleEvent("wake_instance", instanceID, taskID, "failed", err.Error())
			m.broadcastBothStates(instanceID, "offline", "offline", "Unable to restart this agent.")
			return err
		}
	}

	m.logInstanceLifecycleEvent("wake_instance", instanceID, taskID, "accepted", "")
	m.sendInitialStatesForInstance(client, instanceID)
	return m.writeBrowserEvent(client, map[string]any{
		"type":              "wake_accepted",
		"client_message_id": strings.TrimSpace(clientMessageID),
		"status":            "accepted",
	})
}

func (m *manager) handleBrowserShutdownInstance(client *browserClient, clientMessageID, instanceID string) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return fmt.Errorf("instance id is required")
	}
	m.logInstanceLifecycleEvent("shutdown_instance", instanceID, "", "requested", "")
	worker := m.workerForInstanceID(instanceID)
	if worker == nil {
		m.logInstanceLifecycleEvent("shutdown_instance", instanceID, "", "accepted", "no_live_worker")
		return m.writeBrowserEvent(client, map[string]any{
			"type":              "shutdown_accepted",
			"client_message_id": strings.TrimSpace(clientMessageID),
		})
	}
	if err := m.requestGracefulWorkerShutdown(worker); err != nil {
		m.logInstanceLifecycleEvent("shutdown_instance", instanceID, "", "failed", err.Error())
		return err
	}
	m.logInstanceLifecycleEvent("shutdown_instance", instanceID, "", "accepted", "")
	return m.writeBrowserEvent(client, map[string]any{
		"type":              "shutdown_accepted",
		"client_message_id": strings.TrimSpace(clientMessageID),
	})
}

func (m *manager) persistCommand(ctx context.Context, instanceID, text string) (map[string]any, error) {
	var out map[string]any
	err := m.userRequestJSON(ctx, http.MethodPost, "/api/agents/"+instanceID+"/commands", map[string]any{
		"command": text,
	}, &out)
	return out, err
}

func buildTaskFolderErrorComment(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if !strings.HasPrefix(message, "selected folder ") {
		return ""
	}
	return "Task could not start because the selected folder is unavailable on this manager: " + message
}

func (m *manager) reportTaskFolderError(ctx context.Context, task apiTask, err error) {
	commentText := buildTaskFolderErrorComment(err)
	if commentText == "" || strings.TrimSpace(task.ID) == "" {
		return
	}
	m.logError("task_folder_error task_id=%s err=%v", task.ID, err)
}

func (m *manager) deliverBrowserCommand(ctx context.Context, w *worker, commandID, text string) error {
	if w == nil {
		return fmt.Errorf("worker is not available")
	}
	if err := m.writeAgentInputLine(w, text); err != nil {
		return err
	}
	w.markCommandDelivered(commandID)
	if err := m.markCommandExecuted(ctx, w.instanceID, commandID); err != nil {
		m.logError("command_execute_mark_failed instance_id=%s command_id=%s err=%v", w.instanceID, strings.TrimSpace(commandID), err)
	}
	return nil
}

func (m *manager) markCommandExecuted(ctx context.Context, instanceID, commandID string) error {
	instanceID = strings.TrimSpace(instanceID)
	commandID = strings.TrimSpace(commandID)
	if instanceID == "" || commandID == "" {
		return nil
	}
	var out map[string]any
	apiErr := m.userRequestJSON(ctx, http.MethodPost, "/api/agents/"+instanceID+"/commands/"+commandID+"/executed", nil, &out)
	if apiErr == nil {
		return nil
	}
	if strings.TrimSpace(m.cfg.DatabaseURL) == "" {
		return apiErr
	}
	db, err := sql.Open("postgres", m.cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`update agent_commands set executed = true, status = 'EXECUTED', executed_at = now() where id = $1`, commandID)
	return err
}

func (m *manager) fetchPersonaByID(ctx context.Context, agentID string) (apiAgentPersona, error) {
	var personas []apiAgentPersona
	if err := m.managerRequestJSON(ctx, http.MethodGet, "/managers/tasks/agent-personas", nil, &personas); err != nil {
		return apiAgentPersona{}, err
	}
	for _, persona := range personas {
		if persona.ID == agentID {
			return persona, nil
		}
	}
	return apiAgentPersona{}, fmt.Errorf("agent persona %s not found", agentID)
}

func (m *manager) prepareCommandRoute(ctx context.Context, instanceID, taskID string) (struct {
	AgentID string  `json:"agentId"`
	Task    apiTask `json:"task"`
}, error) {
	var out struct {
		AgentID string  `json:"agentId"`
		Task    apiTask `json:"task"`
	}
	err := m.managerRequestJSON(ctx, http.MethodPost, "/agent-instances/"+instanceID+"/prepare-command-route", map[string]any{
		"taskId": strings.TrimSpace(taskID),
	}, &out)
	return out, err
}

func (m *manager) broadcastLivestreamState(instanceID, state, message string) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return
	}
	clients := m.subscribedBrowserClientsForInstance(instanceID)
	payload := map[string]any{
		"type":        "livestream_state",
		"instance_id": instanceID,
		"state":       strings.TrimSpace(state),
	}
	if message = strings.TrimSpace(message); message != "" {
		payload["message"] = message
	}
	for _, client := range clients {
		if err := m.writeBrowserEvent(client, payload); err != nil {
			m.removeBrowserClient(client)
		}
	}
}

func (m *manager) broadcastAgentState(instanceID, state, message string) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return
	}
	clients := m.subscribedBrowserClientsForInstance(instanceID)
	payload := map[string]any{
		"type":        "agent_state",
		"instance_id": instanceID,
		"state":       strings.TrimSpace(state),
	}
	if workingDir := m.workerWorkingDir(instanceID); workingDir != "" {
		payload["working_dir"] = workingDir
	}
	if message = strings.TrimSpace(message); message != "" {
		payload["message"] = message
	}
	for _, client := range clients {
		if err := m.writeBrowserEvent(client, payload); err != nil {
			m.removeBrowserClient(client)
		}
	}
}

func (m *manager) broadcastBothStates(instanceID, livestreamState, agentState, message string) {
	m.broadcastLivestreamState(instanceID, livestreamState, "")
	m.broadcastAgentState(instanceID, agentState, message)
}

func (m *manager) computeLivestreamState(instanceID string) string {
	worker := m.workerForInstanceID(instanceID)
	if worker == nil {
		return "offline"
	}
	if worker.livestreamReady.Load() {
		return "online"
	}
	return "offline"
}

func (m *manager) computeAgentState(instanceID string) (state, message string) {
	worker := m.workerForInstanceID(instanceID)
	if worker == nil {
		return "offline", ""
	}
	if shutdownRequested, _, shutdownMode := worker.shutdownRequestDetails(); shutdownRequested {
		return "shutting_down", workerShutdownStatusMessage(shutdownMode)
	}
	if !worker.livestreamReady.Load() {
		return "waking_up", startupWakingMessageForWorker(worker)
	}
	// Check for deferred bootstrap reasons (OpenCode resume, etc)
	if reason, ok := deferredBootstrapReasonForWorker(worker); ok {
		return "working", reason
	}
	// User has taken over (livestream ready) — show current output and return working state
	status := workerPromptStatusForWorker(worker)
	if strings.TrimSpace(status.Snapshot) != "" {
		return "working", status.Snapshot
	}
	// No snapshot available, fall back to agent status
	status2 := workerAgentStatusForWorker(worker)
	if status2 == "" {
		status2 = "working"
	}
	if status2 == "waking_up" {
		return "waking_up", startupWakingMessageForWorker(worker)
	}
	return status2, ""
}

func startupWakingMessageForWorker(w *worker) string {
	if w != nil && isOpenCodeRuntimeCommand(w.runtimeCommand) {
		return "OpenCode can take 30-60 seconds to start. Give it a moment."
	}
	return "Agent is still waking up. Give it a moment."
}

func workerAgentStatusForWorker(w *worker) string {
	if w == nil {
		return ""
	}
	if !w.usesPTY {
		return "working"
	}
	if !w.bootstrapObserved.Load() {
		w.idleTimerMu.Lock()
		w.idleStartedAt = time.Time{}
		w.idleTimerMu.Unlock()
		return "waking_up"
	}
	status := workerPromptStatusForWorker(w)
	if status.State == workerPromptStateReadyEmpty {
		w.bootstrapReadyEmptySeen.Store(true)
		w.idleTimerMu.Lock()
		if w.idleStartedAt.IsZero() {
			w.idleStartedAt = time.Now()
		}
		idleFor := time.Since(w.idleStartedAt)
		w.idleTimerMu.Unlock()
		if idleFor >= workerIdleDisplayThreshold {
			return "idle"
		}
		return "working"
	}
	// Agent is busy — clear idle start so the clock resets next time it goes idle.
	w.idleTimerMu.Lock()
	w.idleStartedAt = time.Time{}
	w.idleTimerMu.Unlock()
	if !w.bootstrapReadyEmptySeen.Load() {
		if observedAt := w.bootstrapObservedAt.Load(); observedAt > 0 {
			if time.Since(time.Unix(0, observedAt)) >= postBootstrapWorkingGrace {
				return "working"
			}
		}
		return "waking_up"
	}
	return "working"
}

func (m *manager) workerWorkingDir(instanceID string) string {
	worker := m.workerForInstanceID(instanceID)
	if worker == nil {
		return ""
	}
	return strings.TrimSpace(worker.workingDir)
}

func (m *manager) stopStreamServer() {
	if m.streamServer != nil {
		_ = m.streamServer.Close()
		m.streamServer = nil
	}
	if m.streamListener != nil {
		_ = m.streamListener.Close()
		m.streamListener = nil
	}
	m.browserMu.Lock()
	for client := range m.browserClients {
		_ = client.conn.Close()
	}
	m.browserClients = map[*browserClient]struct{}{}
	m.browserMu.Unlock()
}

func (m *manager) startCloudflareTunnel(ctx context.Context) error {
	m.tunnelMu.Lock()
	defer m.tunnelMu.Unlock()
	return m.startCloudflareTunnelLocked(ctx)
}

func (m *manager) startCloudflareTunnelLocked(ctx context.Context) error {
	if m.tunnelCmd != nil && m.tunnelCmd.Process != nil {
		return nil
	}
	if m.cfg.StreamPort <= 0 {
		return fmt.Errorf("stream server port not initialized")
	}

	binary := strings.TrimSpace(m.cfg.CloudflaredBinary)
	var err error
	if binary == "" {
		binary, err = exec.LookPath("cloudflared")
		if err != nil {
			return fmt.Errorf("cloudflared not found: %w", err)
		}
	}

	token := strings.TrimSpace(m.tunnelToken)
	if token == "" {
		return fmt.Errorf("missing tunnel token")
	}

	cmd := exec.Command(
		binary,
		"tunnel",
		"run",
		"--token",
		token,
		"--url",
		fmt.Sprintf("http://127.0.0.1:%d", m.cfg.StreamPort),
	)
	if parsedTunnelID := parseTunnelIDFromToken(token); parsedTunnelID != "" {
		m.tunnelID = parsedTunnelID
	}
	writer := newCommandLogger(m, "cloudflared")
	cmd.Stdout = writer
	cmd.Stderr = writer
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start cloudflared: %w", err)
	}
	m.tunnelCmd = cmd
	go func(startedCmd *exec.Cmd) {
		defer m.setTunnelReady(false)
		if err := startedCmd.Wait(); err != nil && ctx.Err() == nil {
			m.logError("cloudflared_exit_error err=%v", err)
		}
		m.tunnelMu.Lock()
		if m.tunnelCmd == startedCmd {
			m.tunnelCmd = nil
		}
		m.tunnelMu.Unlock()
	}(cmd)
	if m.tunnelID != "" {
		m.logInfo("cloudflared_tunnel_started tunnel_id=%s", m.tunnelID)
	} else {
		m.logInfo("cloudflared_tunnel_started")
	}
	return nil
}

func (m *manager) stopCloudflareTunnel() {
	m.tunnelMu.Lock()
	defer m.tunnelMu.Unlock()
	m.stopCloudflareTunnelLocked()
}

func (m *manager) tunnelWatchLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.ensureCloudflareTunnelRunning(ctx); err != nil {
				m.logError("cloudflared_restart_error err=%v", err)
			}
		}
	}
}

func (m *manager) ensureCloudflareTunnelRunning(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	m.tunnelMu.Lock()
	defer m.tunnelMu.Unlock()
	if m.tunnelCmd != nil && m.tunnelCmd.Process != nil && m.tunnelReady.Load() {
		return nil
	}
	m.stopCloudflareTunnelLocked()
	return m.startCloudflareTunnelLocked(ctx)
}

func (m *manager) stopCloudflareTunnelLocked() {
	if m.tunnelCmd == nil || m.tunnelCmd.Process == nil {
		m.tunnelCmd = nil
		m.setTunnelReady(false)
		return
	}
	m.setTunnelReady(false)
	if runtime.GOOS == "windows" {
		_ = m.tunnelCmd.Process.Kill()
	} else {
		_ = m.tunnelCmd.Process.Signal(syscall.SIGTERM)
	}
	m.tunnelCmd = nil
}

func (m *manager) ensureRegistered(ctx context.Context) error {
	if err := m.registerManager(ctx); err != nil {
		if isRefreshTokenRotationError(err) {
			return err
		}
		if strings.TrimSpace(m.state.ManagerID) == "" || strings.TrimSpace(m.state.TunnelToken) == "" {
			return err
		}
		m.logError("manager_registration_sync_failed err=%v", err)
	}
	return nil
}

func (m *manager) registerManager(ctx context.Context) error {
	// After the first successful registration, reuse local manager credentials.
	// If either piece is missing locally, re-fetch the stored manager credentials.
	if strings.TrimSpace(m.state.ManagerID) != "" &&
		strings.TrimSpace(m.state.TunnelToken) != "" &&
		strings.TrimSpace(loadManagerRecoveryToken(m.state)) != "" {
		m.logInfo("manager_already_registered manager_id=%s", m.state.ManagerID)
		return nil
	}
	userJWT, err := m.getValidUserAccessToken(ctx)
	if err != nil && isRefreshTokenRotationError(err) {
		if _, reloadErr := m.getFreshStoredSession(ctx, true); reloadErr == nil {
			userJWT, err = m.getValidUserAccessToken(ctx)
		}
	}
	if err != nil {
		friendly := wrapRefreshTokenError(err)
		if !isRefreshTokenRotationError(err) {
			m.logError("register_manager_failed_no_user_session err=%v", friendly)
		}
		return friendly
	}
	payload := map[string]any{
		"machineName":            m.cfg.MachineName,
		"executionMode":          m.cfg.ExecutionMode,
		"maxConcurrentInstances": m.cfg.MaxConcurrent,
		"supportedRuntimeIds":    []string{},
		"managerVersion":         currentManagerVersion(),
		"platform":               runtime.GOOS,
		"installChannel":         currentInstallChannel(),
	}
	if session, loadErr := m.loadSession(); loadErr == nil && strings.TrimSpace(session.RefreshToken) != "" {
		payload["refreshToken"] = strings.TrimSpace(session.RefreshToken)
	}
	var out struct {
		MachineID            string `json:"machineId"`
		ManagerRecoveryToken string `json:"managerRecoveryToken"`
		ManagerSubdomain     string `json:"managerSubdomain"`
		TunnelID             string `json:"tunnelId"`
		TunnelToken          string `json:"tunnelToken"`
	}
	if err := m.requestJSON(
		ctx,
		http.MethodPost,
		"/local-agent-managers/register",
		payload,
		&out,
		userJWT,
	); err != nil {
		m.logError("register_manager_request_failed err=%v", err)
		return err
	}

	m.state.ManagerID = out.MachineID
	if strings.TrimSpace(out.ManagerRecoveryToken) != "" {
		m.persistManagerRecoveryToken(out.ManagerRecoveryToken)
	}
	if strings.TrimSpace(out.ManagerSubdomain) != "" {
		m.state.ManagerSubdomain = out.ManagerSubdomain
		m.managerSubdomain = out.ManagerSubdomain
	}
	if strings.TrimSpace(out.TunnelToken) != "" {
		m.state.TunnelToken = out.TunnelToken
		m.tunnelToken = out.TunnelToken
	}
	if strings.TrimSpace(out.TunnelID) != "" {
		m.tunnelID = strings.TrimSpace(out.TunnelID)
	}
	m.logInfo("manager_registered manager_id=%s", m.state.ManagerID)
	return writeState(m.cfg.StateFile, m.state)
}

func (m *manager) refreshManagerUserIdentity(ctx context.Context) error {
	userJWT, err := m.getValidUserAccessToken(ctx)
	if err != nil {
		return err
	}
	claims, err := m.validateSupabaseJWT(userJWT)
	if err != nil {
		return err
	}
	userID := strings.TrimSpace(claimString(claims, "sub"))
	if userID == "" {
		return fmt.Errorf("authenticated session missing user id")
	}
	m.mu.Lock()
	m.userID = userID
	m.mu.Unlock()
	return nil
}

func (m *manager) resourceLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.ResourceInterval)
	defer ticker.Stop()
	for {
		if err := m.refreshResourceMetrics(); err != nil {
			m.logError("resource_monitor_error err=%v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *manager) refreshResourceMetrics() error {
	cpuPct, cpuOK, cpuErr := m.sampleCPUPercent()
	vm, memErr := virtualMemoryFunc()

	m.mu.Lock()
	if cpuErr != nil {
		m.cpu = 0
		m.cpuHighStreak = 0
	} else if cpuOK {
		m.cpu = cpuPct
		if m.cpu >= m.cfg.CPUThreshold {
			m.cpuHighStreak++
		} else {
			m.cpuHighStreak = 0
		}
	}
	if memErr == nil {
		m.freeMB = float64(vm.Available) / 1024 / 1024
		m.totalMB = float64(vm.Total) / 1024 / 1024
		if vm.Total > 0 {
			m.freePct = (float64(vm.Available) / float64(vm.Total)) * 100
		}
	}
	m.mu.Unlock()
	m.refreshWorkerResourceObservations(time.Now())

	if cpuErr != nil && memErr != nil {
		return fmt.Errorf("resource metrics unavailable: cpu=%v memory=%v", cpuErr, memErr)
	}
	if cpuErr != nil {
		if memErr == nil {
			return fmt.Errorf("cpu metrics unavailable; memory metrics updated: %w", cpuErr)
		}
		return fmt.Errorf("cpu metrics unavailable: %w", cpuErr)
	}
	if memErr != nil {
		return fmt.Errorf("memory metrics unavailable: %w", memErr)
	}
	return nil
}

func (m *manager) sampleCPUPercent() (float64, bool, error) {
	times, err := cpuTimesFunc(false)
	if err != nil {
		return 0, false, err
	}
	if len(times) == 0 {
		return 0, false, fmt.Errorf("cpu times unavailable")
	}

	current := times[0]

	m.mu.Lock()
	previous := m.lastCPUTimes
	m.lastCPUTimes = &current
	m.mu.Unlock()

	if previous == nil {
		return 0, false, nil
	}

	totalDelta := cpuTotalTime(current) - cpuTotalTime(*previous)
	if totalDelta <= 0 {
		return 0, false, nil
	}

	busyDelta := cpuBusyTime(current) - cpuBusyTime(*previous)
	if busyDelta <= 0 {
		return 0, true, nil
	}

	return math.Min(100, math.Max(0, (busyDelta/totalDelta)*100)), true, nil
}

func cpuTotalTime(sample cpu.TimesStat) float64 {
	return sample.User + sample.System + sample.Idle + sample.Nice + sample.Iowait + sample.Irq +
		sample.Softirq + sample.Steal + sample.Guest + sample.GuestNice
}

func cpuBusyTime(sample cpu.TimesStat) float64 {
	total := cpuTotalTime(sample)
	if managerRuntimeGOOS() == "linux" {
		total -= sample.Guest
		total -= sample.GuestNice
	}
	return total - sample.Idle - sample.Iowait
}

func (m *manager) refreshWorkerResourceObservations(now time.Time) {
	m.mu.Lock()
	workers := make([]*worker, 0, len(m.workers))
	for _, w := range m.workers {
		if w != nil {
			workers = append(workers, w)
		}
	}
	m.mu.Unlock()

	for _, w := range workers {
		observeWorkerResources(w, now)
	}
}

func observeWorkerResources(w *worker, now time.Time) {
	if w == nil || w.cmd == nil || w.cmd.Process == nil {
		return
	}
	pid := int32(w.cmd.Process.Pid)
	if pid <= 0 {
		return
	}

	proc, err := process.NewProcess(pid)
	if err != nil {
		return
	}

	observed := false
	rssMB := 0.0
	if memInfo, memErr := proc.MemoryInfo(); memErr == nil && memInfo != nil {
		rssMB = float64(memInfo.RSS) / 1024 / 1024
		observed = true
	}

	cpuCores := 0.0
	if cpuPct, cpuErr := proc.Percent(0); cpuErr == nil {
		cpuCores = maxFloat64(cpuPct/100.0, 0)
		observed = true
	}

	if !observed {
		return
	}

	w.resourceMu.Lock()
	if rssMB > 0 {
		w.observedRSSMB = smoothObservedMetric(w.observedRSSMB, rssMB)
	}
	w.observedCPUCores = smoothObservedMetric(w.observedCPUCores, cpuCores)
	w.lastResourceSampleAt = now
	w.resourceMu.Unlock()
}

func smoothObservedMetric(previous, observed float64) float64 {
	if observed < 0 {
		observed = 0
	}
	if previous <= 0 {
		return observed
	}
	return previous*0.65 + observed*0.35
}

func (m *manager) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		_ = m.sendHeartbeat(ctx)
		if err := m.refreshAllowedFoldersFromBackend(ctx); err != nil {
			m.logError("allowed_folders_refresh_error err=%v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *manager) sendHeartbeat(ctx context.Context) error {
	m.mu.Lock()
	cpuVal, memVal, totalMB := m.cpu, m.freeMB, m.totalMB
	managerID := m.state.ManagerID
	m.mu.Unlock()
	tunnelReady := m.tunnelReady.Load()
	capacity := m.currentCapacitySnapshot()

	payload := map[string]any{
		"resource_metrics": map[string]any{
			"cpu":                    cpuVal,
			"memory":                 memVal,
			"memory_total_mb":        totalMB,
			"current_running_agents": capacity.CurrentRunningAgents,
			"max_parallel_agents":    capacity.MaxParallelAgents,
			"tunnel_ready":           tunnelReady,
		},
		"manager_version": currentManagerVersion(),
		"platform":        runtime.GOOS,
		"install_channel": currentInstallChannel(),
	}
	if tunnelReady && m.tunnelID != "" {
		payload["tunnel_id"] = m.tunnelID
	}
	return m.managerRequestJSON(ctx, http.MethodPatch, "/api/managers/"+managerID+"/heartbeat", payload, nil)
}

func (m *manager) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	runPoll := func(bypassSpawnCooldown bool) bool {
		if err := m.pollOnce(ctx, bypassSpawnCooldown); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				select {
				case <-ctx.Done():
					return false
				default:
				}
			}
			m.logError("task_poll_error err=%v", err)
		}
		return true
	}

	if !runPoll(false) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !runPoll(false) {
				return
			}
		case <-m.pollNow:
			if !runPoll(true) {
				return
			}
		}
	}
}

type lessonsSyncPersonaPayload struct {
	AgentPersonaID string `json:"agentPersonaId"`
	ContentHash    string `json:"contentHash"`
	Content        string `json:"content"`
}

type lessonsSyncRequest struct {
	Personas []lessonsSyncPersonaPayload `json:"personas"`
}

type lessonsSyncResponse struct {
	Results []struct {
		AgentPersonaID string `json:"agentPersonaId"`
		Status         string `json:"status"`
		Inserted       int    `json:"inserted"`
		Error          string `json:"error"`
	} `json:"results"`
}

type lessonsExportItem struct {
	AgentPersonaID string   `json:"agent_persona_id"`
	PersonaName    string   `json:"persona_name"`
	Lessons        []string `json:"lessons"`
}

func (m *manager) lessonsSyncLoop(ctx context.Context) {
	_ = m.syncLessonsOnce(ctx)
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = m.syncLessonsOnce(ctx)
		}
	}
}

func (m *manager) lessonsPullLoop(ctx context.Context) {
	_ = m.pullLessonsOnce(ctx)
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = m.pullLessonsOnce(ctx)
		}
	}
}

func (m *manager) syncLessonsOnce(ctx context.Context) error {
	return m.syncLessonsForPersonaIDs(ctx, nil)
}

func (m *manager) syncLessonsForAgent(agentID string) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return
	}
	go func() {
		syncCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if err := m.syncLessonsForPersonaIDs(syncCtx, []string{agentID}); err != nil {
			m.logError("lessons_sync_after_completion_error agent_id=%s err=%v", agentID, err)
		}
	}()
}

func (m *manager) syncLessonsForPersonaIDs(ctx context.Context, personaIDs []string) error {
	var personas []apiAgentPersona
	if err := m.managerRequestJSON(
		ctx,
		http.MethodGet,
		"/managers/tasks/agent-personas",
		nil,
		&personas,
	); err != nil {
		m.logError("lessons_sync_personas_error err=%v", err)
		return err
	}

	filteredIDs := make(map[string]struct{}, len(personaIDs))
	for _, personaID := range personaIDs {
		personaID = strings.TrimSpace(personaID)
		if personaID == "" {
			continue
		}
		filteredIDs[personaID] = struct{}{}
	}

	payload := lessonsSyncRequest{Personas: make([]lessonsSyncPersonaPayload, 0)}
	for _, persona := range personas {
		if len(filteredIDs) > 0 {
			if _, ok := filteredIDs[persona.ID]; !ok {
				continue
			}
		}
		content, hash, err := m.loadPersonaLessonsFile(persona.ID)
		if err != nil {
			m.logError(
				"lessons_sync_file_error persona_id=%s persona_name=%s err=%v",
				persona.ID,
				persona.Name,
				err,
			)
			continue
		}
		if !m.shouldSyncPersonaLessons(persona.ID, hash) {
			continue
		}
		payload.Personas = append(payload.Personas, lessonsSyncPersonaPayload{
			AgentPersonaID: persona.ID,
			ContentHash:    hash,
			Content:        content,
		})
	}

	if len(payload.Personas) == 0 {
		return nil
	}

	var response lessonsSyncResponse
	if err := m.userRequestJSON(
		ctx,
		http.MethodPost,
		"/agent-lessons/sync",
		payload,
		&response,
	); err != nil {
		m.logError("lessons_sync_request_error err=%v", err)
		return err
	}

	resultByPersona := make(map[string]struct {
		Status string
		Error  string
	})
	for _, result := range response.Results {
		resultByPersona[result.AgentPersonaID] = struct {
			Status string
			Error  string
		}{
			Status: strings.TrimSpace(result.Status),
			Error:  strings.TrimSpace(result.Error),
		}
	}

	for _, row := range payload.Personas {
		result, ok := resultByPersona[row.AgentPersonaID]
		if !ok {
			continue
		}
		if result.Status == "failed" {
			m.logError(
				"lessons_sync_failed persona_id=%s err=%s",
				row.AgentPersonaID,
				result.Error,
			)
			continue
		}
		m.updatePersonaLessonHash(row.AgentPersonaID, row.ContentHash)
	}

	return nil
}

func (m *manager) pullLessonsOnce(ctx context.Context) error {
	var exports []lessonsExportItem
	if err := m.userRequestJSON(ctx, http.MethodGet, "/agent-lessons/export", nil, &exports); err != nil {
		m.logError("lessons_pull_error err=%v", err)
		return err
	}

	for _, item := range exports {
		content := "# Lessons Learned\n\n"
		if len(item.Lessons) == 0 {
			content += "- (none yet)\n"
		} else {
			for _, lesson := range item.Lessons {
				lesson = strings.TrimSpace(lesson)
				if lesson == "" {
					continue
				}
				content += "- " + lesson + "\n"
			}
		}

		path, err := m.lessonFilePath(item.AgentPersonaID)
		if err != nil {
			m.logError(
				"lessons_pull_path_error persona_id=%s persona_name=%s err=%v",
				item.AgentPersonaID,
				item.PersonaName,
				err,
			)
			continue
		}
		if err := writeFileAtomic(path, []byte(content), 0o600, 0o700); err != nil {
			m.logError(
				"lessons_pull_write_error persona_id=%s persona_name=%s err=%v",
				item.AgentPersonaID,
				item.PersonaName,
				err,
			)
			continue
		}

		sum := sha256.Sum256([]byte(content))
		hash := fmt.Sprintf("%x", sum[:])
		m.updatePersonaLessonHash(item.AgentPersonaID, hash)
	}

	m.mu.Lock()
	m.state.LastLessonsPullAt = time.Now().UTC().Format(time.RFC3339Nano)
	_ = writeState(m.cfg.StateFile, m.state)
	m.mu.Unlock()
	return nil
}

func (m *manager) shouldSyncPersonaLessons(agentPersonaID, contentHash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	last, ok := m.state.PersonaLessonHashes[agentPersonaID]
	return !ok || strings.TrimSpace(last) != strings.TrimSpace(contentHash)
}

func (m *manager) updatePersonaLessonHash(agentPersonaID, contentHash string) {
	m.mu.Lock()
	if m.state.PersonaLessonHashes == nil {
		m.state.PersonaLessonHashes = map[string]string{}
	}
	m.state.PersonaLessonHashes[agentPersonaID] = strings.TrimSpace(contentHash)
	_ = writeState(m.cfg.StateFile, m.state)
	m.mu.Unlock()
}

func (m *manager) loadPersonaLessonsFile(personaID string) (string, string, error) {
	path, err := m.lessonFilePath(personaID)
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", "", err
	}

	initial := []byte("# Lessons Learned\n\n- (none yet)\n")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := writeFileAtomic(path, initial, 0o600, 0o700); err != nil {
			return "", "", err
		}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	content := string(raw)
	sum := sha256.Sum256(raw)
	return content, fmt.Sprintf("%x", sum[:]), nil
}

func (m *manager) lessonFilePath(personaID string) (string, error) {
	personaID = strings.TrimSpace(personaID)
	if personaID == "" {
		return "", fmt.Errorf("missing persona id")
	}
	if _, err := uuid.Parse(personaID); err != nil {
		return "", fmt.Errorf("invalid persona id: %w", err)
	}
	return filepath.Join(m.cfg.LessonsBaseDir, personaID, "lessons.md"), nil
}

func (m *manager) personaFilePath(personaID string) (string, error) {
	personaID = strings.TrimSpace(personaID)
	if personaID == "" {
		return "", fmt.Errorf("missing persona id")
	}
	if _, err := uuid.Parse(personaID); err != nil {
		return "", fmt.Errorf("invalid persona id: %w", err)
	}
	return filepath.Join(m.cfg.LessonsBaseDir, personaID, "AGENT_PERSONA.md"), nil
}

func (m *manager) writePersonaPromptFile(persona apiAgentPersona, lessonsFilePath string) (string, error) {
	path, err := m.personaFilePath(persona.ID)
	if err != nil {
		return "", err
	}
	content := buildPersonaDocument(persona, lessonsFilePath)
	if err := writeFileAtomic(path, []byte(content), 0o600, 0o700); err != nil {
		return "", err
	}
	return path, nil
}

func (m *manager) requestImmediatePoll() {
	select {
	case m.pollNow <- struct{}{}:
	default:
	}
}

func (m *manager) pollOnce(ctx context.Context, bypassSpawnCooldown bool) error {
	m.mu.Lock()
	busyCount := len(m.workers)
	cpuVal, memVal, memPct := m.cpu, m.freeMB, m.freePct
	cpuHighStreak := m.cpuHighStreak
	lastSpawnAt := m.lastSpawnAt
	m.mu.Unlock()
	dynamicMaxSlots := m.calculateDynamicMaxSlots()
	if !bypassSpawnCooldown && !lastSpawnAt.IsZero() && time.Since(lastSpawnAt) < spawnCooldown {
		m.logInfo("poll_skipped_spawn_cooldown since_last_spawn=%s", time.Since(lastSpawnAt).Truncate(time.Millisecond))
		return nil
	}
	if !hasPollingCapacity(
		busyCount,
		dynamicMaxSlots,
		cpuVal,
		m.cfg.CPUThreshold,
		memVal,
		memPct,
		cpuHighStreak,
	) {
		m.logInfo(
			"poll_skipped_capacity busy=%d max=%d cpu=%.2f cpu_threshold=%.2f cpu_high_streak=%d free_mb=%.2f free_pct=%.2f",
			busyCount,
			dynamicMaxSlots,
			cpuVal,
			m.cfg.CPUThreshold,
			cpuHighStreak,
			memVal,
			memPct,
		)
		return nil
	}

	var personas []apiAgentPersona
	if err := m.managerRequestJSON(ctx, http.MethodGet, "/managers/tasks/agent-personas", nil, &personas); err != nil {
		return err
	}
	m.logInfo("poll_personas_loaded count=%d", len(personas))
	if len(personas) == 0 {
		return nil
	}

	agentIDs := make([]string, 0, len(personas))
	for _, p := range personas {
		agentIDs = append(agentIDs, p.ID)
	}

	var tasks []apiTask
	if err := m.managerRequestJSON(ctx, http.MethodPost, "/managers/tasks/eligible", map[string]any{"agentIds": agentIDs}, &tasks); err != nil {
		return err
	}
	m.logInfo("poll_tasks_loaded count=%d", len(tasks))
	if err := writePollSnapshot(m.cfg.PollSnapshotFile, personas, tasks); err != nil {
		m.logError("poll_snapshot_write_error err=%v", err)
	}

	remainingCapacity := dynamicMaxSlots - busyCount
	if remainingCapacity <= 0 {
		return nil
	}

	m.mu.Lock()
	runningTaskIDs := map[string]bool{}
	for _, w := range m.workers {
		runningTaskIDs[w.taskID] = true
	}
	m.mu.Unlock()

	for _, t := range tasks {
		if remainingCapacity <= 0 {
			break
		}
		if runningTaskIDs[t.ID] {
			continue
		}
		p := choosePersonaForTask(personas, t)
		if p == nil {
			m.logInfo("task_skip_no_eligible_persona task_id=%s", t.ID)
			continue
		}
		if strings.TrimSpace(p.RuntimeID) == "" {
			m.logError(
				"task_skip_persona_missing_runtime task_id=%s persona_id=%s",
				t.ID,
				p.ID,
			)
			continue
		}
		// Re-check capacity under the lock immediately before spawning so the
		// check and the slot reservation are atomic with respect to concurrent
		// goroutines that may also be adding workers.
		m.mu.Lock()
		liveSlotsFree := dynamicMaxSlots - len(m.workers)
		m.mu.Unlock()
		if liveSlotsFree <= 0 {
			break
		}
		m.logInfo("task_spawn_attempt task_id=%s persona_id=%s", t.ID, p.ID)
		if err := m.spawnWorker(ctx, *p, t, dynamicMaxSlots, ""); err != nil {
			m.logError("spawn_error task_id=%s persona_id=%s err=%v", t.ID, p.ID, err)
			continue
		}
		m.mu.Lock()
		m.lastSpawnAt = time.Now()
		m.mu.Unlock()
		m.logInfo("task_spawned task_id=%s persona_id=%s", t.ID, p.ID)
		remainingCapacity--
	}
	return nil
}

func hasPollingCapacity(
	busyCount, maxConcurrent int,
	cpuVal, cpuThreshold, freeMB, freePct float64,
	cpuHighStreak int,
) bool {
	if busyCount >= maxConcurrent {
		return false
	}
	if cpuVal >= cpuThreshold && cpuHighStreak >= cpuHighStreakThreshold {
		return false
	}
	// Only block on RAM utilization cap when absolute free memory is also low.
	// High utilization % with plenty of free MB (e.g. 82% used but 2 GB free) is fine.
	if freePct > 0 && (100-freePct) >= ramUtilizationCapPct && freeMB <= 1024 {
		return false
	}
	if !(freePct > 10 || freeMB > 1024) {
		return false
	}
	return true
}

func (m *manager) calculateDynamicMaxSlots() int {
	return m.currentCapacitySnapshot().MaxParallelAgents
}

type pollSnapshot struct {
	CapturedAt string            `json:"captured_at"`
	Personas   []apiAgentPersona `json:"personas"`
	Tasks      []apiTask         `json:"tasks"`
}

func writePollSnapshot(path string, personas []apiAgentPersona, tasks []apiTask) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	payload := pollSnapshot{
		CapturedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Personas:   personas,
		Tasks:      tasks,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, raw, 0o600, 0o700)
}

func choosePersonaForTask(personas []apiAgentPersona, task apiTask) *apiAgentPersona {
	if len(task.EligibleAgentIDs) == 0 {
		return &personas[0]
	}
	allowed := map[string]struct{}{}
	for _, id := range task.EligibleAgentIDs {
		allowed[id] = struct{}{}
	}
	for i := range personas {
		if _, ok := allowed[personas[i].ID]; ok {
			return &personas[i]
		}
	}
	return nil
}

func (m *manager) spawnWorker(ctx context.Context, persona apiAgentPersona, task apiTask, targetSlots int, preferredInstanceID string) (err error) {
	return m.spawnWorkerWithWorkingDir(ctx, persona, task, targetSlots, preferredInstanceID, "")
}

func (m *manager) spawnWorkerWithWorkingDir(
	ctx context.Context,
	persona apiAgentPersona,
	task apiTask,
	targetSlots int,
	preferredInstanceID string,
	workingDirOverride string,
) (err error) {
	workingDir := strings.TrimSpace(workingDirOverride)
	if workingDir == "" {
		workingDir, err = m.resolveTaskWorkingDir(ctx, task)
	} else {
		err = os.MkdirAll(workingDir, 0o755)
	}
	if err != nil {
		m.reportTaskFolderError(ctx, task, err)
		return err
	}

	instanceID, managerSubdomain, err := m.registerAgentInstance(ctx, persona, targetSlots, preferredInstanceID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(managerSubdomain) != "" {
		m.mu.Lock()
		m.state.ManagerSubdomain = managerSubdomain
		m.managerSubdomain = managerSubdomain
		_ = writeState(m.cfg.StateFile, m.state)
		m.mu.Unlock()
	}

	claimPayload := map[string]string{"taskId": task.ID, "agentId": persona.ID}
	var claimOut struct {
		Claimed bool `json:"claimed"`
	}
	if err := m.managerRequestJSON(
		ctx,
		http.MethodPost,
		"/agent-instances/"+instanceID+"/claim-task",
		claimPayload,
		&claimOut,
	); err != nil {
		return err
	}
	if !claimOut.Claimed {
		return fmt.Errorf("task claim was not granted")
	}
	m.logInfo("task_status_updated task_id=%s status=IN_PROGRESS", task.ID)

	checkpoints, checkpointsErr := m.fetchTaskCheckpoints(ctx, task.ID, 5)
	if checkpointsErr != nil {
		m.logError("task_checkpoints_fetch_error task_id=%s err=%v", task.ID, checkpointsErr)
	}
	lessonsContent, _, lessonsLoadErr := m.loadPersonaLessonsFile(persona.ID)
	if lessonsLoadErr != nil {
		m.logError("lessons_file_load_error agent_id=%s err=%v", persona.ID, lessonsLoadErr)
		lessonsContent = defaultLessonsDocument()
	}
	personaFilePath, lessonsFilePath, taskContextFilePath, workspaceFilesErr := writeTaskWorkspaceFiles(
		workingDir,
		persona,
		task,
		checkpoints,
		lessonsContent,
	)
	if workspaceFilesErr != nil {
		m.logError("workspace_files_write_error instance_id=%s err=%v", instanceID, workspaceFilesErr)
		personaFilePath = ""
		lessonsFilePath = ""
		taskContextFilePath = ""
	}
	prompt := buildPersonaPrompt(
		persona,
		task,
		checkpoints,
		lessonsFilePath,
		personaFilePath,
		taskContextFilePath,
	)

	command := strings.TrimSpace(persona.AgentRuntime.CommandTemplate)
	if command == "" {
		command = "gemini"
	}
	launchCommand := command
	if supportsStartupBootstrapPrompt(command) {
		launchCommand = commandWithStartupBootstrapPrompt(command, prompt)
	}
	sessionID := uuid.NewString()
	w, err := m.startBootstrapWorker(
		ctx,
		instanceID,
		task.ID,
		persona.ID,
		sessionID,
		workingDir,
		command,
		launchCommand,
	)
	if err != nil {
		return err
	}

	if supportsStartupBootstrapPrompt(command) {
		if reason, ok := deferredBootstrapReasonForWorker(w); ok {
			m.logInfo(
				"bootstrap_prompt_waiting_for_user instance_id=%s milestone=bootstrap_waiting_on_user livestream_state=%s agent_state=%s reason=%q runtime=%s",
				w.instanceID,
				workerLivestreamStateForWorker(w),
				workerAgentStatusForWorker(w),
				reason,
				strings.TrimSpace(w.runtimeCommand),
			)
		}
		m.markWorkerBootstrapObserved(w)
		m.logWorkerStateMilestone(w, "bootstrap_prompt_started_with_runtime")
		m.logInfo("instance_spawned instance_id=%s task_id=%s agent_id=%s", instanceID, task.ID, persona.ID)
		return nil
	}

	if reason, ok := deferredBootstrapReasonForWorker(w); ok {
		m.logInfo(
			"bootstrap_prompt_waiting_for_user instance_id=%s milestone=bootstrap_waiting_on_user livestream_state=%s agent_state=%s reason=%q runtime=%s",
			w.instanceID,
			workerLivestreamStateForWorker(w),
			workerAgentStatusForWorker(w),
			reason,
			strings.TrimSpace(w.runtimeCommand),
		)
		m.unblockWorkerAfterBootstrapTimeout(w)
		m.logInfo("instance_spawned instance_id=%s task_id=%s agent_id=%s", instanceID, task.ID, persona.ID)
		return nil
	}

	if err := m.writeSpawnBootstrapPrompt(ctx, w, prompt); err != nil {
		m.logWarn("prompt_submission_unconfirmed instance_id=%s err=%v", instanceID, err)
		_ = m.maybeForceBootstrapReadyAfterObservationTimeout(w, err)
	} else {
		m.markWorkerBootstrapObserved(w)
	}

	m.logInfo("instance_spawned instance_id=%s task_id=%s agent_id=%s", instanceID, task.ID, persona.ID)
	return nil
}

func (m *manager) startBootstrapWorker(
	ctx context.Context,
	instanceID, taskID, agentID, sessionID, workingDir, runtimeCommand, launchCommand string,
) (*worker, error) {
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		w, err := m.startWorkerProcess(
			ctx,
			instanceID,
			taskID,
			agentID,
			sessionID,
			workingDir,
			runtimeCommand,
			launchCommand,
		)
		if err != nil {
			lastErr = err
			if attempt == 2 {
				return nil, err
			}
			m.logWarn("agent_start_retrying instance_id=%s attempt=%d err=%v", instanceID, attempt, err)
			continue
		}

		attachAddr := m.attachWorkerRuntime(w)
		m.trackWorker(ctx, w, attachAddr)
		if w.usesPTY {
			m.markWorkerLivestreamReady(w, "Agent connected in livestream.")
		}

		readyErr := m.waitBeforeBootstrapPrompt(ctx, w, bootstrapPromptDelayForCommand(runtimeCommand))
		if readyErr == nil {
			return w, nil
		}
		m.logAgentReadyWaitUnconfirmed(instanceID, w, readyErr)

		select {
		case <-w.done:
			lastErr = readyErr
			if attempt == 2 {
				return nil, readyErr
			}
			if cleanupErr := m.waitForWorkerUntracked(ctx, w); cleanupErr != nil {
				return nil, cleanupErr
			}
			m.broadcastAgentState(instanceID, "waking_up", "Resuming the agent session.")
			m.logWarn("agent_start_retrying instance_id=%s attempt=%d err=%v", instanceID, attempt, readyErr)
			continue
		default:
			return w, nil
		}
	}
	return nil, lastErr
}

func (m *manager) waitForWorkerUntracked(ctx context.Context, w *worker) error {
	if w == nil {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		if m.workerForInstanceID(w.instanceID) != w {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("worker cleanup did not finish before restart: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func bootstrapPromptDelayForCommand(command string) time.Duration {
	if isGeminiRuntimeCommand(command) {
		return geminiBootstrapDelay
	}
	return bootstrapPromptDelay
}

func supportsStartupBootstrapPrompt(command string) bool {
	return isGeminiRuntimeCommand(command) || isCodexRuntimeCommand(command) || isOpenCodeRuntimeCommand(command) || isClaudeRuntimeCommand(command)
}

func commandWithStartupBootstrapPrompt(command, prompt string) string {
	command = strings.TrimSpace(command)
	prompt = strings.TrimSpace(prompt)
	if command == "" || prompt == "" {
		return command
	}
	if isGeminiRuntimeCommand(command) {
		return command + " -i " + shellQuoteArgument(prompt)
	}
	if isOpenCodeRuntimeCommand(command) {
		return command + " --prompt " + shellQuoteArgument(prompt)
	}
	return command + " " + shellQuoteArgument(prompt)
}

func shellQuoteArgument(arg string) string {
	if runtime.GOOS == "windows" {
		return strconv.Quote(arg)
	}
	return "'" + strings.ReplaceAll(arg, "'", `'"'"'`) + "'"
}

func (m *manager) logAgentReadyWaitUnconfirmed(instanceID string, w *worker, readyErr error) {
	m.logWarn("agent_ready_wait_unconfirmed instance_id=%s err=%v", instanceID, readyErr)
	if w == nil {
		return
	}
	wStatus := workerPromptStatusForWorker(w)
	processAlive := "running"
	select {
	case <-w.done:
		processAlive = "exited"
	default:
	}
	m.logWarn("agent_ready_wait_diagnostic instance_id=%s prompt_state=%v process=%s runtime=%s",
		instanceID, wStatus.State, processAlive, w.runtimeCommand)
	if snapshot := workerReadySnapshot(w); snapshot != "" {
		lines := strings.Split(strings.TrimSpace(snapshot), "\n")
		if start := len(lines) - 5; start > 0 {
			lines = lines[start:]
		}
		tail := strings.Join(lines, " | ")
		if len(tail) > 300 {
			tail = "..." + tail[len(tail)-300:]
		}
		m.logWarn("agent_ready_wait_terminal instance_id=%s tail=%q", instanceID, tail)
	}
}

func buildPersonaPrompt(
	persona apiAgentPersona,
	task apiTask,
	checkpoints []apiTaskCheckpoint,
	lessonsFilePath string,
	personaFilePath string,
	taskContextFilePath string,
) string {
	parts := []string{
		"# Session Start",
		"- Read AGENT_PERSONA.md, lessons.md, and TASK_CONTEXT.md before doing anything else.",
	}
	if strings.TrimSpace(personaFilePath) != "" {
		parts = append(parts, "- Persona file: "+strings.TrimSpace(personaFilePath))
	}
	if strings.TrimSpace(lessonsFilePath) != "" {
		parts = append(parts, "- Lessons file: "+strings.TrimSpace(lessonsFilePath))
	}
	if strings.TrimSpace(taskContextFilePath) != "" {
		parts = append(parts, "- Task context file: "+strings.TrimSpace(taskContextFilePath))
	}
	parts = append(parts, "", buildTaskContextDocument(task, checkpoints))
	return strings.Join(parts, "\n\n")
}

func valueOrPlaceholder(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "- (not provided)"
	}
	return value
}

func buildPersonaDocument(persona apiAgentPersona, lessonsFilePath string) string {
	section := func(title, value string) string {
		return fmt.Sprintf("## %s\n%s", title, valueOrPlaceholder(value))
	}

	lessonLines := make([]string, 0, len(persona.Lessons))
	for _, lesson := range persona.Lessons {
		lesson = strings.TrimSpace(lesson)
		if lesson == "" {
			continue
		}
		lessonLines = append(lessonLines, "- "+lesson)
	}
	lessonSection := "## Persona Lessons\n- (none synced yet)"
	if len(lessonLines) > 0 {
		lessonSection = "## Persona Lessons\n" + strings.Join(lessonLines, "\n")
	}

	selfImprovementLines := []string{
		"- Read this file at the start of every session.",
		"- If the user corrects you or you discover a durable mistake pattern, update lessons.md before finishing the task.",
		"- Do not add duplicate lessons.",
		"- Keep lessons short, concrete, and action-oriented.",
	}
	if strings.TrimSpace(lessonsFilePath) != "" {
		selfImprovementLines = append([]string{
			"- Lessons file: " + strings.TrimSpace(lessonsFilePath),
			"- Review relevant lessons from lessons.md before starting implementation.",
			"- Write new lessons directly into lessons.md when you learn something durable.",
		}, selfImprovementLines...)
	}

	parts := []string{
		fmt.Sprintf("# Agent Persona: %s", strings.TrimSpace(persona.Name)),
		section("Role", persona.Role),
		section("Personality", persona.Personality),
		section("Instructions", persona.Instructions),
		section("Do and Do Not Rules", persona.Guardrails),
		section("Examples", persona.Examples),
		lessonSection,
		"## Self-Improvement Loop\n" + strings.Join(selfImprovementLines, "\n"),
	}
	return strings.Join(parts, "\n\n") + "\n"
}

func defaultLessonsDocument() string {
	return "# Lessons Learned\n\n- (none yet)\n"
}

func buildTaskContextDocument(
	task apiTask,
	checkpoints []apiTaskCheckpoint,
) string {
	summarySection := "### Recent Task Checkpoints\n- (none yet)"
	if len(checkpoints) > 0 {
		lines := make([]string, 0, len(checkpoints))
		for _, checkpoint := range checkpoints {
			text := cleanPromptContextText(checkpoint.CheckpointText)
			if text == "" {
				continue
			}
			checkpointType := strings.TrimSpace(checkpoint.CheckpointType)
			if checkpointType == "" {
				checkpointType = "STEP"
			}
			lines = append(lines, fmt.Sprintf("- [%s] %s", checkpointType, text))
		}
		if len(lines) > 0 {
			summarySection = "### Recent Task Checkpoints\n" + strings.Join(lines, "\n")
		}
	}

	parts := []string{
		"# Task Brief",
		"### Task Name",
		valueOrPlaceholder(task.Name),
		"",
		"### Task Description",
		valueOrPlaceholder(task.Description),
		"",
		"# Task Continuation Context",
		summarySection,
		"",
		"# Lifecycle Signals",
		"When you complete the task, output EXACTLY this line on its own:",
		"[TASK_COMPLETED] <one-paragraph summary of what you did>",
		"",
		"When you need the user's input to proceed, output EXACTLY this line on its own:",
		"[NEEDS_USER_INPUT] <your question for the user>",
		"",
		"Start work now. Keep responses concise and actionable.",
	}
	return strings.Join(parts, "\n\n") + "\n"
}

func writeTaskWorkspaceFiles(
	workingDir string,
	persona apiAgentPersona,
	task apiTask,
	checkpoints []apiTaskCheckpoint,
	lessonsContent string,
) (string, string, string, error) {
	workingDir = strings.TrimSpace(workingDir)
	if workingDir == "" {
		return "", "", "", fmt.Errorf("missing working directory")
	}

	lessonsContent = strings.TrimSpace(lessonsContent)
	if lessonsContent == "" {
		lessonsContent = strings.TrimSpace(defaultLessonsDocument())
	}
	lessonsContent += "\n"

	lessonsPath := filepath.Join(workingDir, "lessons.md")
	if err := writeFileAtomic(lessonsPath, []byte(lessonsContent), 0o600, 0o700); err != nil {
		return "", "", "", err
	}

	personaPath := filepath.Join(workingDir, "AGENT_PERSONA.md")
	if err := writeFileAtomic(
		personaPath,
		[]byte(buildPersonaDocument(persona, lessonsPath)),
		0o600,
		0o700,
	); err != nil {
		return "", "", "", err
	}

	taskContextPath := filepath.Join(workingDir, "TASK_CONTEXT.md")
	if err := writeFileAtomic(
		taskContextPath,
		[]byte(buildTaskContextDocument(task, checkpoints)),
		0o600,
		0o700,
	); err != nil {
		return "", "", "", err
	}

	return personaPath, lessonsPath, taskContextPath, nil
}

func ensureLocalInstanceFiles(stateFilePath, instanceID string) (string, string, string, error) {
	baseDir, logPath, transcriptPath, cmdPath := localInstanceFilePaths(stateFilePath, instanceID)
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(logPath, []byte{}, 0o600); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(transcriptPath, []byte{}, 0o600); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(cmdPath, []byte{}, 0o600); err != nil {
		return "", "", "", err
	}
	return logPath, transcriptPath, cmdPath, nil
}

func localInstanceFilePaths(stateFilePath, instanceID string) (baseDir, logPath, transcriptPath, cmdPath string) {
	baseDir = filepath.Join(filepath.Dir(stateFilePath), "instances", strings.TrimSpace(instanceID))
	logPath = filepath.Join(baseDir, "logs.raw")
	transcriptPath = filepath.Join(baseDir, "transcript.ndjson")
	cmdPath = filepath.Join(baseDir, "commands.txt")
	return baseDir, logPath, transcriptPath, cmdPath
}

func (m *manager) startWorkerProcess(
	ctx context.Context,
	instanceID, taskID, agentID, sessionID, workingDir, runtimeCommand, launchCommand string,
) (*worker, error) {
	cmd := newRuntimeCommand(launchCommand)
	cmd.Dir = workingDir
	var workerInput io.WriteCloser
	var workerOutput io.ReadCloser

	width, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		width, height = 80, 24
	}
	preferredSize, hasPreferredSize := m.preferredBrowserSize(instanceID)
	if hasPreferredSize && preferredSize.cols > 0 && preferredSize.rows > 0 {
		width = preferredSize.cols
		height = preferredSize.rows
	}
	width, height, _ = clampPTYDimensions(width, height)

	ws := &pty.Winsize{
		Rows: uint16(height),
		Cols: uint16(width),
	}

	var ptmx *os.File
	cmd, ptmx, err = m.startPTYProcess(instanceID, runtimeCommand, launchCommand, workingDir, ws)
	if err == nil {
		workerInput = ptmx
		workerOutput = ptmx
	} else {
		if requiresPTYRuntime(runtimeCommand) {
			return nil, fmt.Errorf("pty required for %s but unavailable: %w", runtimeDisplayName(runtimeCommand), err)
		}
		cmd = newRuntimeCommand(launchCommand)
		cmd.Dir = workingDir
		stdinPipe, stdinErr := cmd.StdinPipe()
		if stdinErr != nil {
			return nil, stdinErr
		}
		reader, writer, pipeErr := os.Pipe()
		if pipeErr != nil {
			_ = stdinPipe.Close()
			return nil, pipeErr
		}
		cmd.Stdout = writer
		cmd.Stderr = writer
		startErr := cmdStart(cmd)
		_ = writer.Close()
		if startErr != nil {
			_ = stdinPipe.Close()
			_ = reader.Close()
			return nil, startErr
		}
		workerInput = stdinPipe
		workerOutput = reader
	}

	localLogPath, localTranscriptPath, localCommandPath, err := ensureLocalInstanceFiles(m.cfg.StateFile, instanceID)
	if err != nil {
		_ = workerInput.Close()
		_ = workerOutput.Close()
		if cmd.Process != nil {
			_, _ = cmd.Process.Wait()
		}
		return nil, err
	}

	w := &worker{
		instanceID:              instanceID,
		taskID:                  taskID,
		agentID:                 agentID,
		sessionID:               sessionID,
		workingDir:              workingDir,
		runtimeCommand:          runtimeCommand,
		startedAt:               time.Now(),
		cmd:                     cmd,
		input:                   workerInput,
		output:                  workerOutput,
		logs:                    make(chan logEntry, 1024),
		stopBatch:               make(chan struct{}),
		outputBuffer:            &outputRingBuffer{maxSize: 4096},
		terminalState:           newVTScreenState(width, height),
		terminalReplayBuffer:    &terminalReplayBuffer{maxBytes: maxTerminalReplayBytes},
		terminalCols:            width,
		terminalRows:            height,
		terminalSizeLocked:      hasPreferredSize,
		localLogFilePath:        localLogPath,
		localTranscriptFilePath: localTranscriptPath,
		localCommandFilePath:    localCommandPath,
		usesPTY:                 ptmx != nil,
		assistantStop:           make(chan struct{}),
		attachClients:           make(map[net.Conn]struct{}),
		deliveredCommandIDs:     make(map[string]struct{}),
		done:                    make(chan struct{}),
	}
	if w.terminalReplayBuffer != nil {
		w.terminalReplayEventSeq++
		w.terminalReplayBuffer.AppendResize(w.terminalReplayEventSeq, width, height)
	}
	go m.readOutputLoop(ctx, w)
	go m.monitorWorkerExit(w)
	return w, nil
}

func (m *manager) startPTYProcess(instanceID, runtimeCommand, launchCommand, workingDir string, ws *pty.Winsize) (*exec.Cmd, *os.File, error) {
	cmd := newRuntimeCommand(launchCommand)
	cmd.Dir = workingDir
	ptmx, err := ptyStartWithSize(cmd, ws)
	if err == nil {
		m.logPTYStartSuccess(instanceID, runtimeCommand, workingDir, 1)
		return cmd, ptmx, nil
	}
	m.logPTYStartFailure(instanceID, runtimeCommand, workingDir, 1, err)
	if !shouldRetryPTYWithoutSetctty(err) {
		return nil, nil, err
	}
	m.logPTYRetryWithoutSetctty(instanceID, runtimeCommand, workingDir, err)

	cmd = newRuntimeCommand(launchCommand)
	cmd.Dir = workingDir
	ptmx, tty, err := ptyOpen()
	if err != nil {
		m.logPTYStartFailure(instanceID, runtimeCommand, workingDir, 2, err)
		return nil, nil, err
	}
	success := false
	defer func() {
		if success {
			return
		}
		_ = tty.Close()
		_ = ptmx.Close()
	}()
	if err := ptySetSize(ptmx, ws); err != nil {
		m.logPTYStartFailure(instanceID, runtimeCommand, workingDir, 2, err)
		return nil, nil, err
	}
	cmd.Stdin = tty
	cmd.Stdout = tty
	cmd.Stderr = tty
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setsid = true
	cmd.SysProcAttr.Setctty = false
	cmd.SysProcAttr.Ctty = 0
	cmd.SysProcAttr.Setpgid = false
	if err := cmdStart(cmd); err != nil {
		m.logPTYStartFailure(instanceID, runtimeCommand, workingDir, 2, err)
		return nil, nil, err
	}
	_ = tty.Close()
	success = true
	m.logPTYStartSuccess(instanceID, runtimeCommand, workingDir, 2)
	return cmd, ptmx, nil
}

func waitForExitedOpenCodeResumeCommand(
	ctx context.Context,
	w *worker,
	timeout time.Duration,
) (string, bool) {
	if w == nil || !isOpenCodeRuntimeCommand(w.runtimeCommand) {
		return "", false
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	current := strings.Join(strings.Fields(strings.TrimSpace(w.runtimeCommand)), " ")
	exited := false
	var hintedCommand string
	var hintedAt time.Time

	for {
		if command, ok := extractOpenCodeResumeCommand(workerReadySnapshot(w)); ok && !strings.EqualFold(command, current) {
			if exited {
				return command, true
			}
			if command != hintedCommand {
				hintedCommand = command
				hintedAt = time.Now()
			} else if !hintedAt.IsZero() && time.Since(hintedAt) >= openCodeResumeForceKill {
				if stopWorkerForOpenCodeResume(w, openCodeResumeStopWait) {
					return command, true
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return "", false
		case <-w.done:
			exited = true
		case <-ticker.C:
		}
	}
}

func stopWorkerForOpenCodeResume(w *worker, timeout time.Duration) bool {
	if w == nil || w.cmd == nil || w.cmd.Process == nil {
		return false
	}
	_ = signalManagedPID(w.cmd.Process.Pid, syscall.SIGTERM)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-w.done:
		return true
	case <-timer.C:
	}
	if err := killManagedPID(w.cmd.Process.Pid); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return false
	}
	select {
	case <-w.done:
		return true
	case <-time.After(300 * time.Millisecond):
		return false
	}
}

func openCodeResumeWaitTimeout(w *worker) time.Duration {
	if w == nil {
		return openCodeResumeWait
	}
	if _, ok := extractOpenCodeResumeCommand(workerReadySnapshot(w)); ok {
		return openCodeResumeHintWait
	}
	return openCodeResumeWait
}

func (m *manager) attachWorkerRuntime(w *worker) string {
	if w == nil {
		return ""
	}
	if ptmx, ok := w.input.(*os.File); ok && ptmx != nil {
		if !w.terminalSizeLocked {
			_ = SyncInitialPTYSize(ptmx, os.Stdin)
		}
		return m.startAttachServer(w, ptmx)
	}
	return ""
}

func (m *manager) trackWorker(ctx context.Context, w *worker, attachAddr string) {
	if w == nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	status := workerAgentStatusForWorker(w)
	if status == "" {
		status = "working"
	}
	lastWorkingAt := ""
	lastIdleAt := ""
	if status == "idle" {
		lastIdleAt = now
	} else {
		lastWorkingAt = now
	}
	m.mu.Lock()
	m.workers[w.instanceID] = w
	m.state.Instances[w.instanceID] = persistedWorker{
		PID:                 w.cmd.Process.Pid,
		TaskID:              w.taskID,
		AgentID:             w.agentID,
		SessionID:           w.sessionID,
		Status:              status,
		LastWorkingAt:       lastWorkingAt,
		LastIdleAt:          lastIdleAt,
		WorkingDir:          w.workingDir,
		RuntimeCommand:      w.runtimeCommand,
		LocalLogFile:        w.localLogFilePath,
		LocalTranscriptFile: w.localTranscriptFilePath,
		LocalCommandFile:    w.localCommandFilePath,
		AttachAddr:          attachAddr,
	}
	_ = writeState(m.cfg.StateFile, m.state)
	m.mu.Unlock()
	w.tracked.Store(true)
	go m.batchLogsLoop(context.Background(), w)
	go m.listenCommandsLoop(ctx, w)
	go m.listenLocalCommandsLoop(ctx, w)
	go m.assistantSemanticLoop(w)
	m.startIdleTimeoutLoop(w)
}

func (m *manager) fetchTaskCheckpoints(ctx context.Context, taskID string, limit int) ([]apiTaskCheckpoint, error) {
	if strings.TrimSpace(m.managerSubdomain) == "" {
		return nil, fmt.Errorf("fetchTaskCheckpoints: no manager subdomain registered")
	}
	if limit <= 0 {
		limit = 10
	}
	var out []apiTaskCheckpoint
	path := fmt.Sprintf("/managers/tasks/%s/checkpoints?limit=%d", taskID, limit)
	if err := m.managerRequestJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (m *manager) registerAgentInstance(ctx context.Context, persona apiAgentPersona, targetSlots int, preferredInstanceID string) (string, string, error) {
	if targetSlots <= 0 {
		targetSlots = 1
	}
	payload := map[string]any{
		"managerId":           m.state.ManagerID,
		"agentId":             persona.ID,
		"runtimeId":           persona.RuntimeID,
		"executionMode":       m.cfg.ExecutionMode,
		"targetSlots":         targetSlots,
		"preferredInstanceId": strings.TrimSpace(preferredInstanceID),
		"occupiedInstanceIds": m.liveInstanceIDsForAgent(persona.ID),
	}
	var out struct {
		ID               string `json:"id"`
		ManagerSubdomain string `json:"manager_subdomain"`
	}
	err := m.managerRequestJSON(ctx, http.MethodPost, "/agent-instances/register", payload, &out)
	return out.ID, out.ManagerSubdomain, err
}

func (m *manager) liveInstanceIDsForAgent(agentID string) []string {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.workers))
	for instanceID, worker := range m.workers {
		if worker == nil || strings.TrimSpace(worker.agentID) != agentID {
			continue
		}
		ids = append(ids, strings.TrimSpace(instanceID))
	}
	sort.Strings(ids)
	return ids
}

func (m *manager) readOutputLoop(ctx context.Context, w *worker) {
	reader := bufio.NewReader(w.output)
	buf := make([]byte, 1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			chunk := string(buf[:n])
			chunkBytes := append([]byte(nil), buf[:n]...)
			w.terminalStateMu.Lock()
			if w.terminalState != nil {
				w.terminalState.Write(chunk)
			}
			w.terminalOutputSeq++
			sequence := w.terminalOutputSeq
			if w.terminalReplayBuffer != nil {
				w.terminalReplayEventSeq++
				w.terminalReplayBuffer.AppendOutput(w.terminalReplayEventSeq, sequence, chunkBytes)
			}
			w.terminalStateMu.Unlock()
			w.logs <- logEntry{
				Line:      chunk,
				LogType:   "stdout",
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			}
			_ = appendRawLocalLogChunk(w.localLogFilePath, chunkBytes)
			m.broadcastAttachOutput(w, chunkBytes)
			w.outputBuffer.Write(chunk)
			m.captureAssistantFragment(w, chunk)
			m.persistWorkerActivityStatus(w)
			m.broadcastBrowserTerminalOutput(w.instanceID, sequence, chunkBytes)
			m.scanLifecycleMarkers(ctx, w, chunk)
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) {
			w.logs <- logEntry{
				Line:      fmt.Sprintf("read_output_error: %v", err),
				LogType:   "error",
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			}
		}
		return
	}
}

func (m *manager) startAttachServer(w *worker, ptmx *os.File) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		m.logError("attach_listen_error instance_id=%s err=%v", w.instanceID, err)
		return ""
	}
	w.attachListener = ln
	go m.acceptAttachClients(w, ptmx)
	return ln.Addr().String()
}

func (m *manager) acceptAttachClients(w *worker, ptmx *os.File) {
	for {
		conn, err := w.attachListener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				m.logError("attach_accept_error instance_id=%s err=%v", w.instanceID, err)
			}
			return
		}
		w.attachMu.Lock()
		w.attachClients[conn] = struct{}{}
		w.attachMu.Unlock()
		go m.handleAttachClientInput(w, conn)
	}
}

func (m *manager) handleAttachClientInput(w *worker, conn net.Conn) {
	defer func() {
		_ = conn.Close()
		w.attachMu.Lock()
		delete(w.attachClients, conn)
		w.attachMu.Unlock()
	}()

	reader := bufio.NewReader(conn)
	if closed := applyAttachSizeHandshakeForPTY(conn, reader, func(cols, rows int) {
		m.setWorkerTerminalSize(w, cols, rows, true, true)
	}); closed {
		return
	}

	_, _ = io.Copy(w.input, reader)
}

func applyAttachSizeHandshakeForPTY(conn net.Conn, bufferedConn *bufio.Reader, applySize func(cols, rows int)) bool {
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	line, err := bufferedConn.ReadString('\n')
	if err != nil {
		return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
	}
	line = strings.TrimSpace(line)
	parts := strings.Split(line, " ")
	if len(parts) != 3 || parts[0] != attachHandshakePrefix {
		return false
	}
	cols, err := strconv.Atoi(parts[1])
	if err != nil || cols <= 0 {
		return false
	}
	rows, err := strconv.Atoi(parts[2])
	if err != nil || rows <= 0 {
		return false
	}
	if applySize != nil {
		applySize(cols, rows)
	}
	return false
}

func (m *manager) broadcastAttachOutput(w *worker, chunk []byte) {
	w.attachMu.Lock()
	conns := make([]net.Conn, 0, len(w.attachClients))
	for conn := range w.attachClients {
		conns = append(conns, conn)
	}
	w.attachMu.Unlock()

	for _, conn := range conns {
		_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		if _, err := conn.Write(chunk); err != nil {
			_ = conn.Close()
			w.attachMu.Lock()
			delete(w.attachClients, conn)
			w.attachMu.Unlock()
		}
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

func (m *manager) broadcastBrowserTerminalOutput(instanceID string, sequence uint64, chunk []byte) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || len(chunk) == 0 {
		return
	}
	if worker := m.workerForInstanceID(instanceID); !workerTerminalVisibleToBrowser(worker) {
		return
	}
	dataBase64 := base64.StdEncoding.EncodeToString(chunk)
	m.browserMu.RLock()
	clients := make([]*browserClient, 0, len(m.browserClients))
	for client := range m.browserClients {
		client.subscriptionMu.RLock()
		_, ok := client.subscribedInstance[instanceID]
		client.subscriptionMu.RUnlock()
		if ok {
			clients = append(clients, client)
		}
	}
	m.browserMu.RUnlock()

	for _, client := range clients {
		if !m.enqueueBrowserEvent(client, map[string]any{
			"type":        "terminal_output",
			"instance_id": instanceID,
			"sequence":    sequence,
			"data_base64": dataBase64,
		}) {
			continue
		}
	}
}

func (m *manager) workerForInstanceID(instanceID string) *worker {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.workers[instanceID]
}

func (m *manager) lockInstanceSpawn(instanceID string) func() {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return func() {}
	}
	m.mu.Lock()
	lock := m.instanceSpawnLocks[instanceID]
	if lock == nil {
		lock = &sync.Mutex{}
		m.instanceSpawnLocks[instanceID] = lock
	}
	m.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func (m *manager) captureAssistantFragment(w *worker, chunk string) {
	fragment := extractAssistantText(chunk)
	if fragment == "" {
		return
	}
	w.assistantMu.Lock()
	defer w.assistantMu.Unlock()
	w.assistantBuf.WriteString(fragment)
	if !strings.HasSuffix(fragment, "\n") {
		w.assistantBuf.WriteString("\n")
	}
	w.assistantLastFragmentAt = time.Now()
}

func (m *manager) assistantSemanticLoop(w *worker) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-w.assistantStop:
			m.flushAssistantMessage(w)
			return
		case <-ticker.C:
			w.assistantMu.Lock()
			hasPending := w.assistantBuf.Len() > 0
			lastAt := w.assistantLastFragmentAt
			w.assistantMu.Unlock()
			if !hasPending {
				continue
			}
			if !lastAt.IsZero() && time.Since(lastAt) < 900*time.Millisecond {
				continue
			}
			m.flushAssistantMessage(w)
		}
	}
}

func (m *manager) flushAssistantMessage(w *worker) {
	w.assistantMu.Lock()
	raw := strings.TrimSpace(w.assistantBuf.String())
	w.assistantBuf.Reset()
	w.assistantLastFragmentAt = time.Time{}
	w.assistantMu.Unlock()
	if raw == "" {
		return
	}
	w.logs <- logEntry{
		Line:      raw,
		LogType:   "assistant",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func extractAssistantText(chunk string) string {
	clean := sanitizeTerminalFragment(chunk)
	if strings.TrimSpace(clean) == "" {
		return ""
	}
	lines := strings.Split(clean, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		switch {
		case strings.Contains(lower, "openai codex"),
			strings.Contains(lower, "model:"),
			strings.Contains(lower, "directory:"),
			strings.Contains(lower, "for shortcuts"),
			strings.Contains(lower, "context left"),
			strings.Contains(lower, "gpt-5.3-codex medium"),
			strings.Contains(lower, "tip:"),
			strings.Contains(lower, "build together."),
			strings.Contains(lower, "task_completed"),
			strings.Contains(lower, "needs_user_input"):
			continue
		}
		if strings.HasPrefix(line, "╭") || strings.HasPrefix(line, "│") || strings.HasPrefix(line, "╰") {
			continue
		}
		if strings.Contains(line, "[>") || strings.Contains(line, "MMMM") || strings.Contains(line, "[39") {
			continue
		}
		trimmedPrompt := strings.TrimSpace(strings.TrimPrefix(line, "›"))
		if trimmedPrompt != strings.TrimSpace(line) && trimmedPrompt != "" {
			// Drop prompt-box lines that reflect typed input, not assistant responses.
			continue
		}
		if shouldDropAssistantLine(line) {
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func shouldDropAssistantLine(line string) bool {
	letters := 0
	spaces := 0
	for _, r := range line {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			letters++
		}
		if r == ' ' || r == '\t' {
			spaces++
		}
	}
	if letters < 3 {
		return true
	}
	if spaces == 0 && len(line) > 24 {
		return true
	}
	return false
}

func (m *manager) scanLifecycleMarkers(ctx context.Context, w *worker, chunk string) {
	m.flushPendingTaskCompleted(ctx, w, false)

	recent := w.outputBuffer.Last(2048)
	if recent == "" {
		recent = chunk
	}
	event, ok := extractLatestLifecycleEvent(recent)
	if !ok {
		return
	}
	switch event.status {
	case "WAITING_FOR_USER_INPUT":
		if w.sawNeedsUserInput {
			return
		}
		w.pendingTaskCompleted = ""
		w.pendingTaskCompletedAt = time.Time{}
		payload := map[string]string{
			"status":  event.status,
			"comment": event.comment,
		}
		if err := m.managerRequestJSON(
			ctx,
			http.MethodPost,
			"/agent-instances/"+w.instanceID+"/update-task",
			payload,
			nil,
		); err != nil {
			m.logError("lifecycle_marker_update_error instance_id=%s marker=%s err=%v", w.instanceID, event.marker, err)
			return
		}
		w.sawNeedsUserInput = true
		m.logWorkerStateMilestone(w, "needs_user_input")
		m.logInfo("lifecycle_marker instance_id=%s marker=%s", w.instanceID, event.marker)
	case "READY_FOR_REVIEW":
		if w.sawTaskCompleted || w.sawNeedsUserInput {
			return
		}
		if w.pendingTaskCompleted == event.comment {
			return
		}
		w.pendingTaskCompleted = event.comment
		w.pendingTaskCompletedAt = time.Now()
	}
}

func (m *manager) flushPendingTaskCompleted(
	ctx context.Context,
	w *worker,
	force bool,
) {
	if w.sawTaskCompleted || w.sawNeedsUserInput {
		return
	}
	if strings.TrimSpace(w.pendingTaskCompleted) == "" || w.pendingTaskCompletedAt.IsZero() {
		return
	}
	if !force && time.Since(w.pendingTaskCompletedAt) < taskCompletedGrace {
		return
	}
	payload := map[string]string{
		"status":  "READY_FOR_REVIEW",
		"comment": w.pendingTaskCompleted,
	}
	if err := m.managerRequestJSON(
		ctx,
		http.MethodPost,
		"/agent-instances/"+w.instanceID+"/update-task",
		payload,
		nil,
	); err != nil {
		m.logError("lifecycle_marker_update_error instance_id=%s marker=TASK_COMPLETED err=%v", w.instanceID, err)
		if force {
			w.pendingTaskCompleted = ""
			w.pendingTaskCompletedAt = time.Time{}
		}
		return
	}
	w.sawTaskCompleted = true
	w.pendingTaskCompleted = ""
	w.pendingTaskCompletedAt = time.Time{}
	m.syncLessonsForAgent(w.agentID)
	m.logInfo("lifecycle_marker instance_id=%s marker=TASK_COMPLETED", w.instanceID)
}

func (m *manager) batchLogsLoop(ctx context.Context, w *worker) {
	ticker := time.NewTicker(m.cfg.LogFlushInterval)
	defer ticker.Stop()
	batch := make([]logEntry, 0, m.cfg.LogFlushBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		payload := map[string]any{"agent_instance_id": w.instanceID, "session_id": w.sessionID, "logs": batch}
		_ = m.managerRequestJSON(ctx, http.MethodPost, "/api/logs/batch", payload, nil)
		batch = batch[:0]
	}
	for {
		select {
		case entry := <-w.logs:
			if entry.LogType == "user" || entry.LogType == "assistant" {
				_ = appendTranscriptLocalLogEntry(w.localTranscriptFilePath, entry)
			}
			batch = append(batch, entry)
			if len(batch) >= m.cfg.LogFlushBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-w.stopBatch:
			flush()
			return
		}
	}
}

func (m *manager) listenCommandsLoop(ctx context.Context, w *worker) {
	if err := m.ensureSupabaseBootstrap(ctx); err != nil {
		m.logError("realtime_bootstrap_error instance_id=%s err=%v", w.instanceID, err)
		return
	}

	db, err := sql.Open("postgres", m.cfg.DatabaseURL)
	hasDB := err == nil && m.cfg.DatabaseURL != ""
	if hasDB {
		defer db.Close()
	}

	deliverPending := func() {
		if !hasDB {
			return
		}
		if err := m.drainPendingCommands(ctx, w, db); err != nil {
			m.logError("pending_commands_drain_error instance_id=%s err=%v", w.instanceID, err)
		}
	}
	deliverPending()

	reconnectDelay := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rc, err := NewRealtimeClient(
			m.cfg.SupabaseURL,
			m.cfg.SupabaseAnonKey,
			func() (string, error) {
				return m.getValidUserAccessToken(ctx)
			},
		)
		if err != nil {
			if refreshErr := m.refreshRealtimeHandshakeState(ctx, err); refreshErr != nil {
				m.logError("realtime_handshake_refresh_error instance_id=%s err=%v", w.instanceID, refreshErr)
			}
			m.logError("realtime_connect_error instance_id=%s err=%v", w.instanceID, err)
			deliverPending()
			if !sleepWithContext(ctx, reconnectDelay) {
				return
			}
			reconnectDelay = nextRealtimeReconnectDelay(reconnectDelay)
			continue
		}

		topic := "realtime:public:agent_commands"
		filter := "agent_instance_id=eq." + w.instanceID
		ch, err := rc.Subscribe(topic, filter)
		if err != nil {
			rc.Close()
			m.logError("realtime_subscribe_error instance_id=%s err=%v", w.instanceID, err)
			deliverPending()
			if !sleepWithContext(ctx, reconnectDelay) {
				return
			}
			reconnectDelay = nextRealtimeReconnectDelay(reconnectDelay)
			continue
		}

		reconnectDelay = time.Second
		recoveryTicker := time.NewTicker(3 * time.Second)
		shouldReconnect := false
		for {
			select {
			case <-ctx.Done():
				recoveryTicker.Stop()
				rc.Close()
				return
			case <-recoveryTicker.C:
				deliverPending()
			case payload, ok := <-ch:
				if !ok {
					recoveryTicker.Stop()
					rc.Close()
					m.logError("realtime_subscription_closed instance_id=%s", w.instanceID)
					shouldReconnect = true
					break
				}
				if code, message, attempts, hasRealtimeError := parseRealtimeErrorPayload(payload); hasRealtimeError {
					recoveryTicker.Stop()
					rc.Close()
					m.logError(
						"realtime_failed instance_id=%s code=%s attempts=%d err=%s",
						w.instanceID,
						code,
						attempts,
						message,
					)
					shouldReconnect = true
					break
				}
				record, ok := payload["record"].(map[string]any)
				if !ok {
					continue
				}

				cmdStr, _ := record["command"].(string)
				cmdID, _ := record["id"].(string)
				executed, _ := record["executed"].(bool)

				if executed || cmdStr == "" {
					continue
				}
				if w.hasDeliveredCommand(cmdID) {
					_ = m.markCommandExecuted(ctx, w.instanceID, cmdID)
					continue
				}

				if err := m.writeAgentInputLine(w, cmdStr); err == nil {
					w.markCommandDelivered(cmdID)
					_ = m.markCommandExecuted(ctx, w.instanceID, cmdID)
				}
			}
			if shouldReconnect {
				break
			}
		}
		if !shouldReconnect {
			continue
		}
		deliverPending()
		if !sleepWithContext(ctx, reconnectDelay) {
			return
		}
		reconnectDelay = nextRealtimeReconnectDelay(reconnectDelay)
	}
}

func (m *manager) drainPendingCommands(ctx context.Context, w *worker, db *sql.DB) error {
	if db == nil || w == nil {
		return nil
	}
	rows, err := db.QueryContext(
		ctx,
		`select id, command from agent_commands where agent_instance_id = $1 and executed = false and status in ('PENDING', 'RETRYABLE') order by created_at asc limit 50`,
		w.instanceID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var commandID string
		var commandText string
		if scanErr := rows.Scan(&commandID, &commandText); scanErr != nil {
			return scanErr
		}
		if w.hasDeliveredCommand(commandID) {
			if err := m.markCommandExecuted(ctx, w.instanceID, commandID); err != nil {
				m.logError("command_execute_mark_failed instance_id=%s command_id=%s err=%v", w.instanceID, strings.TrimSpace(commandID), err)
			}
			continue
		}
		if err := m.writeAgentInputLine(w, commandText); err != nil {
			return err
		}
		w.markCommandDelivered(commandID)
		if err := m.markCommandExecuted(ctx, w.instanceID, commandID); err != nil {
			m.logError("command_execute_mark_failed instance_id=%s command_id=%s err=%v", w.instanceID, strings.TrimSpace(commandID), err)
		}
	}
	return rows.Err()
}

func (m *manager) listenLocalCommandsLoop(ctx context.Context, w *worker) {
	path := strings.TrimSpace(w.localCommandFilePath)
	if path == "" {
		return
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var offset int64
	var carry string

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data, nextOffset, err := readNewFileBytes(path, offset)
			if err != nil {
				m.logError("local_command_read_error instance_id=%s err=%v", w.instanceID, err)
				continue
			}
			offset = nextOffset
			if len(data) == 0 {
				continue
			}

			blob := carry + string(data)
			parts := strings.Split(blob, "\n")
			carry = parts[len(parts)-1]
			for _, raw := range parts[:len(parts)-1] {
				line := strings.TrimSpace(raw)
				if line == "" {
					continue
				}
				if err := m.writeAgentInputLine(w, line); err != nil {
					m.logError("local_command_write_error instance_id=%s err=%v", w.instanceID, err)
				}
			}
		}
	}
}

func (m *manager) writeAgentInputWithOptions(w *worker, line string, logAsUser, clearDraft bool) error {
	line = strings.TrimRight(line, "\r\n")
	if w.usesPTY {
		if clearDraft {
			if _, err := io.WriteString(w.input, "\x15"); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w.input, line); err != nil {
			return err
		}
		time.Sleep(150 * time.Millisecond)
		if err := m.submitAgentInput(w); err != nil {
			return err
		}
	} else {
		if _, err := io.WriteString(w.input, line+"\n"); err != nil {
			return err
		}
	}
	w.resetIdleTimer()
	if logAsUser {
		w.logs <- logEntry{
			Line:      line,
			LogType:   "user",
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}
	return nil
}

func (m *manager) writeAgentInput(w *worker, line string, logAsUser bool) error {
	return m.writeAgentInputWithOptions(w, line, logAsUser, false)
}

func (m *manager) submitAgentInput(w *worker) error {
	if w.usesPTY {
		if _, err := io.WriteString(w.input, "\r"); err != nil {
			return err
		}
		return nil
	}
	if _, err := io.WriteString(w.input, "\n"); err != nil {
		return err
	}
	return nil
}

func (m *manager) writeAgentInputLine(w *worker, line string) error {
	return m.writeAgentInput(w, line, true)
}

func (m *manager) writeAutomatedPrompt(w *worker, line string) error {
	return m.writeAgentInputWithOptions(w, line, false, true)
}

func (m *manager) writeSpawnBootstrapPrompt(ctx context.Context, w *worker, prompt string) error {
	if err := m.writeAutomatedPrompt(w, prompt); err != nil {
		return err
	}
	w.markBootstrapPromptPending(prompt)
	m.logWorkerStateMilestone(w, "bootstrap_prompt_sent")
	submitCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	if !awaitWorkerPromptSubmitted(submitCtx, w) {
		w.clearBootstrapPromptPending()
		err := submitCtx.Err()
		if err == nil {
			err = fmt.Errorf("bootstrap prompt submission was not confirmed")
		}
		m.logWarn("bootstrap_prompt_submission_unconfirmed instance_id=%s err=%v", w.instanceID, err)
		return err
	}
	w.clearBootstrapPromptPending()
	m.logWorkerStateMilestone(w, "bootstrap_prompt_submitted")
	return nil
}

func awaitWorkerPromptSubmitted(ctx context.Context, w *worker) bool {
	if w == nil || !w.usesPTY {
		return false
	}
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	busySeen := false
	for {
		select {
		case <-ctx.Done():
			return false
		case <-w.done:
			return false
		case <-ticker.C:
			rawSnapshot, terminalSnapshot := workerPromptSnapshots(w)
			rawStatus := workerPromptStatusForRuntime(w.runtimeCommand, rawSnapshot)
			if rawStatus.State == workerPromptStateReadyWithDraft {
				continue
			}
			terminalStatus := workerPromptStatus{}
			if strings.TrimSpace(terminalSnapshot) != "" {
				terminalStatus = workerPromptStatusForRuntime(w.runtimeCommand, terminalSnapshot)
				if terminalStatus.State == workerPromptStateReadyWithDraft {
					continue
				}
			}
			if rawStatus.State == workerPromptStateBusy || terminalStatus.State == workerPromptStateBusy {
				busySeen = true
				return true
			}
			if (rawStatus.State == workerPromptStateReadyEmpty || terminalStatus.State == workerPromptStateReadyEmpty) && busySeen {
				return true
			}
		}
	}
}

func awaitWorkerPromptReadyEmpty(ctx context.Context, w *worker) bool {
	if w == nil || !w.usesPTY {
		return false
	}
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-w.done:
			return false
		case <-ticker.C:
			if workerPromptStatusForWorker(w).State == workerPromptStateReadyEmpty {
				return true
			}
		}
	}
}

func (m *manager) waitForAgentReady(ctx context.Context, w *worker, timeout time.Duration) error {
	if !w.usesPTY {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	visibleCount := 0
	for {
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-w.done:
			return fmt.Errorf("worker exited before visible TUI output")
		case <-ticker.C:
			snapshot := strings.TrimSpace(workerReadySnapshot(w))
			if snapshot == "" {
				visibleCount = 0
				continue
			}
			visibleCount++
			if visibleCount >= 3 {
				return nil
			}
		}
	}
}

func (m *manager) waitBeforeBootstrapPrompt(ctx context.Context, w *worker, delay time.Duration) error {
	if !w.usesPTY || delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return fmt.Errorf("worker exited before bootstrap delay elapsed")
	case <-timer.C:
		return nil
	}
}

func shutdownKillRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := shutdownKillRetryBaseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= shutdownKillRetryMaxDelay {
			return shutdownKillRetryMaxDelay + randomDurationUpTo(shutdownKillRetryBaseDelay)
		}
	}
	return delay + randomDurationUpTo(shutdownKillRetryBaseDelay)
}

func randomDurationUpTo(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(limit.Nanoseconds()+1))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

func (m *manager) killWorkerWithRetry(w *worker) error {
	if w == nil || w.cmd == nil || w.cmd.Process == nil {
		return nil
	}
	pid := w.cmd.Process.Pid
	var lastErr error
	for attempt := 1; attempt <= shutdownKillRetryAttempts; attempt++ {
		if err := killManagedPIDFunc(pid); err != nil && !errors.Is(err, os.ErrProcessDone) {
			lastErr = err
		} else if waitForProcessExit(pid, 200*time.Millisecond) {
			return nil
		} else {
			lastErr = fmt.Errorf("process %d still running after kill signal", pid)
		}
		if attempt == shutdownKillRetryAttempts {
			break
		}
		delay := shutdownKillRetryDelay(attempt)
		m.logWarn("graceful_shutdown_kill_retrying instance_id=%s attempt=%d delay=%s err=%v", w.instanceID, attempt, delay, lastErr)
		timer := time.NewTimer(delay)
		select {
		case <-w.done:
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("process %d still running after %d kill attempts", pid, shutdownKillRetryAttempts)
	}
	return lastErr
}

func workerReadySnapshot(w *worker) string {
	if w == nil {
		return ""
	}
	rawSnapshot := sanitizeTerminalFragment(w.outputBuffer.Last(4096))
	w.terminalStateMu.Lock()
	state := w.terminalState
	w.terminalStateMu.Unlock()
	if state != nil {
		if snapshot := strings.TrimSpace(sanitizeTerminalFragment(state.SnapshotANSI())); snapshot != "" {
			if isOpenCodeRuntimeCommand(w.runtimeCommand) {
				if _, ok := extractOpenCodeResumeCommand(rawSnapshot); ok {
					return rawSnapshot
				}
			}
			return snapshot
		}
	}
	return rawSnapshot
}

func workerPromptSnapshots(w *worker) (string, string) {
	if w == nil {
		return "", ""
	}
	rawSnapshot := sanitizeTerminalFragment(w.outputBuffer.Last(4096))
	w.terminalStateMu.Lock()
	state := w.terminalState
	w.terminalStateMu.Unlock()
	if state == nil {
		return rawSnapshot, ""
	}
	return rawSnapshot, strings.TrimSpace(sanitizeTerminalFragment(state.SnapshotANSI()))
}

func compactTerminalTail(snapshot string) string {
	snapshot = strings.TrimSpace(sanitizeTerminalFragment(snapshot))
	if snapshot == "" {
		return ""
	}
	lines := strings.Split(snapshot, "\n")
	if start := len(lines) - 5; start > 0 {
		lines = lines[start:]
	}
	tail := strings.Join(lines, " | ")
	if len(tail) > 300 {
		tail = "..." + tail[len(tail)-300:]
	}
	return tail
}

func workerPromptStatusForWorker(w *worker) workerPromptStatus {
	if w == nil {
		return workerPromptStatus{State: workerPromptStateBusy}
	}
	runtimeCommand := strings.TrimSpace(w.runtimeCommand)
	rawSnapshot, terminalSnapshot := workerPromptSnapshots(w)
	rawStatus := workerPromptStatusForRuntime(runtimeCommand, rawSnapshot)
	if isOpenCodeRuntimeCommand(runtimeCommand) {
		if _, ok := extractOpenCodeResumeCommand(rawSnapshot); ok {
			return rawStatus
		}
	}
	if terminalSnapshot == "" {
		return rawStatus
	}
	terminalStatus := workerPromptStatusForRuntime(runtimeCommand, terminalSnapshot)
	if terminalStatus.State != workerPromptStateBusy {
		return terminalStatus
	}
	if rawStatus.State != workerPromptStateBusy {
		return rawStatus
	}
	if terminalStatus.Snapshot != "" {
		return terminalStatus
	}
	return rawStatus
}

func extractOpenCodeResumeCommand(snapshot string) (string, bool) {
	matches := openCodeResumeRegex.FindStringSubmatch(sanitizeTerminalFragment(snapshot))
	if len(matches) < 2 {
		return "", false
	}
	command := strings.Join(strings.Fields(strings.TrimSpace(matches[1])), " ")
	if command == "" {
		return "", false
	}
	return command, true
}

func workerPromptStatusForRuntime(runtimeCommand, snapshot string) workerPromptStatus {
	snapshot = strings.TrimSpace(snapshot)
	status := workerPromptStatus{
		State:    workerPromptStateBusy,
		Snapshot: snapshot,
	}
	if snapshot == "" {
		return status
	}
	lower := strings.ToLower(snapshot)
	if isGeminiRuntimeCommand(runtimeCommand) || strings.Contains(lower, "gemini cli") {
		draft := normalizePromptDraftText(geminiPromptDraft(snapshot))
		status.Draft = draft
		if draft != "" {
			status.State = workerPromptStateReadyWithDraft
			return status
		}
		if geminiSnapshotLooksBlocked(snapshot) {
			return status
		}
		if !looksLikeGeminiReadySnapshot(snapshot) {
			return status
		}
		status.State = workerPromptStateReadyEmpty
		return status
	}
	if isCodexLikeRuntimeCommand(runtimeCommand) || strings.Contains(lower, "openai codex") || strings.Contains(lower, "codex") || strings.Contains(lower, "opencode") || strings.Contains(lower, "claude code") {
		draft := normalizePromptDraftText(codexPromptDraft(snapshot))
		status.Draft = draft
		if draft != "" {
			status.State = workerPromptStateReadyWithDraft
			return status
		}
		if looksLikeCodexReadySnapshot(snapshot) {
			status.State = workerPromptStateReadyEmpty
		}
		return status
	}
	if looksLikeAgentReadySnapshot(snapshot) {
		status.State = workerPromptStateReadyEmpty
	}
	return status
}

func looksLikeAgentReadySnapshot(snapshot string) bool {
	lower := strings.ToLower(strings.TrimSpace(snapshot))
	if lower == "" {
		return false
	}
	if strings.Contains(lower, "openai codex") || strings.Contains(lower, "codex") || strings.Contains(lower, "opencode") || strings.Contains(lower, "claude code") {
		return looksLikeCodexReadySnapshot(snapshot)
	}
	if strings.Contains(lower, "gemini cli") {
		if geminiPromptDraft(snapshot) != "" {
			return false
		}
		return looksLikeGeminiReadySnapshot(snapshot)
	}
	return false
}

func looksLikeGeminiReadySnapshot(snapshot string) bool {
	lower := strings.ToLower(strings.TrimSpace(snapshot))
	if lower == "" {
		return false
	}
	return strings.Contains(lower, "type your message") ||
		strings.Contains(lower, "@path") ||
		strings.Contains(snapshot, "›")
}

func looksLikeCodexReadySnapshot(snapshot string) bool {
	lower := strings.ToLower(strings.TrimSpace(snapshot))
	if lower == "" {
		return false
	}
	if looksLikeOpenCodeResumeChooser(snapshot) {
		return false
	}
	return strings.Contains(snapshot, "›") ||
		strings.Contains(lower, "for shortcuts") ||
		strings.Contains(lower, "context left")
}

func looksLikeOpenCodeResumeChooser(snapshot string) bool {
	lower := strings.ToLower(strings.TrimSpace(snapshot))
	if lower == "" {
		return false
	}
	_, ok := extractOpenCodeResumeCommand(snapshot)
	return ok
}

func geminiSnapshotLooksBlocked(snapshot string) bool {
	if geminiSnapshotNeedsDirectoryTrustDecision(snapshot) {
		return true
	}
	lower := strings.ToLower(strings.TrimSpace(snapshot))
	if lower == "" {
		return true
	}
	blockedMarkers := []string{
		"waiting for user confirmation",
		"allow execution?",
		"allow once",
		"allow always",
		"quick safety check",
		"security guide",
		"enter to confirm",
		"esc to cancel",
		"? shell ",
	}
	for _, marker := range blockedMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func geminiSnapshotNeedsUserDecision(snapshot string) bool {
	if geminiSnapshotNeedsDirectoryTrustDecision(snapshot) {
		return true
	}
	lower := strings.ToLower(strings.TrimSpace(snapshot))
	if lower == "" {
		return false
	}
	userDecisionMarkers := []string{
		"waiting for user confirmation",
		"allow execution?",
		"allow once",
		"allow always",
		"enter to confirm",
		"esc to cancel",
	}
	for _, marker := range userDecisionMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func geminiSnapshotNeedsDirectoryTrustDecision(snapshot string) bool {
	lower := strings.ToLower(strings.TrimSpace(snapshot))
	if lower == "" {
		return false
	}
	return strings.Contains(lower, "do you trust the contents of this directory") &&
		strings.Contains(lower, "press enter to continue")
}

func geminiPromptDraft(snapshot string) string {
	lines := strings.Split(sanitizeTerminalFragment(snapshot), "\n")
	draftLines := make([]string, 0, 4)
	candidateWrappedLines := make([]string, 0, 4)
	for idx := len(lines) - 1; idx >= 0; idx-- {
		line := strings.TrimSpace(lines[idx])
		if line == "" {
			if len(candidateWrappedLines) > 0 {
				break
			}
			continue
		}

		trimmedPrompt := strings.TrimSpace(strings.TrimLeft(line, "│ "))
		if strings.HasPrefix(trimmedPrompt, "›") || strings.HasPrefix(trimmedPrompt, ">") {
			rest := strings.TrimSpace(strings.TrimPrefix(trimmedPrompt, "›"))
			rest = strings.TrimSpace(strings.TrimPrefix(rest, ">"))
			if rest == "" {
				return ""
			}
			draftLines = append([]string{rest}, draftLines...)
			if len(candidateWrappedLines) > 0 {
				draftLines = append(draftLines, candidateWrappedLines...)
			}
			return strings.TrimSpace(strings.Join(draftLines, " "))
		}

		if strings.HasPrefix(line, "│") {
			rest := strings.TrimSpace(strings.Trim(line, "│ "))
			if rest != "" {
				candidateWrappedLines = append([]string{rest}, candidateWrappedLines...)
			}
			continue
		}

		candidateWrappedLines = append([]string{line}, candidateWrappedLines...)
		if len(candidateWrappedLines) > 6 {
			candidateWrappedLines = candidateWrappedLines[1:]
		}
	}
	return ""
}

func codexPromptDraft(snapshot string) string {
	lines := strings.Split(sanitizeTerminalFragment(snapshot), "\n")
	pastStatusBar := false
	linesPastStatusBar := 0
	for idx := len(lines) - 1; idx >= 0; idx-- {
		line := strings.TrimSpace(lines[idx])
		if line == "" {
			continue
		}
		trimmedPrompt := strings.TrimSpace(strings.TrimLeft(line, "│ "))
		if strings.HasPrefix(trimmedPrompt, "›") || strings.HasPrefix(trimmedPrompt, ">") {
			rest := strings.TrimSpace(strings.TrimPrefix(trimmedPrompt, "›"))
			rest = strings.TrimSpace(strings.TrimPrefix(rest, ">"))
			return rest
		}
		if strings.Contains(strings.ToLower(line), "esc to interrupt") ||
			strings.Contains(strings.ToLower(line), "for shortcuts") ||
			strings.Contains(strings.ToLower(line), "context left") {
			pastStatusBar = true
			continue
		}
		if pastStatusBar {
			linesPastStatusBar++
			if linesPastStatusBar > 5 {
				break
			}
		}
	}
	return ""
}

func deferredBootstrapReasonForWorker(w *worker) (string, bool) {
	if w == nil {
		return "", false
	}
	status := workerPromptStatusForWorker(w)
	if isOpenCodeRuntimeCommand(w.runtimeCommand) {
		if _, ok := extractOpenCodeResumeCommand(status.Snapshot); ok {
			return "OpenCode is waiting in the terminal. Use livestream to continue the session.", true
		}
	}
	return "", false
}

func parseRealtimeErrorPayload(payload map[string]any) (string, string, int, bool) {
	if payload == nil {
		return "", "", 0, false
	}
	kind, _ := payload["kind"].(string)
	if kind != "realtime_error" {
		return "", "", 0, false
	}
	details, _ := payload["error"].(map[string]any)
	code, _ := details["code"].(string)
	message, _ := details["message"].(string)
	attemptsValue, _ := details["attempts"].(int)
	if attemptsValue == 0 {
		if floatAttempts, ok := details["attempts"].(float64); ok {
			attemptsValue = int(floatAttempts)
		}
	}
	return code, message, attemptsValue, true
}

func nextRealtimeReconnectDelay(current time.Duration) time.Duration {
	if current <= 0 {
		return time.Second
	}
	next := current * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}

func isRealtimeHandshakeAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "status=401") ||
		strings.Contains(msg, "status=403") ||
		strings.Contains(msg, "bad handshake")
}

func (m *manager) refreshRealtimeHandshakeState(ctx context.Context, dialErr error) error {
	if !isRealtimeHandshakeAuthError(dialErr) {
		return nil
	}
	m.mu.Lock()
	m.supabaseBootstrapSynced = false
	m.mu.Unlock()
	if err := m.ensureSupabaseBootstrap(ctx); err != nil {
		return err
	}
	_, err := m.forceRefreshUserAccessToken(ctx)
	return err
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (m *manager) monitorWorkerExit(w *worker) {
	err := w.cmd.Wait()
	w.stopIdleTimer()
	m.flushPendingTaskCompleted(context.Background(), w, true)
	close(w.done)
	close(w.assistantStop)
	close(w.stopBatch)
	_ = w.output.Close()
	_ = w.input.Close()
	if w.attachListener != nil {
		_ = w.attachListener.Close()
	}
	w.attachMu.Lock()
	for conn := range w.attachClients {
		_ = conn.Close()
	}
	w.attachClients = map[net.Conn]struct{}{}
	w.attachMu.Unlock()
	if !w.tracked.Load() {
		return
	}

	status := "COMPLETED"
	if err != nil {
		status = "FAILED"
	}

	// Capture last output as summary.
	summary := w.outputBuffer.Last(2048)
	comment := "Agent process exited."
	if summary != "" {
		comment = "Agent process exited. Last output:\n" + summary
	}
	if err != nil {
		comment = fmt.Sprintf("Agent process failed: %v\nLast output:\n%s", err, summary)
	}

	if w.sawTaskCompleted || w.sawNeedsUserInput {
		m.mu.Lock()
		delete(m.workers, w.instanceID)
		delete(m.state.Instances, w.instanceID)
		delete(m.instanceSpawnLocks, w.instanceID)
		_ = writeState(m.cfg.StateFile, m.state)
		m.mu.Unlock()

		m.requestImmediatePoll()
		m.broadcastBothStates(w.instanceID, "offline", "offline", "Agent process exited.")
		m.logInfo("instance_exit instance_id=%s status=%s", w.instanceID, status)
		return
	}

	if shutdownRequested, _, shutdownMode := w.shutdownRequestDetails(); shutdownRequested {
		if shutdownMode == shutdownModeManagerRestart {
			persistedStatus := workerAgentStatusForWorker(w)
			if persistedStatus == "" {
				persistedStatus = "working"
			}
			now := time.Now().UTC().Format(time.RFC3339Nano)
			m.mu.Lock()
			delete(m.workers, w.instanceID)
			delete(m.instanceSpawnLocks, w.instanceID)
			persisted := m.state.Instances[w.instanceID]
			persisted.PID = 0
			if strings.TrimSpace(persisted.TaskID) == "" {
				persisted.TaskID = w.taskID
			}
			if strings.TrimSpace(persisted.AgentID) == "" {
				persisted.AgentID = w.agentID
			}
			if strings.TrimSpace(persisted.SessionID) == "" {
				persisted.SessionID = w.sessionID
			}
			persisted.Status = persistedStatus
			if persistedStatus == "idle" {
				persisted.LastIdleAt = now
			} else {
				persisted.LastWorkingAt = now
			}
			persisted.WorkingDir = w.workingDir
			persisted.RuntimeCommand = w.runtimeCommand
			persisted.AttachAddr = ""
			m.state.Instances[w.instanceID] = persisted
			_ = writeState(m.cfg.StateFile, m.state)
			m.mu.Unlock()

			m.broadcastBothStates(
				w.instanceID,
				"offline",
				"offline",
				"Manager stopped this session after persisting progress. It will be restarted when the manager comes back.",
			)
			m.logInfo("instance_exit instance_id=%s status=%s restartable=true", w.instanceID, status)
			return
		}
		comment = "Agent was shut down after being asked to persist progress to TASK_CONTEXT.md and lessons to lessons.md."
		if shutdownMode == shutdownModeIdleTimeout {
			comment = "Agent was shut down after being idle with no activity for 10 minutes. Progress persisted to TASK_CONTEXT.md."
		}
		if summary != "" {
			comment += "\nLast output:\n" + summary
		}
		payload := map[string]string{
			"comment": comment,
			"status":  "WAITING_FOR_USER_INPUT",
		}
		_ = m.managerRequestJSON(context.Background(), http.MethodPost, "/agent-instances/"+w.instanceID+"/update-task", payload, nil)
		m.syncLessonsForAgent(w.agentID)

		m.mu.Lock()
		delete(m.workers, w.instanceID)
		delete(m.state.Instances, w.instanceID)
		delete(m.instanceSpawnLocks, w.instanceID)
		_ = writeState(m.cfg.StateFile, m.state)
		m.mu.Unlock()

		m.requestImmediatePoll()
		m.broadcastBothStates(w.instanceID, "offline", "offline", "Agent shut down.")
		m.logInfo("instance_exit instance_id=%s status=%s", w.instanceID, status)
		return
	}

	statusUpdate := "READY_FOR_REVIEW"
	if err != nil {
		statusUpdate = "WAITING_FOR_USER_INPUT"
	}

	payload := map[string]string{
		"comment": comment,
		"status":  statusUpdate,
	}
	_ = m.managerRequestJSON(context.Background(), http.MethodPost, "/agent-instances/"+w.instanceID+"/update-task", payload, nil)
	if err == nil {
		m.syncLessonsForAgent(w.agentID)
	}

	m.mu.Lock()
	delete(m.workers, w.instanceID)
	delete(m.state.Instances, w.instanceID)
	delete(m.instanceSpawnLocks, w.instanceID)
	_ = writeState(m.cfg.StateFile, m.state)
	m.mu.Unlock()

	m.requestImmediatePoll()
	m.broadcastBothStates(w.instanceID, "offline", "offline", "Agent process exited.")
	m.logInfo("instance_exit instance_id=%s status=%s", w.instanceID, status)
}

func (m *manager) startIdleTimeoutLoop(w *worker) {
	w.idleTimerMu.Lock()
	w.idleTimer = time.AfterFunc(workerIdleTimeout, func() {
		m.checkWorkerIdleTimeout(w)
	})
	w.idleTimerMu.Unlock()
}

func (m *manager) checkWorkerIdleTimeout(w *worker) {
	select {
	case <-w.done:
		return
	default:
	}
	// Call workerAgentStatusForWorker to ensure idleStartedAt is updated even
	// when no browser clients are connected to trigger broadcastAgentState.
	_ = workerAgentStatusForWorker(w)

	w.idleTimerMu.Lock()
	idleStarted := w.idleStartedAt
	w.idleTimerMu.Unlock()

	if idleStarted.IsZero() || time.Since(idleStarted) < workerIdleShutdownThreshold {
		// Not yet idle, or hasn't been idle long enough — check again later.
		w.resetIdleTimer()
		return
	}
	if requested, _, _ := w.shutdownRequestDetails(); requested {
		return
	}
	m.logInfo("worker_idle_timeout instance_id=%s", w.instanceID)
	if err := m.requestIdleTimeoutShutdown(w); err != nil {
		m.logWarn("worker_idle_timeout_shutdown_error instance_id=%s err=%v", w.instanceID, err)
	}
}

func (m *manager) instanceHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		m.sendInstanceHeartbeats(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *manager) sendInstanceHeartbeats(ctx context.Context) {
	m.mu.Lock()
	instanceIDs := make([]string, 0, len(m.workers))
	for instanceID := range m.workers {
		instanceIDs = append(instanceIDs, instanceID)
	}
	m.mu.Unlock()

	for _, instanceID := range instanceIDs {
		if err := m.managerRequestJSON(
			ctx,
			http.MethodPost,
			"/agent-instances/"+instanceID+"/heartbeat",
			map[string]any{},
			nil,
		); err != nil {
			m.logError("instance_heartbeat_error instance_id=%s err=%v", instanceID, err)

			// If the backend actively rejected the heartbeat (400 or 403), it means lock loss.
			// Terminate the local process to prevent duplicate work.
			if strings.Contains(err.Error(), "status=400") || strings.Contains(err.Error(), "status=403") {
				m.logError("lock_lost_terminating_worker instance_id=%s", instanceID)
				m.mu.Lock()
				w, ok := m.workers[instanceID]
				m.mu.Unlock()
				if ok && w.cmd != nil && w.cmd.Process != nil {
					_ = killManagedPID(w.cmd.Process.Pid)
				}
			}
		}
	}
}

func (m *manager) userSessionRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := m.getValidUserAccessToken(ctx); err != nil {
				m.logError("session_refresh_warning err=%v", err)
			}
			if err := m.refreshManagerUserIdentity(ctx); err != nil {
				m.logWarn(
					"manager_user_identity_refresh_warning user_jwt_configured=%t err=%v",
					strings.TrimSpace(m.cfg.UserJWT) != "",
					err,
				)
			}
		}
	}
}

func (m *manager) logInfo(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if m.logger == nil {
		m.logger = slog.Default()
	}
	m.logger.Info(msg)
}

func (m *manager) logError(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if m.logger == nil {
		m.logger = slog.Default()
	}
	m.logger.Error(msg)
}

func (m *manager) logWarn(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if m.logger == nil {
		m.logger = slog.Default()
	}
	m.logger.Warn(msg)
}

func (m *manager) getValidUserAccessToken(ctx context.Context) (string, error) {
	if strings.TrimSpace(m.cfg.UserJWT) != "" {
		return strings.TrimSpace(m.cfg.UserJWT), nil
	}
	session, err := m.getFreshStoredSession(ctx, false)
	if err != nil {
		return "", err
	}
	return session.AccessToken, nil
}

func (m *manager) refreshSession(ctx context.Context, refreshToken string) (storedSession, error) {
	if strings.TrimSpace(refreshToken) == "" {
		return storedSession{}, fmt.Errorf("missing refresh token; run login again")
	}
	if err := m.ensureSupabaseBootstrap(ctx); err != nil {
		return storedSession{}, err
	}
	authClient := m.newAuthClient()
	tokenResp, err := authClient.RefreshToken(refreshToken)
	if err != nil {
		return storedSession{}, fmt.Errorf("refresh session failed: %w", err)
	}

	select {
	case <-ctx.Done():
		return storedSession{}, ctx.Err()
	default:
	}

	return storedSession{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresAt:    tokenResp.ExpiresAt,
		UserID:       tokenResp.User.ID.String(),
		UserEmail:    tokenResp.User.Email,
	}, nil
}

func (m *manager) getFreshStoredSession(ctx context.Context, forceRefresh bool) (storedSession, error) {
	session, err := m.loadSession()
	if err != nil {
		return storedSession{}, fmt.Errorf("no stored session; run 'passiveagents login' first")
	}
	if !forceRefresh && !sessionNeedsRefresh(session) {
		return session, nil
	}
	initialSession := session

	m.sessionRefreshMu.Lock()
	defer m.sessionRefreshMu.Unlock()

	session, err = m.loadSession()
	if err != nil {
		return storedSession{}, fmt.Errorf("no stored session; run 'passiveagents login' first")
	}
	if shouldReuseReloadedSession(forceRefresh, initialSession, session) {
		return session, nil
	}
	if !forceRefresh && !sessionNeedsRefresh(session) {
		return session, nil
	}
	if m.shouldReuseSessionDuringRefreshBackoff(forceRefresh, session) {
		return session, nil
	}

	refreshed, err := m.callRefreshSession(ctx, session.RefreshToken)
	if err != nil {
		if isRefreshTokenRotationError(err) {
			if reloaded, reloadErr := m.loadSession(); reloadErr == nil {
				if reloaded.RefreshToken != session.RefreshToken {
					if !sessionNeedsRefresh(reloaded) {
						return reloaded, nil
					}
					if refreshed, err = m.callRefreshSession(ctx, reloaded.RefreshToken); err == nil {
						if err := m.saveSession(refreshed); err != nil {
							return storedSession{}, err
						}
						return refreshed, nil
					}
					session = reloaded
					if !forceRefresh && isRefreshRateLimitError(err) && !sessionAccessTokenExpired(session) {
						m.sessionRefreshRetryAt = time.Now().Add(sessionRefreshRateLimitBackoff)
						return session, nil
					}
				}
			}
			if recovered, recoverErr := m.recoverManagerSession(ctx); recoverErr == nil {
				m.logInfo("manager_session_recovery_succeeded manager_id=%s", strings.TrimSpace(m.state.ManagerID))
				if err := m.saveSession(recovered); err != nil {
					return storedSession{}, err
				}
				return recovered, nil
			} else {
				m.logWarn("manager_session_recovery_failed manager_id=%s err=%v", strings.TrimSpace(m.state.ManagerID), recoverErr)
			}
			return storedSession{}, wrapRefreshTokenError(err)
		}
		if !forceRefresh && isRefreshRateLimitError(err) && !sessionAccessTokenExpired(session) {
			m.sessionRefreshRetryAt = time.Now().Add(sessionRefreshRateLimitBackoff)
			return session, nil
		}
		return storedSession{}, err
	}
	m.sessionRefreshRetryAt = time.Time{}
	if err := m.saveSession(refreshed); err != nil {
		return storedSession{}, err
	}
	return refreshed, nil
}

func (m *manager) saveSession(session storedSession) error {
	if strings.TrimSpace(session.AccessToken) == "" || strings.TrimSpace(session.RefreshToken) == "" {
		return fmt.Errorf("invalid session tokens")
	}
	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}
	if err := keyringSet(keyringService, keyringUser, string(raw)); err != nil {
		if keyringUnavailable(err) {
			return writeFallbackSession(raw)
		}
		return sanitizeKeyringError(err)
	}
	if err := writeFallbackSession(raw); err != nil {
		m.logWarn("session_fallback_sync_warning err=%v", err)
	}
	return nil
}

func (m *manager) loadSession() (storedSession, error) {
	var (
		keyringSession  storedSession
		fallbackSession storedSession
		keyringErr      error
		fallbackErr     error
	)

	if raw, err := keyringGet(keyringService, keyringUser); err == nil {
		keyringSession, keyringErr = decodeStoredSession([]byte(raw))
	} else {
		keyringErr = sanitizeKeyringError(err)
	}

	if raw, err := readFallbackSession(); err == nil {
		fallbackSession, fallbackErr = decodeStoredSession(raw)
	} else {
		fallbackErr = err
	}

	switch {
	case keyringErr == nil && fallbackErr == nil:
		return chooseNewerStoredSession(keyringSession, fallbackSession), nil
	case keyringErr == nil:
		return keyringSession, nil
	case fallbackErr == nil:
		return fallbackSession, nil
	case keyringErr != nil:
		return storedSession{}, keyringErr
	default:
		return storedSession{}, fallbackErr
	}
}

func (m *manager) callRefreshSession(ctx context.Context, refreshToken string) (storedSession, error) {
	if m.refreshSessionHook != nil {
		return m.refreshSessionHook(ctx, refreshToken)
	}
	return m.refreshSession(ctx, refreshToken)
}

func (m *manager) recoverManagerSession(ctx context.Context) (storedSession, error) {
	managerID := strings.TrimSpace(m.state.ManagerID)
	recoveryToken, tokenSource := loadManagerRecoveryTokenWithSource(m.state)
	if managerID == "" || recoveryToken == "" {
		return storedSession{}, fmt.Errorf(
			"manager session recovery unavailable: manager_id_present=%t recovery_token_source=%s",
			managerID != "",
			tokenSource,
		)
	}
	m.logWarn("manager_session_recovery_attempt manager_id=%s recovery_token_source=%s", managerID, tokenSource)

	var recovered struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresAt    int64  `json:"expiresAt"`
		UserID       string `json:"userId"`
	}
	headers := map[string]string{"X-Manager-Id": managerID}
	path := fmt.Sprintf("/local-agent-managers/%s/session/recover", managerID)
	if err := m.requestJSONWithHeaders(ctx, http.MethodPost, path, nil, &recovered, recoveryToken, headers); err != nil {
		return storedSession{}, err
	}
	session := storedSession{
		AccessToken:  strings.TrimSpace(recovered.AccessToken),
		RefreshToken: strings.TrimSpace(recovered.RefreshToken),
		ExpiresAt:    recovered.ExpiresAt,
		UserID:       strings.TrimSpace(recovered.UserID),
	}
	if session.AccessToken == "" || session.RefreshToken == "" || session.ExpiresAt <= 0 {
		return storedSession{}, fmt.Errorf("manager session recovery returned an invalid session")
	}
	return session, nil
}

func (m *manager) persistManagerRecoveryToken(token string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}
	err := keyringSet(keyringService, keyringRecoveryTokenUser, token)
	if err == nil {
		m.state.ManagerRecoveryToken = token
		return
	}
	m.logWarn("manager_recovery_token_keyring_warning err=%v", err)
	m.state.ManagerRecoveryToken = token
}

type supabaseJWKS struct {
	Keys []supabaseJWK `json:"keys"`
}

type supabaseJWK struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func (m *manager) validateSupabaseJWT(token string) (jwt.MapClaims, error) {
	jwksURL := strings.TrimSuffix(m.cfg.SupabaseURL, "/") + "/auth/v1/keys"
	req, err := http.NewRequest(http.MethodGet, jwksURL, nil)
	if err != nil {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}

	var jwks supabaseJWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}
	if len(jwks.Keys) == 0 {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}

	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, fmt.Errorf("missing kid")
		}
		for _, key := range jwks.Keys {
			if key.Kid != kid || key.Kty != "RSA" {
				continue
			}
			publicKey, convErr := rsaPublicKeyFromJWK(key)
			if convErr != nil {
				return nil, convErr
			}
			return publicKey, nil
		}
		return nil, fmt.Errorf("kid not found in jwks")
	})
	if err != nil || parsed == nil || !parsed.Valid {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}

	issuer := strings.TrimSuffix(m.cfg.SupabaseURL, "/") + "/auth/v1"
	if !claims.VerifyIssuer(issuer, true) {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}
	if !claims.VerifyExpiresAt(time.Now().Unix(), true) {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}
	if claimString(claims, "sub") == "" {
		return m.validateJWTViaSupabaseUserEndpoint(token)
	}
	return claims, nil
}

func (m *manager) validateJWTViaSupabaseUserEndpoint(token string) (jwt.MapClaims, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		strings.TrimSuffix(m.cfg.SupabaseURL, "/")+"/auth/v1/user",
		nil,
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("apikey", m.cfg.SupabaseAnonKey)
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid access token")
	}

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	claims := jwt.MapClaims{}
	_, _, parseErr := parser.ParseUnverified(token, claims)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid access token")
	}
	if claimString(claims, "sub") == "" {
		return nil, fmt.Errorf("invalid access token")
	}
	return claims, nil
}

func rsaPublicKeyFromJWK(jwk supabaseJWK) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(jwk.N)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
	if err != nil {
		return nil, err
	}
	if len(eBytes) == 0 {
		return nil, fmt.Errorf("empty exponent")
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: e,
	}, nil
}

func claimString(claims jwt.MapClaims, key string) string {
	value, _ := claims[key]
	out, _ := value.(string)
	return strings.TrimSpace(out)
}

func sanitizeKeyringError(err error) error {
	if err == nil {
		return nil
	}
	if keyringUnavailable(err) {
		return fmt.Errorf("secure credential store unavailable; install or unlock a keyring service and retry login")
	}
	return err
}

func shouldReuseReloadedSession(forceRefresh bool, initial, reloaded storedSession) bool {
	if !forceRefresh {
		return false
	}
	if strings.TrimSpace(reloaded.AccessToken) == "" || strings.TrimSpace(reloaded.RefreshToken) == "" {
		return false
	}
	return initial.AccessToken != reloaded.AccessToken ||
		initial.RefreshToken != reloaded.RefreshToken ||
		initial.ExpiresAt != reloaded.ExpiresAt
}

func chooseNewerStoredSession(a, b storedSession) storedSession {
	if strings.TrimSpace(a.AccessToken) == "" || strings.TrimSpace(a.RefreshToken) == "" {
		return b
	}
	if strings.TrimSpace(b.AccessToken) == "" || strings.TrimSpace(b.RefreshToken) == "" {
		return a
	}
	if b.ExpiresAt > a.ExpiresAt {
		return b
	}
	return a
}

func isRefreshTokenRotationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "refresh_token_already_used") ||
		strings.Contains(msg, "refresh_token_not_found") ||
		strings.Contains(msg, "invalid refresh token") ||
		strings.Contains(msg, "refresh token is not valid") ||
		strings.Contains(msg, "refresh token not found") ||
		strings.Contains(msg, "refresh token invalid or already used")
}

func wrapRefreshTokenError(err error) error {
	if !isRefreshTokenRotationError(err) {
		return err
	}
	return fmt.Errorf("refresh session failed: refresh token invalid or already used; run 'passiveagents login'")
}

func cleanCLIError(err error) string {
	if err == nil {
		return ""
	}
	if isRefreshTokenRotationError(err) {
		return "Refresh token invalid or already used. Run 'passiveagents login'."
	}
	return sanitizeKeyringError(err).Error()
}

func sessionNeedsRefresh(session storedSession) bool {
	if session.ExpiresAt <= 0 {
		return true
	}
	expiry := time.Unix(session.ExpiresAt, 0)
	return time.Until(expiry) <= tokenRefreshSkew
}

func isRefreshRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "over_request_rate_limit") || strings.Contains(msg, "status code 429")
}

func sessionAccessTokenExpired(session storedSession) bool {
	if session.ExpiresAt <= 0 {
		return true
	}
	return !time.Now().Before(time.Unix(session.ExpiresAt, 0))
}

func (m *manager) shouldReuseSessionDuringRefreshBackoff(forceRefresh bool, session storedSession) bool {
	if forceRefresh {
		return false
	}
	if m.sessionRefreshRetryAt.IsZero() || time.Now().After(m.sessionRefreshRetryAt) {
		return false
	}
	if strings.TrimSpace(session.AccessToken) == "" {
		return false
	}
	return !sessionAccessTokenExpired(session)
}

func parseJWTExpiry(accessToken string) int64 {
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	claims := jwt.MapClaims{}
	_, _, err := parser.ParseUnverified(accessToken, claims)
	if err != nil {
		return 0
	}
	switch exp := claims["exp"].(type) {
	case float64:
		return int64(exp)
	case int64:
		return exp
	case json.Number:
		parsed, _ := exp.Int64()
		return parsed
	default:
		return 0
	}
}

func (m *manager) requestJSON(ctx context.Context, method, path string, payload any, out any, token string) error {
	return m.requestJSONWithHeaders(ctx, method, path, payload, out, token, nil)
}

func (m *manager) userRequestJSON(ctx context.Context, method, path string, payload any, out any) error {
	token, err := m.getValidUserAccessToken(ctx)
	if err != nil {
		return err
	}
	err = m.requestJSONWithHeaders(ctx, method, path, payload, out, token, nil)
	if err == nil || !isAuthTokenExpiredError(err) || strings.TrimSpace(m.cfg.UserJWT) != "" {
		return err
	}
	refreshedToken, refreshErr := m.forceRefreshUserAccessToken(ctx)
	if refreshErr != nil {
		return refreshErr
	}
	return m.requestJSONWithHeaders(ctx, method, path, payload, out, refreshedToken, nil)
}

func (m *manager) managerRequestJSON(ctx context.Context, method, path string, payload any, out any) error {
	token, err := m.getValidUserAccessToken(ctx)
	if err != nil {
		return err
	}
	headers := map[string]string{}
	if strings.TrimSpace(m.state.ManagerID) != "" {
		headers["X-Manager-Id"] = strings.TrimSpace(m.state.ManagerID)
	}
	err = m.requestJSONWithHeaders(ctx, method, path, payload, out, token, headers)
	if err == nil || !isAuthTokenExpiredError(err) || strings.TrimSpace(m.cfg.UserJWT) != "" {
		return err
	}
	refreshedToken, refreshErr := m.forceRefreshUserAccessToken(ctx)
	if refreshErr != nil {
		return refreshErr
	}
	return m.requestJSONWithHeaders(ctx, method, path, payload, out, refreshedToken, headers)
}

func (m *manager) requestJSONWithHeaders(
	ctx context.Context,
	method, path string,
	payload any,
	out any,
	token string,
	headers map[string]string,
) error {
	var bodyReader *bytes.Reader
	if payload == nil {
		bodyReader = bytes.NewReader([]byte{})
	} else {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, m.cfg.APIBaseURL+path, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		req.Header.Set(key, value)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return fmt.Errorf("%s %s failed: status=%d body=%s", method, path, resp.StatusCode, strings.TrimSpace(buf.String()))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (m *manager) listWorkingTasks(ctx context.Context) error {
	output, err := m.renderWorkingTasks(ctx)
	if err != nil {
		return err
	}
	fmt.Print(output)
	return nil
}

func (m *manager) listAgentInstances(ctx context.Context) error {
	output, err := m.renderAgentInstances(ctx)
	if err != nil {
		return err
	}
	fmt.Print(output)
	return nil
}

func (m *manager) renderAgentInstances(ctx context.Context) (string, error) {
	state, err := readState(m.cfg.StateFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if errors.Is(err, os.ErrNotExist) {
		state = persistedState{Instances: map[string]persistedWorker{}}
	}
	if state.Instances == nil {
		state.Instances = map[string]persistedWorker{}
	}

	type apiAgentLite struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Role string `json:"role"`
	}
	type apiTaskLite struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Status string `json:"status"`
	}

	agentByID := map[string]apiAgentLite{}
	var agents []apiAgentLite
	if err := m.userRequestJSON(ctx, http.MethodGet, "/agents", nil, &agents); err == nil {
		for _, agent := range agents {
			agentByID[agent.ID] = agent
		}
	}

	taskByID := map[string]apiTaskLite{}
	var tasks []apiTaskLite
	if err := m.userRequestJSON(ctx, http.MethodGet, "/tasks", nil, &tasks); err == nil {
		for _, task := range tasks {
			taskByID[task.ID] = task
		}
	}

	instanceIDs := make([]string, 0, len(state.Instances))
	for instanceID := range state.Instances {
		instanceIDs = append(instanceIDs, instanceID)
	}
	sort.Strings(instanceIDs)

	if len(instanceIDs) == 0 {
		return "No local agent instances.\n", nil
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "AGENT_INSTANCE_ID\tAGENT_NAME\tAGENT_ROLE\tTASK_ID\tTASK_NAME\tSTATUS")
	for _, instanceID := range instanceIDs {
		instance := state.Instances[instanceID]
		agentName := instance.AgentID
		agentRole := ""
		if agent, ok := agentByID[instance.AgentID]; ok {
			if strings.TrimSpace(agent.Name) != "" {
				agentName = agent.Name
			}
			agentRole = agent.Role
		}

		taskID := instance.TaskID
		taskName := instance.TaskID
		status := "LOCAL_RUNNING"
		if !isProcessRunning(instance.PID) {
			status = "LOCAL_STOPPED"
		}
		if task, ok := taskByID[instance.TaskID]; ok {
			taskID = task.ID
			if strings.TrimSpace(task.Name) != "" {
				taskName = task.Name
			}
			if strings.TrimSpace(task.Status) != "" {
				status = task.Status
			}
		}

		_, _ = fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			instanceID,
			agentName,
			agentRole,
			taskID,
			taskName,
			status,
		)
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (m *manager) resetTaskForTesting(ctx context.Context, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}

	state, err := readState(m.cfg.StateFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		state = persistedState{Instances: map[string]persistedWorker{}}
	}
	if state.Instances == nil {
		state.Instances = map[string]persistedWorker{}
	}

	stopped := 0
	removed := 0
	for instanceID, instance := range state.Instances {
		if instance.TaskID != taskID {
			continue
		}
		if isProcessRunning(instance.PID) {
			if proc, findErr := os.FindProcess(instance.PID); findErr == nil {
				if sigErr := proc.Signal(syscall.SIGTERM); sigErr == nil {
					stopped++
				}
			}
		}
		delete(state.Instances, instanceID)
		removed++
	}
	if removed > 0 {
		if writeErr := writeState(m.cfg.StateFile, state); writeErr != nil {
			return writeErr
		}
	}

	payload := map[string]string{"status": "READY"}
	if err := m.userRequestJSON(ctx, http.MethodPut, "/tasks/"+taskID, payload, nil); err != nil {
		return err
	}

	fmt.Printf(
		"Task %s reset to READY. removed_instances=%d stopped_processes=%d\n",
		taskID,
		removed,
		stopped,
	)
	return nil
}

func (m *manager) renderWorkingTasks(ctx context.Context) (string, error) {
	state, err := readState(m.cfg.StateFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if state.Instances == nil {
		state.Instances = map[string]persistedWorker{}
	}

	taskByID := map[string]apiTask{}
	var allTasks []apiTask
	if err := m.userRequestJSON(ctx, http.MethodGet, "/tasks", nil, &allTasks); err == nil {
		for _, task := range allTasks {
			taskByID[task.ID] = task
		}
	}

	agentNameByID := map[string]string{}
	type apiAgentLite struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	var agents []apiAgentLite
	if err := m.userRequestJSON(ctx, http.MethodGet, "/agents", nil, &agents); err == nil {
		for _, agent := range agents {
			agentNameByID[agent.ID] = agent.Name
		}
	}

	type row struct {
		taskID     string
		taskName   string
		instanceID string
		agentName  string
		status     string
	}
	rows := make([]row, 0)
	seenTaskIDs := map[string]struct{}{}
	for instanceID, instance := range state.Instances {
		if strings.TrimSpace(instance.TaskID) == "" {
			continue
		}
		if _, exists := seenTaskIDs[instance.TaskID]; exists {
			continue
		}
		agentName := agentNameByID[instance.AgentID]
		taskName := instance.TaskID
		status := "LOCAL_RUNNING"
		if !isProcessRunning(instance.PID) {
			status = "LOCAL_STOPPED"
		}
		if task, ok := taskByID[instance.TaskID]; ok {
			if strings.TrimSpace(task.Name) != "" {
				taskName = task.Name
			}
			if strings.TrimSpace(task.Status) != "" {
				status = task.Status
			}
		}
		rows = append(rows, row{
			taskID:     instance.TaskID,
			taskName:   taskName,
			instanceID: instanceID,
			agentName:  agentName,
			status:     status,
		})
		seenTaskIDs[instance.TaskID] = struct{}{}
	}

	var readyTasks []apiTask
	if err := m.userRequestJSON(
		ctx,
		http.MethodGet,
		"/tasks?status=READY",
		nil,
		&readyTasks,
	); err != nil {
		return "", fmt.Errorf("unable to fetch READY tasks for current user session: %w", err)
	}
	for _, task := range readyTasks {
		if _, exists := seenTaskIDs[task.ID]; exists {
			continue
		}
		rows = append(rows, row{
			taskID:     task.ID,
			taskName:   task.Name,
			instanceID: "",
			agentName:  "",
			status:     task.Status,
		})
	}

	if len(rows) == 0 {
		return "No local running tasks and no READY tasks for the currently authenticated user.\n", nil
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TASK_ID\tTASK_NAME\tAGENT_NAME\tAGENT_INSTANCE_ID\tSTATUS")
	for _, r := range rows {
		_, _ = fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\n",
			r.taskID,
			r.taskName,
			r.agentName,
			r.instanceID,
			r.status,
		)
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (m *manager) forceRefreshUserAccessToken(ctx context.Context) (string, error) {
	session, err := m.getFreshStoredSession(ctx, true)
	if err != nil {
		return "", err
	}
	return session.AccessToken, nil
}

func isAuthTokenExpiredError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "jwt expired") ||
		strings.Contains(msg, "invalid jwt") ||
		strings.Contains(msg, "status=401") ||
		strings.Contains(msg, "status=403")
}

func (m *manager) reconcileLocalState() {
	m.mu.Lock()
	if m.state.Instances == nil {
		m.state.Instances = map[string]persistedWorker{}
		m.mu.Unlock()
		return
	}

	changed := false
	for instanceID, worker := range m.state.Instances {
		if isProcessRunning(worker.PID) {
			continue
		}
		if m.shouldKeepRestartableInstance(worker) {
			continue
		}
		delete(m.state.Instances, instanceID)
		changed = true
	}
	statePath := m.cfg.StateFile
	if changed {
		_ = writeState(statePath, m.state)
	}
	m.mu.Unlock()
}

func (m *manager) shouldKeepRestartableInstance(worker persistedWorker) bool {
	status := strings.ToLower(strings.TrimSpace(worker.Status))
	if status != "working" && status != "idle" {
		return false
	}
	if m.previousManagerPID <= 0 || m.previousManagerPID == os.Getpid() {
		return false
	}
	return strings.TrimSpace(worker.TaskID) != "" &&
		strings.TrimSpace(worker.AgentID) != "" &&
		strings.TrimSpace(worker.WorkingDir) != "" &&
		strings.TrimSpace(worker.RuntimeCommand) != ""
}

func (m *manager) shutdownWorkersForManagerStop() {
	m.mu.Lock()
	workers := make([]*worker, 0, len(m.workers))
	for _, w := range m.workers {
		if w != nil {
			workers = append(workers, w)
		}
	}
	m.mu.Unlock()
	if len(workers) == 0 {
		return
	}
	for _, w := range workers {
		if err := m.requestManagerRestartWorkerShutdown(w); err != nil {
			m.logWarn("manager_stop_shutdown_request_error instance_id=%s err=%v", w.instanceID, err)
		}
	}
	deadline := time.Now().Add(gracefulShutdownWait + time.Second)
	for _, w := range workers {
		if w == nil {
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		timer := time.NewTimer(remaining)
		select {
		case <-w.done:
			timer.Stop()
		case <-timer.C:
			return
		}
	}
}

func (m *manager) recoverRestartableWorkers(ctx context.Context) error {
	if m.previousManagerPID <= 0 || m.previousManagerPID == os.Getpid() {
		return nil
	}

	m.mu.Lock()
	candidates := make(map[string]persistedWorker, len(m.state.Instances))
	for instanceID, persisted := range m.state.Instances {
		if !m.shouldKeepRestartableInstance(persisted) {
			continue
		}
		if m.workers[instanceID] != nil {
			continue
		}
		if persisted.PID > 0 && isProcessRunning(persisted.PID) {
			continue
		}
		candidates[instanceID] = persisted
	}
	m.mu.Unlock()

	for instanceID, persisted := range candidates {
		prepared, err := m.prepareCommandRoute(ctx, instanceID, persisted.TaskID)
		if err != nil {
			m.logWarn("restartable_worker_prepare_error instance_id=%s task_id=%s err=%v", instanceID, persisted.TaskID, err)
			continue
		}
		persona, err := m.fetchPersonaByID(ctx, prepared.AgentID)
		if err != nil {
			m.logWarn("restartable_worker_persona_error instance_id=%s agent_id=%s err=%v", instanceID, prepared.AgentID, err)
			continue
		}
		if strings.TrimSpace(persisted.RuntimeCommand) != "" {
			persona.AgentRuntime.CommandTemplate = persisted.RuntimeCommand
		}
		if err := m.spawnWorkerWithWorkingDir(
			ctx,
			persona,
			prepared.Task,
			1,
			instanceID,
			persisted.WorkingDir,
		); err != nil {
			m.logWarn("restartable_worker_spawn_error instance_id=%s task_id=%s err=%v", instanceID, prepared.Task.ID, err)
			continue
		}
	}
	return nil
}

func (m *manager) orphanRecoveryLoop(ctx context.Context) {
	if m.previousManagerPID <= 0 || m.previousManagerPID == os.Getpid() {
		return
	}
	ticker := time.NewTicker(orphanRecoveryPollInterval)
	defer ticker.Stop()
	for {
		if err := m.recoverOrphanedWorkers(ctx); err != nil {
			m.logWarn("orphaned_worker_recovery_error err=%v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *manager) recoverOrphanedWorkers(ctx context.Context) error {
	if m.previousManagerPID <= 0 || m.previousManagerPID == os.Getpid() {
		return nil
	}

	m.mu.Lock()
	candidates := make(map[string]persistedWorker, len(m.state.Instances))
	for instanceID, persisted := range m.state.Instances {
		if !m.shouldKeepRestartableInstance(persisted) {
			continue
		}
		if m.workers[instanceID] != nil {
			continue
		}
		if persisted.PID <= 0 || !isProcessRunning(persisted.PID) {
			continue
		}
		candidates[instanceID] = persisted
	}
	m.mu.Unlock()

	for instanceID, persisted := range candidates {
		status, err := m.fetchAgentInstanceStatus(ctx, instanceID)
		if err != nil {
			m.logWarn("orphaned_worker_status_error instance_id=%s err=%v", instanceID, err)
			continue
		}
		normalized := normalizeRestartableWorkerStatus(status.Status)
		persistedNormalized := normalizeRestartableWorkerStatus(persisted.Status)
		if persistedNormalized == "idle" && normalized == "working" {
			normalized = "idle"
		}
		m.updatePersistedWorkerStatus(instanceID, normalized)
		if !m.shouldTerminateOrphanedWorker(instanceID, persisted, normalized) {
			continue
		}
		if err := terminateProcess(persisted.PID); err != nil && !errors.Is(err, os.ErrProcessDone) {
			m.logWarn("orphaned_worker_shutdown_error instance_id=%s pid=%d err=%v", instanceID, persisted.PID, err)
			continue
		}
		if !waitForProcessExit(persisted.PID, 2*time.Second) {
			m.logWarn("orphaned_worker_shutdown_timeout instance_id=%s pid=%d", instanceID, persisted.PID)
		}
	}

	return m.recoverRestartableWorkers(ctx)
}

func (m *manager) fetchAgentInstanceStatus(ctx context.Context, instanceID string) (apiAgentInstanceStatus, error) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return apiAgentInstanceStatus{}, fmt.Errorf("missing instance id")
	}
	var out apiAgentInstanceStatus
	if err := m.userRequestJSON(ctx, http.MethodGet, "/agents/instances/"+instanceID, nil, &out); err != nil {
		return apiAgentInstanceStatus{}, err
	}
	return out, nil
}

func normalizeRestartableWorkerStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "IDLE":
		return "idle"
	case "OFFLINE":
		return "offline"
	default:
		return "working"
	}
}

func (m *manager) shouldTerminateOrphanedWorker(instanceID string, persisted persistedWorker, normalizedStatus string) bool {
	if normalizedStatus != "working" {
		return true
	}
	snapshot, err := m.orphanedWorkerSnapshot(instanceID, persisted)
	if err != nil {
		m.logWarn("orphaned_worker_snapshot_error instance_id=%s err=%v", instanceID, err)
		return false
	}
	if snapshot == "" {
		return false
	}
	status := workerPromptStatusForRuntime(strings.TrimSpace(persisted.RuntimeCommand), snapshot)
	if status.State == workerPromptStateReadyEmpty {
		return true
	}
	return false
}

func (m *manager) orphanedWorkerSnapshot(instanceID string, persisted persistedWorker) (string, error) {
	logPath := strings.TrimSpace(persisted.LocalLogFile)
	if logPath == "" {
		logPath = filepath.Join(filepath.Dir(m.cfg.StateFile), "instances", instanceID, "logs.raw")
	}
	raw, err := readFileTail(logPath, 4096)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	return sanitizeTerminalFragment(string(raw)), nil
}

func readFileTail(path string, maxBytes int64) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	start := info.Size() - maxBytes
	if start < 0 {
		start = 0
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(file)
}

func (m *manager) updatePersistedWorkerStatus(instanceID, status string) {
	instanceID = strings.TrimSpace(instanceID)
	status = strings.TrimSpace(status)
	if instanceID == "" || status == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	persisted, ok := m.state.Instances[instanceID]
	if !ok {
		return
	}
	if strings.TrimSpace(persisted.Status) == status {
		return
	}
	persisted.Status = status
	persisted.AttachAddr = ""
	m.state.Instances[instanceID] = persisted
	_ = writeState(m.cfg.StateFile, m.state)
}

func (m *manager) persistWorkerActivityStatus(w *worker) {
	if w == nil || strings.TrimSpace(w.instanceID) == "" {
		return
	}
	status := workerAgentStatusForWorker(w)
	if status == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	m.mu.Lock()
	defer m.mu.Unlock()
	persisted, ok := m.state.Instances[w.instanceID]
	if !ok {
		return
	}
	if strings.TrimSpace(persisted.Status) == status {
		return
	}
	persisted.Status = status
	if status == "idle" {
		persisted.LastIdleAt = now
	} else if status == "working" {
		persisted.LastWorkingAt = now
	}
	m.state.Instances[w.instanceID] = persisted
	_ = writeState(m.cfg.StateFile, m.state)
	w.resetIdleTimer()
	m.mu.Unlock()
	m.broadcastAgentState(w.instanceID, status, "")
	m.mu.Lock()
}

func readState(path string) (persistedState, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return persistedState{}, err
	}
	var state persistedState
	if err := json.Unmarshal(raw, &state); err != nil {
		return persistedState{}, err
	}
	if state.ManagerPID <= 0 && state.LegacyDaemonPID > 0 {
		state.ManagerPID = state.LegacyDaemonPID
	}
	state.LegacyDaemonPID = 0
	if state.Instances == nil {
		state.Instances = map[string]persistedWorker{}
	}
	if state.PersonaLessonHashes == nil {
		state.PersonaLessonHashes = map[string]string{}
	}
	return state, nil
}

func ensurePersistedManagerRecoveryToken(statePath string) error {
	state, err := readState(statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if strings.TrimSpace(state.ManagerID) == "" {
		return nil
	}
	if strings.TrimSpace(loadManagerRecoveryToken(state)) != "" {
		return nil
	}
	return missingRecoveryTokenInstructionError()
}

func loadManagerRecoveryToken(state persistedState) string {
	token, _ := loadManagerRecoveryTokenWithSource(state)
	return token
}

func loadManagerRecoveryTokenWithSource(state persistedState) (string, string) {
	if token, err := keyringGet(keyringService, keyringRecoveryTokenUser); err == nil {
		token = strings.TrimSpace(token)
		if token != "" {
			return token, "keyring"
		}
	}
	if token := strings.TrimSpace(state.ManagerRecoveryToken); token != "" {
		return token, "state_file"
	}
	return "", "missing"
}

func writeState(path string, state persistedState) error {
	if state.Instances == nil {
		state.Instances = map[string]persistedWorker{}
	}
	if state.PersonaLessonHashes == nil {
		state.PersonaLessonHashes = map[string]string{}
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, raw, 0o600, 0o700)
}

func tailManagerLogs(path string, follow bool) error {
	offset := int64(0)
	for {
		file, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && follow {
				time.Sleep(1 * time.Second)
				continue
			}
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("manager log file not found: %s", path)
			}
			return err
		}

		stat, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return statErr
		}
		if offset > stat.Size() {
			offset = 0
		}
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			_ = file.Close()
			return err
		}
		written, copyErr := io.Copy(os.Stdout, file)
		_ = file.Close()
		if copyErr != nil {
			return copyErr
		}
		offset += written

		if !follow {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

func tailLogs(cfg config, instanceID string, follow bool) error {
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("PASSIVEAGENTS_DATABASE_URL is required")
	}
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	)
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	last := time.Unix(0, 0)
	for {
		rows, err := db.Query(`
			select log_line, log_type, "timestamp"
			from agent_logs
			where agent_instance_id = $1 and "timestamp" > $2
			order by "timestamp" asc
		`, instanceID, last)
		if err != nil {
			return err
		}
		for rows.Next() {
			var line string
			var logType string
			var ts time.Time
			if scanErr := rows.Scan(&line, &logType, &ts); scanErr != nil {
				_ = rows.Close()
				return scanErr
			}
			level := slog.LevelInfo
			if strings.EqualFold(logType, "error") {
				level = slog.LevelError
			}
			logger.Log(
				context.Background(),
				level,
				"agent_log",
				slog.String("log_type", logType),
				slog.String("line", line),
				slog.Time("timestamp", ts.UTC()),
				slog.String("agent_instance_id", instanceID),
			)
			last = ts
		}
		_ = rows.Close()

		if !follow {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}

func tailLocalAgentLogs(stateFilePath, instanceID string, watch bool, raw bool) error {
	state, err := readState(stateFilePath)
	if err != nil {
		return err
	}
	instance, ok := state.Instances[instanceID]
	if !ok {
		return fmt.Errorf("agent %s is no longer running", instanceID)
	}

	logPath := strings.TrimSpace(instance.LocalLogFile)
	if logPath == "" {
		logPath = filepath.Join(filepath.Dir(stateFilePath), "instances", instanceID, "logs.raw")
	}

	if raw {
		return tailRawFile(logPath, watch)
	}

	transcriptPath := strings.TrimSpace(instance.LocalTranscriptFile)
	if transcriptPath == "" {
		transcriptPath = filepath.Join(filepath.Dir(stateFilePath), "instances", instanceID, "transcript.ndjson")
	}
	return tailTranscriptFile(transcriptPath, watch)
}

func tailRawFile(path string, watch bool) error {
	offset := int64(0)
	for {
		file, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && watch {
				time.Sleep(1 * time.Second)
				continue
			}
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("agent log file not found: %s", path)
			}
			return err
		}

		stat, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return statErr
		}
		if offset > stat.Size() {
			offset = 0
		}
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			_ = file.Close()
			return err
		}
		written, copyErr := io.Copy(os.Stdout, file)
		_ = file.Close()
		if copyErr != nil {
			return copyErr
		}
		offset += written

		if !watch {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

func tailTranscriptFile(path string, watch bool) error {
	logs, offset, err := readLocalAgentLogsFromStart(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("agent transcript file not found: %s", path)
		}
		return err
	}
	printRenderedChatLogs(logs)

	if !watch {
		return nil
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		fresh, nextOffset, readErr := readLocalAgentLogsFromOffset(path, offset)
		if readErr != nil {
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			return readErr
		}
		printRenderedChatLogs(fresh)
		offset = nextOffset
	}
	return nil
}

type apiAgentLog struct {
	ID        string `json:"id"`
	LogLine   string `json:"log_line"`
	Line      string `json:"line"`
	LogType   string `json:"log_type"`
	Timestamp string `json:"timestamp"`
}

func (m *manager) chatWithInstance(ctx context.Context, instanceID string) error {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return fmt.Errorf("instance id is required")
	}
	attachAddr, err := m.resolveAttachAddress(instanceID)
	if err != nil {
		return err
	}
	return HijackAttach(attachAddr)
}

func (m *manager) resolveAttachAddress(instanceID string) (string, error) {
	state, err := readState(m.cfg.StateFile)
	if err != nil {
		return "", err
	}
	instance, ok := state.Instances[instanceID]
	if !ok || !isProcessRunning(instance.PID) {
		return "", fmt.Errorf("agent %s is no longer running", instanceID)
	}
	addr := strings.TrimSpace(instance.AttachAddr)
	if addr == "" {
		return "", fmt.Errorf("agent %s does not support live attach; restart the agent instance", instanceID)
	}
	return addr, nil
}

func (m *manager) chatWithInstancePlain(ctx context.Context, instanceID string) error {
	logPath, cmdPath, err := m.resolveLocalChatFiles(instanceID)
	if err != nil {
		return err
	}

	fmt.Printf("Connected to instance %s\n", instanceID)
	fmt.Println("Type a command and press Enter. Type /exit to quit.")
	history, offset, err := readLocalAgentLogsFromStart(logPath)
	if err != nil {
		return err
	}
	printRenderedChatLogs(history)

	inputCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	inputCh := make(chan string, 8)
	inputErrCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			select {
			case inputCh <- line:
			case <-inputCtx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			inputErrCh <- err
			return
		}
		close(inputCh)
	}()
	poll := time.NewTicker(1 * time.Second)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-inputErrCh:
			return err
		case line, ok := <-inputCh:
			if !ok {
				return nil
			}
			if line == "" {
				continue
			}
			if line == "/exit" || line == "exit" {
				return nil
			}
			if err := appendCommandToFile(cmdPath, line); err != nil {
				fmt.Printf("send_error: %s\n", cleanCLIError(err))
			}
		case <-poll.C:
			fresh, nextOffset, pollErr := readLocalAgentLogsFromOffset(logPath, offset)
			if pollErr != nil {
				return pollErr
			}
			printRenderedChatLogs(fresh)
			offset = nextOffset
		}
	}
}

func (m *manager) resolveLocalChatFiles(instanceID string) (string, string, error) {
	state, err := readState(m.cfg.StateFile)
	if err != nil {
		return "", "", err
	}
	instance, ok := state.Instances[instanceID]
	if !ok {
		return "", "", fmt.Errorf("agent %s is no longer running", instanceID)
	}

	logPath := strings.TrimSpace(instance.LocalLogFile)
	cmdPath := strings.TrimSpace(instance.LocalCommandFile)
	if logPath == "" || cmdPath == "" {
		return "", "", fmt.Errorf("instance %s has no local chat files; restart manager/worker", instanceID)
	}
	return logPath, cmdPath, nil
}

func readNewFileBytes(path string, offset int64) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, offset, nil
		}
		return nil, offset, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, offset, err
	}
	if offset > info.Size() {
		offset = 0
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, offset, err
	}
	return data, offset + int64(len(data)), nil
}

func readLocalAgentLogsFromStart(path string) ([]apiAgentLog, int64, error) {
	return readLocalAgentLogsFromOffset(path, 0)
}

func readLocalAgentLogsFromOffset(path string, offset int64) ([]apiAgentLog, int64, error) {
	data, nextOffset, err := readNewFileBytes(path, offset)
	if err != nil {
		return nil, offset, err
	}
	if len(data) == 0 {
		return nil, nextOffset, nil
	}

	lines := strings.Split(string(data), "\n")
	out := make([]apiAgentLog, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry logEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		out = append(out, apiAgentLog{
			LogLine:   entry.Line,
			Line:      entry.Line,
			LogType:   entry.LogType,
			Timestamp: entry.Timestamp,
		})
	}
	return out, nextOffset, nil
}

func appendCommandToFile(path, command string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty command file path")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.WriteString(f, strings.TrimSpace(command)+"\n")
	return err
}

func printRenderedChatLogs(entries []apiAgentLog) {
	for _, line := range renderChatLogs(entries) {
		fmt.Println(line)
	}
}

func renderChatLogs(entries []apiAgentLog) []string {
	lines := make([]string, 0, len(entries))
	var stdoutFragments strings.Builder

	flushStdout := func() {
		if stdoutFragments.Len() == 0 {
			return
		}
		line := normalizeAgentStdout(stdoutFragments.String())
		if line != "" {
			lines = append(lines, line)
		}
		stdoutFragments.Reset()
	}

	appendStdout := func(fragment string) {
		fragment = sanitizeTerminalFragment(fragment)
		if fragment == "" {
			return
		}
		for _, r := range fragment {
			if r == '\n' {
				flushStdout()
				continue
			}
			stdoutFragments.WriteRune(r)
		}
	}

	for _, entry := range entries {
		raw := entry.LogLine
		if raw == "" {
			raw = entry.Line
		}
		logType := strings.TrimSpace(entry.LogType)
		if logType == "" {
			logType = "stdout"
		}

		if logType == "user" {
			if looksLikeEchoedPrompt(raw) {
				continue
			}
			flushStdout()
			for _, userLine := range formatUserLogLines(raw) {
				lines = append(lines, "you> "+userLine)
			}
			continue
		}
		if logType == "assistant" {
			flushStdout()
			for _, assistantLine := range formatUserLogLines(raw) {
				lines = append(lines, "assistant> "+assistantLine)
			}
			continue
		}

		if logType == "stdout" {
			appendStdout(raw)
			continue
		}

		flushStdout()
		lines = append(lines, formatAgentLog(entry))
	}

	flushStdout()
	return lines
}

func formatUserLogLines(raw string) []string {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	chunks := strings.Split(raw, "\n")
	lines := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			if len(lines) > 0 {
				lines = append(lines, "")
			}
			continue
		}
		lines = append(lines, chunk)
	}
	if len(lines) == 0 {
		return []string{"(empty)"}
	}
	return lines
}

func maxLogTimestampMillis(entries []apiAgentLog) int64 {
	var maxMillis int64
	for _, entry := range entries {
		tsMillis := parseLogTimestampMillis(entry.Timestamp)
		if tsMillis > maxMillis {
			maxMillis = tsMillis
		}
	}
	return maxMillis
}

func sanitizeTerminalFragment(value string) string {
	if value == "" {
		return ""
	}
	value = ansiOSCRegex.ReplaceAllString(value, "")
	value = ansiCSIRegex.ReplaceAllString(value, "")
	// Strip CSI parameter residue when escape was split across chunks (e.g. "39;49m").
	value = csiParamResidueRegex.ReplaceAllString(value, "")
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if r < 32 {
			return -1
		}
		return r
	}, value)
	return value
}

type lifecycleEvent struct {
	status  string
	marker  string
	comment string
	index   int
}

func extractLatestLifecycleEvent(recent string) (lifecycleEvent, bool) {
	completed, hasCompleted := extractLifecycleEvent(
		recent,
		taskCompletedRegex,
		"TASK_COMPLETED",
		"READY_FOR_REVIEW",
		"Task completed (no summary).",
	)
	needsInput, hasNeedsInput := extractLifecycleEvent(
		recent,
		needsInputRegex,
		"NEEDS_USER_INPUT",
		"WAITING_FOR_USER_INPUT",
		"Agent requested user input.",
	)
	switch {
	case hasCompleted && hasNeedsInput:
		if needsInput.index >= completed.index {
			return needsInput, true
		}
		return completed, true
	case hasNeedsInput:
		return needsInput, true
	case hasCompleted:
		return completed, true
	default:
		return lifecycleEvent{}, false
	}
}

func extractLifecycleEvent(
	recent string,
	re *regexp.Regexp,
	marker string,
	status string,
	fallback string,
) (lifecycleEvent, bool) {
	matches := re.FindAllStringSubmatchIndex(recent, -1)
	if len(matches) == 0 {
		return lifecycleEvent{}, false
	}
	for idx := len(matches) - 1; idx >= 0; idx-- {
		loc := matches[idx]
		comment := fallback
		rawComment := ""
		if len(loc) >= 4 && loc[2] >= 0 && loc[3] >= 0 {
			rawComment = recent[loc[2]:loc[3]]
			comment = cleanLifecycleComment(rawComment, fallback)
		}
		if isPlaceholderLifecycleComment(marker, rawComment, comment) {
			continue
		}
		return lifecycleEvent{
			status:  status,
			marker:  marker,
			comment: comment,
			index:   loc[0],
		}, true
	}
	return lifecycleEvent{}, false
}

func cleanLifecycleComment(raw string, fallback string) string {
	value := sanitizeTerminalFragment(raw)
	value = splitCSIResidueRegex.ReplaceAllString(value, "")

	lines := strings.Split(value, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		line = strings.Trim(line, "│")
		line = strings.TrimSpace(line)
		line = strings.Join(strings.Fields(line), " ")
		if line == "" {
			continue
		}
		if isDecorativeTerminalLine(line) {
			continue
		}
		cleaned = append(cleaned, line)
	}
	value = strings.TrimSpace(strings.Join(cleaned, "\n"))
	if value == "" {
		return fallback
	}
	return value
}

func isPlaceholderLifecycleComment(marker, raw, comment string) bool {
	combined := strings.ToLower(strings.TrimSpace(raw + "\n" + comment))
	if combined == "" {
		return false
	}
	normalized := strings.Join(strings.Fields(combined), " ")
	compact := strings.ReplaceAll(normalized, " ", "")
	switch strings.TrimSpace(marker) {
	case "TASK_COMPLETED":
		return strings.Contains(normalized, "<one-paragraph summary of what you did>")
	case "NEEDS_USER_INPUT":
		if strings.Contains(normalized, "<your question for the user>") ||
			strings.Contains(normalized, "<your question for") ||
			strings.HasPrefix(strings.TrimSpace(normalized), "<y") ||
			strings.Contains(compact, "<yourquestionfortheuser>") ||
			strings.Contains(compact, "<yourquestionfor") ||
			strings.HasPrefix(compact, "<y") ||
			strings.Contains(compact, "<yourquesionfor") {
			return true
		}
		return strings.Contains(normalized, "start work now") &&
			strings.Contains(normalized, "keep responses concise and actionable")
	default:
		return false
	}
}

func isDecorativeTerminalLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return true
	}
	for _, r := range trimmed {
		switch {
		case r == '│' || r == '─' || r == '╭' || r == '╮' || r == '╰' || r == '╯':
			continue
		case r == '[' || r == ']' || r == '(' || r == ')' || r == '{' || r == '}':
			continue
		case r == ':' || r == ';' || r == ',' || r == '.' || r == '!' || r == '?' || r == '-' || r == '+':
			continue
		case r == ' ' || r == '\t':
			continue
		default:
			return false
		}
	}
	return true
}

func looksLikeEchoedPrompt(line string) bool {
	return strings.Contains(line, "Agent Persona:") &&
		strings.Contains(line, "Task Brief")
}

func normalizeAgentStdout(raw string) string {
	return raw
}

func cleanPromptContextText(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	value := strings.ReplaceAll(raw, " | ", "\n")
	value = strings.ReplaceAll(value, "|", "\n")
	value = sanitizeTerminalFragment(value)
	if value == "" {
		return ""
	}

	cleaned := make([]string, 0, 8)
	for _, line := range strings.Split(value, "\n") {
		line = strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "###") {
			continue
		}
		if strings.Contains(lower, "[task_completed]") ||
			strings.Contains(lower, "[needs_user_input]") ||
			strings.Contains(lower, "output exactly this line on its own") ||
			strings.Contains(lower, "start work now") ||
			strings.Contains(lower, "keep responses concise") ||
			strings.Contains(lower, "recent task checkpoints") ||
			strings.Contains(lower, "recent task comments") ||
			strings.Contains(lower, "lifecycle signals") ||
			strings.Contains(lower, "pasted content") ||
			strings.Contains(lower, "gpt-5.4") ||
			strings.Contains(lower, "context left") {
			continue
		}
		if shouldDropAssistantLine(line) {
			continue
		}
		cleaned = append(cleaned, line)
	}
	if len(cleaned) == 0 {
		return ""
	}
	return strings.TrimSpace(strings.Join(cleaned, "\n"))
}

func formatAgentLog(entry apiAgentLog) string {
	line := strings.TrimSpace(entry.LogLine)
	if line == "" {
		line = strings.TrimSpace(entry.Line)
	}
	if line == "" {
		line = "(empty)"
	}
	logType := strings.TrimSpace(entry.LogType)
	if logType == "" {
		logType = "stdout"
	}
	ts := strings.TrimSpace(entry.Timestamp)
	if ts == "" {
		return fmt.Sprintf("[%s] %s", logType, line)
	}
	return fmt.Sprintf("%s [%s] %s", ts, logType, line)
}

func parseLogTimestampMillis(value string) int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		return 0
	}
	return parsed.UnixMilli()
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func getenvDurationSeconds(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			return time.Duration(parsed) * time.Second
		}
	}
	return fallback
}

func maskEmail(email string) string {
	email = strings.TrimSpace(email)
	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return "***"
	}
	local := parts[0]
	domain := parts[1]
	if local == "" || domain == "" {
		return "***"
	}
	if len(local) <= 3 {
		return "***@" + domain
	}
	return local[:3] + "***@" + domain
}

type commandLogger struct {
	manager *manager
	prefix  string
	buf     bytes.Buffer
	mu      sync.Mutex
}

func newCommandLogger(m *manager, prefix string) io.Writer {
	return &commandLogger{
		manager: m,
		prefix:  prefix,
	}
}

func (l *commandLogger) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	total := len(p)
	if total == 0 {
		return 0, nil
	}
	l.buf.Write(p)
	for {
		data := l.buf.Bytes()
		idx := bytes.IndexByte(data, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimSpace(string(data[:idx]))
		l.buf.Next(idx + 1)
		if line == "" {
			continue
		}
		if l.prefix == "cloudflared" {
			l.manager.handleCloudflaredLogLine(line)
		}
		l.manager.logInfo("cloudflared_stream line=%s prefix=%s", line, l.prefix)
	}
	return total, nil
}

func (m *manager) handleCloudflaredLogLine(line string) {
	if strings.Contains(line, "Registered tunnel connection") {
		m.setTunnelReady(true)
	}
}

func (m *manager) setTunnelReady(ready bool) {
	previous := m.tunnelReady.Swap(ready)
	if previous == ready {
		return
	}
	if ready {
		if strings.TrimSpace(m.tunnelID) != "" {
			m.logInfo("cloudflared_tunnel_ready tunnel_id=%s", m.tunnelID)
		} else {
			m.logInfo("cloudflared_tunnel_ready")
		}
	} else {
		m.logInfo("cloudflared_tunnel_not_ready")
	}
	go m.sendTunnelStateHeartbeat()
}

func (m *manager) sendTunnelStateHeartbeat() {
	m.mu.Lock()
	managerID := strings.TrimSpace(m.state.ManagerID)
	m.mu.Unlock()
	if managerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.sendHeartbeat(ctx); err != nil {
		m.logError("tunnel_state_heartbeat_error err=%v", err)
	}
}

func readCloudflareTunnelID(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("cloudflare tunnel name is empty")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	path := filepath.Join(home, ".cloudflared", name+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var creds struct {
		TunnelID string `json:"TunnelID"`
	}
	if err := json.Unmarshal(raw, &creds); err != nil {
		return "", err
	}
	if strings.TrimSpace(creds.TunnelID) == "" {
		return "", fmt.Errorf("missing TunnelID in %s", path)
	}
	return strings.TrimSpace(creds.TunnelID), nil
}

func parseTunnelIDFromToken(token string) string {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return ""
	}
	var payload struct {
		TunnelID string `json:"t"`
	}
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.TunnelID)
}

func calculateDynamicMaxConcurrent(systemReserveMB, mbPerAgent float64) int {
	vm, err := mem.VirtualMemory()
	totalMB := 0.0
	if err == nil && vm != nil {
		totalMB = float64(vm.Total) / 1024 / 1024
	}
	return calculateAutomaticMaxConcurrentForSystem(
		runtime.NumCPU(),
		totalMB,
		systemReserveMB,
		mbPerAgent,
	)
}

func calculateAutomaticMaxConcurrentForSystem(cores int, totalMB, systemReserveMB, mbPerAgent float64) int {
	if cores <= 0 {
		cores = 1
	}
	if mbPerAgent <= 0 {
		mbPerAgent = 1024
	}
	if systemReserveMB < 0 {
		systemReserveMB = 0
	}

	cpuCap := int(math.Floor(float64(cores) * 0.5))
	if cpuCap < 1 {
		cpuCap = 1
	}

	if totalMB <= 0 {
		return cpuCap
	}
	usableRAMMB := minFloat64(
		float64(ramUtilizationCapPct)/100.0*totalMB,
		totalMB-systemReserveMB,
	)
	if usableRAMMB < mbPerAgent {
		return 1
	}
	memCap := int(math.Floor(usableRAMMB / mbPerAgent))
	if memCap < 1 {
		memCap = 1
	}

	return maxInt(1, minInt(cpuCap, memCap))
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func readManagerPIDFromState(statePath string) (int, error) {
	state, err := readState(statePath)
	if err != nil {
		return 0, err
	}
	if state.ManagerPID <= 0 {
		return 0, os.ErrNotExist
	}
	return state.ManagerPID, nil
}

func writeManagerPIDToState(statePath string, pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid")
	}
	state, err := readState(statePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		state = persistedState{Instances: map[string]persistedWorker{}}
	}
	state.ManagerPID = pid
	state.LegacyDaemonPID = 0
	return writeState(statePath, state)
}

func clearManagerPIDInState(statePath string, expectedPID int) error {
	state, err := readState(statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if expectedPID > 0 && state.ManagerPID != expectedPID {
		return nil
	}
	state.ManagerPID = 0
	state.LegacyDaemonPID = 0
	return writeState(statePath, state)
}

func persistSupabaseConfigInState(statePath, supabaseURL, supabaseAnonKey string) error {
	state, err := readState(statePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		state = persistedState{Instances: map[string]persistedWorker{}}
	}

	urlValue := strings.TrimRight(strings.TrimSpace(supabaseURL), "/")
	keyValue := strings.TrimSpace(supabaseAnonKey)
	if urlValue == "" || keyValue == "" {
		return nil
	}

	if state.SupabaseURL == urlValue && state.SupabaseAnonKey == keyValue {
		return nil
	}
	state.SupabaseURL = urlValue
	state.SupabaseAnonKey = keyValue
	return writeState(statePath, state)
}

func isProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func terminateProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	return signalManagedPID(pid, syscall.SIGTERM)
}

func signalManagedPID(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		if sig == syscall.SIGKILL {
			return proc.Kill()
		}
		return proc.Signal(sig)
	}
	if pgid, pgErr := syscall.Getpgid(pid); pgErr == nil && pgid == pid {
		if err := syscall.Kill(-pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	if err := proc.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

func killManagedPID(pid int) error {
	return signalManagedPID(pid, syscall.SIGKILL)
}

func waitForProcessExit(pid int, timeout time.Duration) bool {
	if pid <= 0 {
		return true
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !isProcessRunning(pid) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return !isProcessRunning(pid)
}

func tryOpenBrowser(targetURL string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", targetURL)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", targetURL)
	default:
		cmd = exec.Command("xdg-open", targetURL)
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func shouldAttemptBrowserOpen() bool {
	if runtime.GOOS != "linux" {
		return true
	}
	hasDisplay := strings.TrimSpace(os.Getenv("DISPLAY")) != ""
	hasWayland := strings.TrimSpace(os.Getenv("WAYLAND_DISPLAY")) != ""
	if !(hasDisplay || hasWayland) {
		return false
	}
	return hasDBusSession()
}

func hasDBusSession() bool {
	if addr := strings.TrimSpace(os.Getenv("DBUS_SESSION_BUS_ADDRESS")); addr != "" {
		if path, ok := dbusSocketFromAddress(addr); ok {
			if err := tryDialUnix(path); err == nil {
				return true
			}
		}
	}
	if path, err := dbusSocketPath(); err == nil {
		if err := tryDialUnix(path); err == nil {
			return true
		}
	}
	return false
}

func dbusSocketPath() (string, error) {
	dir := fmt.Sprintf("/run/user/%d", os.Getuid())
	bus := filepath.Join(dir, "bus")
	if _, err := os.Stat(bus); err != nil {
		return "", err
	}
	return bus, nil
}

func dbusSocketFromAddress(addr string) (string, bool) {
	for _, part := range strings.Split(addr, ";") {
		p := strings.TrimSpace(part)
		if strings.HasPrefix(p, "unix:path=") {
			return strings.TrimPrefix(p, "unix:path="), true
		}
	}
	return "", false
}

func tryDialUnix(path string) error {
	conn, err := net.DialTimeout("unix", path, 150*time.Millisecond)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

func keyringUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "org.freedesktop.secrets") ||
		strings.Contains(msg, "secret service") ||
		strings.Contains(msg, "keyring is not available") ||
		(managerRuntimeGOOS() == "darwin" && (strings.Contains(msg, "user interaction is not allowed") ||
			strings.Contains(msg, "keychain is locked") ||
			strings.Contains(msg, "interaction with the security server is no longer valid"))) ||
		(strings.Contains(msg, "dial unix") && strings.Contains(msg, "/run/user/") && strings.Contains(msg, "/bus")) ||
		(strings.Contains(msg, "connect: no such file or directory") && strings.Contains(msg, "/bus"))
}

func decodeStoredSession(raw []byte) (storedSession, error) {
	var session storedSession
	if err := json.Unmarshal(raw, &session); err != nil {
		return storedSession{}, err
	}
	if session.ExpiresAt == 0 {
		session.ExpiresAt = parseJWTExpiry(session.AccessToken)
	}
	return session, nil
}

func sessionFilePath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".passiveagents", authFileName), nil
}

func writeFallbackSession(raw []byte) error {
	path, err := sessionFilePath()
	if err != nil {
		return err
	}
	return writeFileAtomic(path, raw, 0o600, 0o700)
}

func readFallbackSession() ([]byte, error) {
	path, err := sessionFilePath()
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func writeFileAtomic(path string, content []byte, filePerm os.FileMode, dirPerm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(content); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Chmod(filePerm); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func generateLoginState() (string, error) {
	b := make([]byte, 24)
	if _, err := cryptorand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate oauth state: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func generatePKCEPair() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := cryptorand.Read(raw); err != nil {
		return "", "", fmt.Errorf("failed to generate pkce verifier: %w", err)
	}
	verifier := base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	return verifier, challenge, nil
}

func supabaseProjectRef(supabaseURL string) (string, error) {
	parsed, err := url.Parse(supabaseURL)
	if err != nil {
		return "", err
	}
	host := parsed.Hostname()
	if host == "" {
		return "", fmt.Errorf("invalid supabase url")
	}
	parts := strings.Split(host, ".")
	if len(parts) < 1 || parts[0] == "" {
		return "", fmt.Errorf("invalid supabase project host")
	}
	return parts[0], nil
}

func newRuntimeCommand(command string) *exec.Cmd {
	command = wrapRuntimeCommand(command)
	if runtime.GOOS == "windows" {
		return exec.Command("cmd", "/C", command)
	}
	cmd := exec.Command("sh", "-lc", command)
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	return cmd
}

func shouldRetryPTYWithoutSetctty(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
		return true
	}
	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "operation not permitted") || strings.Contains(errText, "permission denied")
}

func wrapRuntimeCommand(command string) string {
	command = strings.TrimSpace(command)
	if command == "" || !shouldPrintWorkingDirForRuntime(command) {
		return command
	}
	if runtime.GOOS == "windows" {
		return "cd && " + command
	}
	return "pwd; exec " + command
}

func shouldPrintWorkingDirForRuntime(command string) bool {
	return isGeminiRuntimeCommand(command)
}

func supportsBootstrapSubmissionObservation(command string) bool {
	return isGeminiRuntimeCommand(command) || isCodexLikeRuntimeCommand(command)
}

func requiresPTYRuntime(command string) bool {
	return isGeminiRuntimeCommand(command) || isCodexLikeRuntimeCommand(command)
}

func runtimeDisplayName(command string) string {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) == 0 {
		return "runtime"
	}
	return fields[0]
}

func isGeminiRuntimeCommand(command string) bool {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) == 0 {
		return false
	}
	return strings.EqualFold(fields[0], "gemini")
}

func isCodexRuntimeCommand(command string) bool {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) == 0 {
		return false
	}
	return strings.EqualFold(fields[0], "codex")
}

func isOpenCodeRuntimeCommand(command string) bool {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) == 0 {
		return false
	}
	return strings.EqualFold(fields[0], "opencode")
}

func isClaudeRuntimeCommand(command string) bool {
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) == 0 {
		return false
	}
	return strings.EqualFold(fields[0], "claude")
}

func isCodexLikeRuntimeCommand(command string) bool {
	return isCodexRuntimeCommand(command) || isOpenCodeRuntimeCommand(command) || isClaudeRuntimeCommand(command)
}

func (m *manager) newAuthClient() auth.Client {
	projectRef := "passiveagents"
	if ref, err := supabaseProjectRef(m.cfg.SupabaseURL); err == nil && strings.TrimSpace(ref) != "" {
		projectRef = ref
	}
	return auth.New(projectRef, m.cfg.SupabaseAnonKey).WithCustomAuthURL(
		strings.TrimSuffix(m.cfg.SupabaseURL, "/") + "/auth/v1",
	)
}

func getLocalIPs() ([]string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var ips []string
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}
			ips = append(ips, ip.String())
		}
	}
	return ips, nil
}
