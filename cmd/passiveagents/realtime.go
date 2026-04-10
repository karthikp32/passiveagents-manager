//go:build unix
// +build unix

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const maxRealtimeReconnectAttempts = 5

type realtimeSubscription struct {
	filter string
	ch     chan map[string]any
}

type RealtimeClient struct {
	supabaseURL          string
	apiKey               string
	tokenRefresh         func() (string, error)
	accessToken          string
	conn                 *websocket.Conn
	mu                   sync.Mutex
	subs                 map[string]realtimeSubscription
	ref                  int
	heart                chan struct{}
	closeOnce            sync.Once
	maxReconnectAttempts int
}

func NewRealtimeClient(supabaseURL, apiKey string, tokenRefresh func() (string, error)) (*RealtimeClient, error) {
	if tokenRefresh == nil {
		return nil, fmt.Errorf("token refresh callback is required")
	}

	rc := &RealtimeClient{
		supabaseURL:          supabaseURL,
		apiKey:               apiKey,
		tokenRefresh:         tokenRefresh,
		subs:                 make(map[string]realtimeSubscription),
		heart:                make(chan struct{}),
		maxReconnectAttempts: maxRealtimeReconnectAttempts,
	}

	conn, accessToken, err := rc.connect()
	if err != nil {
		return nil, err
	}
	rc.accessToken = accessToken
	rc.conn = conn

	go rc.readLoop()
	go rc.heartbeatLoop()

	return rc, nil
}

func (rc *RealtimeClient) connect() (*websocket.Conn, string, error) {
	authToken, err := rc.tokenRefresh()
	if err != nil {
		return nil, "", fmt.Errorf("failed to refresh realtime auth token: %w", err)
	}

	wsURL := strings.Replace(rc.supabaseURL, "http", "ws", 1)
	wsURL = strings.TrimSuffix(wsURL, "/") + "/realtime/v1/websocket?apikey=" + url.QueryEscape(rc.apiKey) + "&vsn=1.0.0"

	header := http.Header{}
	header.Set("apikey", rc.apiKey)

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			_ = resp.Body.Close()
			bodyText := strings.TrimSpace(string(body))
			if bodyText != "" {
				quotedBodyText := strconv.QuoteToASCII(bodyText)
				return nil, "", fmt.Errorf(
					"failed to dial realtime endpoint: status=%d body=%s err=%w",
					resp.StatusCode,
					quotedBodyText,
					err,
				)
			}
			return nil, "", fmt.Errorf("failed to dial realtime endpoint: status=%d err=%w", resp.StatusCode, err)
		}
		return nil, "", fmt.Errorf("failed to dial realtime endpoint: %w", err)
	}
	return conn, authToken, nil
}

func (rc *RealtimeClient) Close() {
	rc.closeOnce.Do(func() {
		rc.mu.Lock()
		defer rc.mu.Unlock()
		if rc.conn != nil {
			_ = rc.conn.Close()
			rc.conn = nil
		}
		close(rc.heart)
		for topic, sub := range rc.subs {
			close(sub.ch)
			delete(rc.subs, topic)
		}
		rc.subs = nil
	})
}

func (rc *RealtimeClient) nextRefLocked() string {
	rc.ref++
	return fmt.Sprintf("%d", rc.ref)
}

func (rc *RealtimeClient) Subscribe(topic, filter string) (<-chan map[string]any, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.subs == nil {
		return nil, fmt.Errorf("connection closed")
	}

	ch := make(chan map[string]any, 100)
	rc.subs[topic] = realtimeSubscription{filter: filter, ch: ch}

	if rc.conn == nil {
		delete(rc.subs, topic)
		close(ch)
		return nil, fmt.Errorf("connection closed")
	}

	if err := rc.sendJoinLocked(rc.conn, topic, filter); err != nil {
		delete(rc.subs, topic)
		close(ch)
		return nil, err
	}

	return ch, nil
}

func (rc *RealtimeClient) sendJoinLocked(conn *websocket.Conn, topic, filter string) error {
	parts := strings.Split(topic, ":")
	if len(parts) < 3 {
		return fmt.Errorf("invalid realtime topic: %s", topic)
	}

	joinMsg := map[string]any{
		"topic": topic,
		"event": "phx_join",
		"payload": map[string]any{
			"config": map[string]any{
				"broadcast": map[string]any{
					"ack":  false,
					"self": false,
				},
				"presence": map[string]any{
					"enabled": false,
				},
				"postgres_changes": []map[string]any{
					{
						"event":  "INSERT",
						"schema": "public",
						"table":  parts[2], // expects topic like realtime:public:agent_commands
						"filter": filter,
					},
				},
				"private": false,
			},
			"access_token": rc.accessToken,
		},
		"ref": rc.nextRefLocked(),
	}

	return conn.WriteJSON(joinMsg)
}

func (rc *RealtimeClient) readLoop() {
	for {
		rc.mu.Lock()
		conn := rc.conn
		rc.mu.Unlock()
		if conn == nil {
			return
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			if rc.reconnect(err) {
				continue
			}
			rc.Close()
			return
		}

		var payload map[string]any
		if err := json.Unmarshal(msg, &payload); err != nil {
			continue
		}

		topic, _ := payload["topic"].(string)
		event, _ := payload["event"].(string)

		if event == "postgres_changes" {
			innerPayload, ok := payload["payload"].(map[string]any)
			if !ok {
				continue
			}

			rc.mu.Lock()
			sub, exists := rc.subs[topic]
			if exists {
				select {
				case sub.ch <- innerPayload:
				default:
					log.Printf("realtime message dropped topic=%s reason=subscription_buffer_full", topic)
				}
			}
			rc.mu.Unlock()
		}
	}
}

func (rc *RealtimeClient) reconnect(readErr error) bool {
	lastErr := readErr
	for attempt := 1; attempt <= rc.maxReconnectAttempts; attempt++ {
		select {
		case <-rc.heart:
			return false
		default:
		}

		conn, accessToken, err := rc.connect()
		if err == nil {
			if err = rc.activateConnection(conn, accessToken); err == nil {
				log.Printf("realtime reconnected attempts=%d", attempt)
				return true
			}
			_ = conn.Close()
		}
		if err != nil {
			lastErr = err
		}

		if attempt == rc.maxReconnectAttempts {
			break
		}

		delay := time.Second << (attempt - 1)
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}

		timer := time.NewTimer(delay)
		select {
		case <-rc.heart:
			timer.Stop()
			return false
		case <-timer.C:
		}
	}

	rc.broadcastStructuredError("reconnect_failed", fmt.Sprintf("realtime reconnect failed: %v", lastErr), rc.maxReconnectAttempts)
	return false
}

func (rc *RealtimeClient) activateConnection(conn *websocket.Conn, accessToken string) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.subs == nil {
		return fmt.Errorf("connection closed")
	}

	rc.accessToken = accessToken

	for topic, sub := range rc.subs {
		if err := rc.sendJoinLocked(conn, topic, sub.filter); err != nil {
			return err
		}
	}

	if rc.conn != nil {
		_ = rc.conn.Close()
	}
	rc.conn = conn
	return nil
}

func (rc *RealtimeClient) broadcastStructuredError(code, message string, attempts int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for topic, sub := range rc.subs {
		payload := map[string]any{
			"kind": "realtime_error",
			"error": map[string]any{
				"code":     code,
				"message":  message,
				"attempts": attempts,
			},
			"topic": topic,
		}
		select {
		case sub.ch <- payload:
		default:
			log.Printf("realtime error dropped topic=%s reason=subscription_buffer_full", topic)
		}
	}
}

func (rc *RealtimeClient) heartbeatLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rc.heart:
			return
		case <-ticker.C:
			rc.mu.Lock()
			if rc.conn != nil {
				_ = rc.conn.WriteJSON(map[string]any{
					"topic":   "phoenix",
					"event":   "heartbeat",
					"payload": map[string]any{},
					"ref":     rc.nextRefLocked(),
				})
			}
			rc.mu.Unlock()
		}
	}
}
