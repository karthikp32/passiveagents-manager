//go:build unix
// +build unix

package main

import (
	"bytes"
	"io"
	"net"
	"testing"
)

func TestCopyInputUntilDetachStopsOnCtrlC(t *testing.T) {
	var out bytes.Buffer
	in := bytes.NewReader([]byte("hello\x03world"))

	if err := copyInputUntilDetach(&out, in); err != nil {
		t.Fatalf("copyInputUntilDetach error: %v", err)
	}
	if got := out.String(); got != "hello" {
		t.Fatalf("unexpected output: got=%q want=%q", got, "hello")
	}
}

func TestNormalizeRelayErr(t *testing.T) {
	if err := normalizeRelayErr(nil); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if err := normalizeRelayErr(io.EOF); err != nil {
		t.Fatalf("expected nil for EOF, got %v", err)
	}
	if err := normalizeRelayErr(net.ErrClosed); err != nil {
		t.Fatalf("expected nil for closed network, got %v", err)
	}
}
