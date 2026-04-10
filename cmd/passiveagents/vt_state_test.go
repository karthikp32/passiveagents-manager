//go:build unix
// +build unix

package main

import (
	"strings"
	"testing"
)

func TestVTScreenStateSnapshotPreservesVisibleContentAndCursor(t *testing.T) {
	state := newVTScreenState(12, 4)
	state.Write("Hello\r\nWorld")

	snapshot := state.SnapshotANSI()
	if !strings.Contains(snapshot, "\x1b[1;1HHello") {
		t.Fatalf("snapshot missing first row content: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[2;1HWorld") {
		t.Fatalf("snapshot missing visible content: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[2;6H") {
		t.Fatalf("snapshot missing expected cursor position: %q", snapshot)
	}
}

func TestVTScreenStateHandlesCursorMotion(t *testing.T) {
	state := newVTScreenState(12, 3)
	state.Write("Hello\x1b[1GJ")

	snapshot := state.SnapshotANSI()
	if !strings.Contains(snapshot, "Jello") {
		t.Fatalf("snapshot missing overwritten line: %q", snapshot)
	}
}

func TestVTScreenStateTracksAlternateScreen(t *testing.T) {
	state := newVTScreenState(12, 3)
	state.Write("main")
	state.Write("\x1b[?1049halt")

	altSnapshot := state.SnapshotANSI()
	if !strings.Contains(altSnapshot, "\x1b[?1049h") {
		t.Fatalf("alternate screen snapshot missing enter sequence: %q", altSnapshot)
	}
	if !strings.Contains(altSnapshot, "alt") || strings.Contains(altSnapshot, "main") {
		t.Fatalf("alternate screen snapshot has unexpected content: %q", altSnapshot)
	}

	state.Write("\x1b[?1049l")
	primarySnapshot := state.SnapshotANSI()
	if strings.Contains(primarySnapshot, "\x1b[?1049h") {
		t.Fatalf("primary snapshot should not re-enter alternate screen: %q", primarySnapshot)
	}
	if !strings.Contains(primarySnapshot, "main") {
		t.Fatalf("primary snapshot missing restored content: %q", primarySnapshot)
	}
}

func TestVTScreenStateRewritesStatusLinesWithoutDuplicatingContent(t *testing.T) {
	state := newVTScreenState(48, 3)
	state.Write("old status")
	state.Write("\x1b[1G\x1b[2K⠋ thinking...")
	state.Write("\x1b[1G\x1b[2K⠙ thinking...")

	snapshot := state.SnapshotANSI()
	if strings.Contains(snapshot, "old status") {
		t.Fatalf("snapshot retained stale status content: %q", snapshot)
	}
	if strings.Count(snapshot, "thinking...") != 1 {
		t.Fatalf("snapshot duplicated rewritten status content: %q", snapshot)
	}
}

func TestVTScreenStateSnapshotUsesAbsoluteRowsForFullWidthContent(t *testing.T) {
	state := newVTScreenState(5, 2)
	state.Write("-----")
	state.Write("\x1b[2;1Hnext")

	snapshot := state.SnapshotANSI()
	if strings.Contains(snapshot, "-----\r\n") {
		t.Fatalf("snapshot should not replay rows via CRLF for full-width content: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[1;1H-----") {
		t.Fatalf("snapshot missing full-width first row: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[2;1Hnext") {
		t.Fatalf("snapshot missing absolute-positioned second row: %q", snapshot)
	}
}

func TestVTScreenStateDeleteLinesHonorsScrollRegion(t *testing.T) {
	state := newVTScreenState(5, 4)
	state.Write("\x1b[1;1H111")
	state.Write("\x1b[2;1H222")
	state.Write("\x1b[3;1H333")
	state.Write("\x1b[4;1H444")
	state.Write("\x1b[2;3r")
	state.Write("\x1b[2;1H")
	state.Write("\x1b[M")

	snapshot := state.SnapshotANSI()
	if !strings.Contains(snapshot, "\x1b[1;1H111") {
		t.Fatalf("snapshot changed content above scroll region: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[2;1H333") {
		t.Fatalf("snapshot did not pull next line up within scroll region: %q", snapshot)
	}
	if strings.Contains(snapshot, "\x1b[4;1H   ") {
		t.Fatalf("snapshot should not blank content below scroll region: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[4;1H444") {
		t.Fatalf("snapshot changed content below scroll region: %q", snapshot)
	}
}

func TestVTScreenStateInsertLinesHonorsScrollRegion(t *testing.T) {
	state := newVTScreenState(5, 4)
	state.Write("\x1b[1;1H111")
	state.Write("\x1b[2;1H222")
	state.Write("\x1b[3;1H333")
	state.Write("\x1b[4;1H444")
	state.Write("\x1b[2;3r")
	state.Write("\x1b[2;1H")
	state.Write("\x1b[L")

	snapshot := state.SnapshotANSI()
	if !strings.Contains(snapshot, "\x1b[1;1H111") {
		t.Fatalf("snapshot changed content above scroll region: %q", snapshot)
	}
	if strings.Contains(snapshot, "\x1b[4;1H333") {
		t.Fatalf("snapshot pushed content below scroll region: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[3;1H222") {
		t.Fatalf("snapshot did not shift content down within scroll region: %q", snapshot)
	}
	if !strings.Contains(snapshot, "\x1b[4;1H444") {
		t.Fatalf("snapshot changed content below scroll region: %q", snapshot)
	}
}
