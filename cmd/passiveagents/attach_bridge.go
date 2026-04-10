//go:build unix
// +build unix

package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/creack/pty"
	"golang.org/x/term"
)

const attachHandshakePrefix = "PA_SIZE"

func normalizeRelayErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, io.EOF):
		return nil
	case errors.Is(err, net.ErrClosed):
		return nil
	default:
		return err
	}
}

// AttachInteractive connects local terminal stdin/stdout to a running manager attach socket.
// It enters raw mode to preserve TUI behavior and special key sequences.
func AttachInteractive(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial attach bridge: %w", err)
	}
	defer conn.Close()

	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return fmt.Errorf("stdin is not a terminal")
	}
	if err := sendAttachSizeHandshake(conn); err != nil {
		return err
	}

	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return fmt.Errorf("set raw mode: %w", err)
	}
	defer func() {
		_ = term.Restore(int(os.Stdin.Fd()), oldState)
	}()

	copyErr := make(chan error, 2)
	go func() {
		err := copyInputUntilDetach(conn, os.Stdin)
		_ = conn.Close()
		copyErr <- normalizeRelayErr(err)
	}()
	go func() {
		_, err := io.Copy(os.Stdout, conn)
		copyErr <- normalizeRelayErr(err)
	}()

	err = <-copyErr
	<-copyErr
	return err
}

func copyInputUntilDetach(dst io.Writer, src io.Reader) error {
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			for i, b := range chunk {
				if b == 0x03 { // Ctrl+C in raw mode: detach locally, do not forward to remote PTY.
					if i > 0 {
						if _, werr := dst.Write(chunk[:i]); werr != nil {
							return werr
						}
					}
					return nil
				}
			}
			if _, werr := dst.Write(chunk); werr != nil {
				return werr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func sendAttachSizeHandshake(conn net.Conn) error {
	fd := int(os.Stdout.Fd())
	if !term.IsTerminal(fd) {
		fd = int(os.Stdin.Fd())
	}
	cols, rows, err := term.GetSize(fd)
	if err != nil || cols <= 0 || rows <= 0 {
		return nil
	}
	if _, err := fmt.Fprintf(conn, "%s %d %d\n", attachHandshakePrefix, cols, rows); err != nil {
		return fmt.Errorf("send attach size handshake: %w", err)
	}
	return nil
}

// SyncInitialPTYSize sets PTY dimensions from the current terminal during attach.
// Preferred path uses pty.InheritSize; fallback shows manual term.GetSize + pty.Setsize.
func SyncInitialPTYSize(ptmx *os.File, tty *os.File) error {
	if err := pty.InheritSize(tty, ptmx); err == nil {
		return nil
	}

	w, h, err := term.GetSize(int(tty.Fd()))
	if err != nil {
		return err
	}
	return pty.Setsize(ptmx, &pty.Winsize{
		Cols: uint16(w),
		Rows: uint16(h),
	})
}
