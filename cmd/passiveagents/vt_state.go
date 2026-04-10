//go:build unix
// +build unix

package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

type vtScreenState struct {
	mu           sync.Mutex
	width        int
	height       int
	cursorX      int
	cursorY      int
	savedX       int
	savedY       int
	showCursor   bool
	altScreen    bool
	scrollTop    int
	scrollBottom int
	primary      [][]rune
	alternate    [][]rune
	pending      []byte
}

func newVTScreenState(width, height int) *vtScreenState {
	width, height = normalizeVTSize(width, height)
	return &vtScreenState{
		width:        width,
		height:       height,
		showCursor:   true,
		scrollTop:    0,
		scrollBottom: height - 1,
		primary:      newVTBuffer(width, height),
		alternate:    newVTBuffer(width, height),
	}
}

func normalizeVTSize(width, height int) (int, int) {
	if width <= 0 {
		width = 80
	}
	if height <= 0 {
		height = 24
	}
	return width, height
}

func newVTBuffer(width, height int) [][]rune {
	buf := make([][]rune, height)
	for y := 0; y < height; y++ {
		line := make([]rune, width)
		for x := 0; x < width; x++ {
			line[x] = ' '
		}
		buf[y] = line
	}
	return buf
}

func resizeVTBuffer(src [][]rune, width, height int) [][]rune {
	dst := newVTBuffer(width, height)
	copyHeight := min(height, len(src))
	for y := 0; y < copyHeight; y++ {
		copy(dst[y], src[y][:min(width, len(src[y]))])
	}
	return dst
}

func (s *vtScreenState) Resize(width, height int) {
	width, height = normalizeVTSize(width, height)
	s.mu.Lock()
	defer s.mu.Unlock()

	if width == s.width && height == s.height {
		return
	}

	s.primary = resizeVTBuffer(s.primary, width, height)
	s.alternate = resizeVTBuffer(s.alternate, width, height)
	s.width = width
	s.height = height
	s.scrollTop = 0
	s.scrollBottom = height - 1
	s.cursorX = min(max(s.cursorX, 0), width-1)
	s.cursorY = min(max(s.cursorY, 0), height-1)
	s.savedX = min(max(s.savedX, 0), width-1)
	s.savedY = min(max(s.savedY, 0), height-1)
}

func (s *vtScreenState) Write(chunk string) {
	if chunk == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	data := append(append([]byte(nil), s.pending...), []byte(chunk)...)
	s.pending = s.pending[:0]

	for i := 0; i < len(data); {
		if data[i] == 0x1b {
			n, complete := s.consumeEscape(data[i:])
			if !complete {
				s.pending = append(s.pending, data[i:]...)
				break
			}
			i += n
			continue
		}

		if data[i] < 0x20 || data[i] == 0x7f {
			s.handleControl(data[i])
			i++
			continue
		}

		r, size := utf8.DecodeRune(data[i:])
		if r == utf8.RuneError && size == 1 {
			s.pending = append(s.pending, data[i:]...)
			break
		}
		s.putRune(r)
		i += size
	}
}

func (s *vtScreenState) SnapshotANSI() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out strings.Builder
	if s.altScreen {
		out.WriteString("\x1b[?1049h")
	}
	out.WriteString("\x1b[2J\x1b[H")

	active := s.activeBuffer()
	for y := 0; y < s.height; y++ {
		line := trimVTLineRight(active[y])
		if line == "" {
			continue
		}
		out.WriteString(fmt.Sprintf("\x1b[%d;1H", y+1))
		out.WriteString(line)
	}

	out.WriteString(fmt.Sprintf("\x1b[%d;%dH", s.cursorY+1, s.cursorX+1))
	if s.showCursor {
		out.WriteString("\x1b[?25h")
	} else {
		out.WriteString("\x1b[?25l")
	}
	return out.String()
}

func (s *vtScreenState) activeBuffer() [][]rune {
	if s.altScreen {
		return s.alternate
	}
	return s.primary
}

func trimVTLineRight(line []rune) string {
	end := len(line)
	for end > 0 && line[end-1] == ' ' {
		end--
	}
	return string(line[:end])
}

func (s *vtScreenState) handleControl(b byte) {
	switch b {
	case '\r':
		s.cursorX = 0
	case '\n':
		s.lineFeed()
	case '\b':
		if s.cursorX > 0 {
			s.cursorX--
		}
	case '\t':
		tabStop := ((s.cursorX / 8) + 1) * 8
		s.cursorX = min(tabStop, max(s.width-1, 0))
	}
}

func (s *vtScreenState) consumeEscape(data []byte) (int, bool) {
	if len(data) < 2 {
		return 0, false
	}

	switch data[1] {
	case '[':
		for i := 2; i < len(data); i++ {
			if data[i] >= 0x40 && data[i] <= 0x7e {
				s.handleCSI(string(data[2:i]), data[i])
				return i + 1, true
			}
		}
		return 0, false
	case ']':
		for i := 2; i < len(data); i++ {
			if data[i] == 0x07 {
				return i + 1, true
			}
			if data[i] == 0x1b && i+1 < len(data) && data[i+1] == '\\' {
				return i + 2, true
			}
		}
		return 0, false
	case '7':
		s.savedX, s.savedY = s.cursorX, s.cursorY
		return 2, true
	case '8':
		s.cursorX, s.cursorY = s.savedX, s.savedY
		s.clampCursor()
		return 2, true
	case 'D':
		s.lineFeed()
		return 2, true
	case 'E':
		s.cursorX = 0
		s.lineFeed()
		return 2, true
	case 'M':
		s.reverseIndex()
		return 2, true
	case 'c':
		s.reset()
		return 2, true
	case '(', ')', '*', '+', '-', '.', '/':
		if len(data) < 3 {
			return 0, false
		}
		return 3, true
	default:
		return 2, true
	}
}

func (s *vtScreenState) handleCSI(paramString string, final byte) {
	private := false
	if strings.HasPrefix(paramString, "?") {
		private = true
		paramString = strings.TrimPrefix(paramString, "?")
	}
	params := parseCSIParams(paramString)

	switch final {
	case 'A':
		s.cursorY -= csiParamOr(params, 0, 1)
	case 'B':
		s.cursorY += csiParamOr(params, 0, 1)
	case 'C':
		s.cursorX += csiParamOr(params, 0, 1)
	case 'D':
		s.cursorX -= csiParamOr(params, 0, 1)
	case 'E':
		s.cursorY += csiParamOr(params, 0, 1)
		s.cursorX = 0
	case 'F':
		s.cursorY -= csiParamOr(params, 0, 1)
		s.cursorX = 0
	case 'G':
		s.cursorX = csiParamOr(params, 0, 1) - 1
	case 'H', 'f':
		s.cursorY = csiParamOr(params, 0, 1) - 1
		s.cursorX = csiParamOr(params, 1, 1) - 1
	case 'J':
		s.eraseDisplay(csiParamOr(params, 0, 0))
	case 'K':
		s.eraseLine(csiParamOr(params, 0, 0))
	case 'L':
		s.insertLines(csiParamOr(params, 0, 1))
	case 'M':
		s.deleteLines(csiParamOr(params, 0, 1))
	case '@':
		s.insertChars(csiParamOr(params, 0, 1))
	case 'P':
		s.deleteChars(csiParamOr(params, 0, 1))
	case 'X':
		s.eraseChars(csiParamOr(params, 0, 1))
	case 'm':
		// Ignore styles in the screen-state snapshot for now.
	case 'r':
		top := csiParamOr(params, 0, 1) - 1
		bottom := csiParamOr(params, 1, s.height) - 1
		if top < 0 {
			top = 0
		}
		if bottom >= s.height {
			bottom = s.height - 1
		}
		if top < bottom {
			s.scrollTop = top
			s.scrollBottom = bottom
			s.cursorX, s.cursorY = 0, 0
		}
	case 's':
		s.savedX, s.savedY = s.cursorX, s.cursorY
	case 'u':
		s.cursorX, s.cursorY = s.savedX, s.savedY
	case 'h', 'l':
		if private {
			s.handlePrivateMode(params, final == 'h')
		}
	}

	s.clampCursor()
}

func parseCSIParams(paramString string) []int {
	if paramString == "" {
		return nil
	}
	parts := strings.Split(paramString, ";")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			out = append(out, 0)
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			out = append(out, 0)
			continue
		}
		out = append(out, value)
	}
	return out
}

func csiParamOr(params []int, index, fallback int) int {
	if index < 0 || index >= len(params) || params[index] == 0 {
		return fallback
	}
	return params[index]
}

func (s *vtScreenState) handlePrivateMode(params []int, set bool) {
	for _, param := range params {
		switch param {
		case 25:
			s.showCursor = set
		case 1047, 1049:
			if set {
				if param == 1049 {
					s.savedX, s.savedY = s.cursorX, s.cursorY
				}
				s.altScreen = true
				s.alternate = newVTBuffer(s.width, s.height)
				s.cursorX, s.cursorY = 0, 0
			} else {
				s.altScreen = false
				if param == 1049 {
					s.cursorX, s.cursorY = s.savedX, s.savedY
				}
			}
		case 1048:
			if set {
				s.savedX, s.savedY = s.cursorX, s.cursorY
			} else {
				s.cursorX, s.cursorY = s.savedX, s.savedY
			}
		}
	}
}

func (s *vtScreenState) putRune(r rune) {
	if s.width <= 0 || s.height <= 0 {
		return
	}
	if s.cursorX >= s.width {
		s.cursorX = 0
		s.lineFeed()
	}
	if s.cursorY >= s.height {
		s.scrollUp(1)
		s.cursorY = s.height - 1
	}

	active := s.activeBuffer()
	active[s.cursorY][s.cursorX] = r
	s.cursorX++
	if s.cursorX >= s.width {
		s.cursorX = 0
		s.lineFeed()
	}
}

func (s *vtScreenState) lineFeed() {
	if s.cursorY == s.scrollBottom {
		s.scrollUp(1)
	} else {
		s.cursorY++
		if s.cursorY >= s.height {
			s.cursorY = s.height - 1
		}
	}
}

func (s *vtScreenState) reverseIndex() {
	if s.cursorY == s.scrollTop {
		s.scrollDown(1)
	} else {
		s.cursorY--
		if s.cursorY < 0 {
			s.cursorY = 0
		}
	}
}

func (s *vtScreenState) scrollUp(count int) {
	if count <= 0 {
		return
	}
	active := s.activeBuffer()
	top := s.scrollTop
	bottom := s.scrollBottom
	for count > 0 {
		copy(active[top:bottom], active[top+1:bottom+1])
		active[bottom] = blankVTLine(s.width)
		count--
	}
}

func (s *vtScreenState) scrollDown(count int) {
	if count <= 0 {
		return
	}
	active := s.activeBuffer()
	top := s.scrollTop
	bottom := s.scrollBottom
	for count > 0 {
		copy(active[top+1:bottom+1], active[top:bottom])
		active[top] = blankVTLine(s.width)
		count--
	}
}

func blankVTLine(width int) []rune {
	line := make([]rune, width)
	for i := range line {
		line[i] = ' '
	}
	return line
}

func (s *vtScreenState) eraseDisplay(mode int) {
	active := s.activeBuffer()
	switch mode {
	case 1:
		for y := 0; y <= s.cursorY; y++ {
			start := 0
			end := s.width
			if y == s.cursorY {
				end = min(s.cursorX+1, s.width)
			}
			for x := start; x < end; x++ {
				active[y][x] = ' '
			}
		}
	case 2:
		for y := 0; y < s.height; y++ {
			for x := 0; x < s.width; x++ {
				active[y][x] = ' '
			}
		}
	default:
		for y := s.cursorY; y < s.height; y++ {
			start := 0
			if y == s.cursorY {
				start = s.cursorX
			}
			for x := start; x < s.width; x++ {
				active[y][x] = ' '
			}
		}
	}
}

func (s *vtScreenState) eraseLine(mode int) {
	active := s.activeBuffer()
	line := active[s.cursorY]
	switch mode {
	case 1:
		for x := 0; x <= s.cursorX && x < s.width; x++ {
			line[x] = ' '
		}
	case 2:
		for x := 0; x < s.width; x++ {
			line[x] = ' '
		}
	default:
		for x := s.cursorX; x < s.width; x++ {
			line[x] = ' '
		}
	}
}

func (s *vtScreenState) eraseChars(count int) {
	count = max(count, 1)
	line := s.activeBuffer()[s.cursorY]
	for i := 0; i < count && s.cursorX+i < s.width; i++ {
		line[s.cursorX+i] = ' '
	}
}

func (s *vtScreenState) deleteChars(count int) {
	count = max(count, 1)
	line := s.activeBuffer()[s.cursorY]
	if s.cursorX >= s.width {
		return
	}
	copy(line[s.cursorX:], line[min(s.cursorX+count, s.width):])
	for i := s.width - count; i < s.width; i++ {
		if i >= 0 && i < s.width {
			line[i] = ' '
		}
	}
}

func (s *vtScreenState) insertChars(count int) {
	count = max(count, 1)
	line := s.activeBuffer()[s.cursorY]
	if s.cursorX >= s.width {
		return
	}
	for i := s.width - 1; i >= s.cursorX+count; i-- {
		line[i] = line[i-count]
	}
	for i := 0; i < count && s.cursorX+i < s.width; i++ {
		line[s.cursorX+i] = ' '
	}
}

func (s *vtScreenState) insertLines(count int) {
	count = max(count, 1)
	if s.cursorY < s.scrollTop || s.cursorY > s.scrollBottom {
		return
	}
	active := s.activeBuffer()
	bottom := s.scrollBottom
	count = min(count, bottom-s.cursorY+1)
	for i := bottom; i >= s.cursorY+count; i-- {
		active[i] = append([]rune(nil), active[i-count]...)
	}
	for i := 0; i < count && s.cursorY+i <= bottom; i++ {
		active[s.cursorY+i] = blankVTLine(s.width)
	}
}

func (s *vtScreenState) deleteLines(count int) {
	count = max(count, 1)
	if s.cursorY < s.scrollTop || s.cursorY > s.scrollBottom {
		return
	}
	active := s.activeBuffer()
	bottom := s.scrollBottom
	count = min(count, bottom-s.cursorY+1)
	for i := s.cursorY; i <= bottom-count; i++ {
		active[i] = append([]rune(nil), active[i+count]...)
	}
	for i := max(bottom-count+1, s.cursorY); i <= bottom; i++ {
		active[i] = blankVTLine(s.width)
	}
}

func (s *vtScreenState) clampCursor() {
	if s.width <= 0 || s.height <= 0 {
		s.cursorX, s.cursorY = 0, 0
		return
	}
	s.cursorX = min(max(s.cursorX, 0), s.width-1)
	s.cursorY = min(max(s.cursorY, 0), s.height-1)
}

func (s *vtScreenState) reset() {
	s.primary = newVTBuffer(s.width, s.height)
	s.alternate = newVTBuffer(s.width, s.height)
	s.cursorX, s.cursorY = 0, 0
	s.savedX, s.savedY = 0, 0
	s.showCursor = true
	s.altScreen = false
	s.scrollTop = 0
	s.scrollBottom = s.height - 1
	s.pending = s.pending[:0]
}
