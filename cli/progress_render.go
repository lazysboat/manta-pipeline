package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

const barLogTimePrefixLen = 20 // "YYYY/MM/DD HH:MM:SS "

type barLineMsg struct {
	Work  string  `json:"work"`
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	IsInt bool    `json:"is_int"`
}

type barEntry struct {
	workContext string
	bar         *progressbar.ProgressBar
	buf         *bytes.Buffer
	lastRender  string
	isInt       bool
	vmin        float64
	vmax        float64
}

type progressView struct {
	mu        sync.Mutex
	entries   map[string]*barEntry
	order     []string
	barCount  int
	stepLines []string
	stepCount int
}

func tailAPILogUntilDone(logFile, doneFile, sessionID string) {
	if !term.IsTerminal(int(os.Stdout.Fd())) {
		tailAPILogPlain(logFile, doneFile)
		return
	}

	f, err := openTailAt(logFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "progress: cannot open %s: %v\n", logFile, err)
		return
	}
	defer f.Close()

	view := &progressView{entries: map[string]*barEntry{}}

	pollCtx, pollCancel := context.WithCancel(context.Background())
	defer pollCancel()
	if sessionID != "" {
		go pollStepStatus(pollCtx, sessionID, view)
	}

	reader := bufio.NewReader(f)

	done := false
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			view.handleLine(strings.TrimRight(line, "\n"))
		}
		if err == io.EOF {
			if done {
				break
			}
			if _, err := os.Stat(doneFile); err == nil {
				done = true
				continue
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			return
		}
	}

	fmt.Println()
}

func tailAPILogPlain(logFile, doneFile string) {
	f, err := openTailAt(logFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "progress: cannot open %s: %v\n", logFile, err)
		return
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	done := false
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			stripped := strings.TrimRight(line, "\n")
			if !isBarLine(stripped) {
				fmt.Println(stripped)
			}
		}
		if err == io.EOF {
			if done {
				return
			}
			if _, err := os.Stat(doneFile); err == nil {
				done = true
				continue
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			return
		}
	}
}

// openTailAt opens the file and seeks to roughly the last 4 KB, advancing past
// the next newline so the file position lands on a clean line boundary. The
// caller may then wrap f in a bufio.Reader without losing bytes.
func openTailAt(logFile string) (*os.File, error) {
	f, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	const tailBytes = 4096
	if st.Size() > tailBytes {
		if _, err := f.Seek(-tailBytes, io.SeekEnd); err != nil {
			f.Close()
			return nil, err
		}
		buf := [1]byte{}
		for {
			n, err := f.Read(buf[:])
			if err != nil || n == 0 {
				break
			}
			if buf[0] == '\n' {
				break
			}
		}
	}
	return f, nil
}

func isBarLine(line string) bool {
	body := stripTimePrefix(line)
	return strings.HasPrefix(body, "__bar__ ")
}

func stripTimePrefix(line string) string {
	if len(line) >= barLogTimePrefixLen && line[4] == '/' && line[7] == '/' && line[10] == ' ' && line[13] == ':' && line[16] == ':' && line[19] == ' ' {
		return line[barLogTimePrefixLen:]
	}
	return line
}

func (v *progressView) handleLine(line string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	body := stripTimePrefix(line)
	if strings.HasPrefix(body, "__bar__ ") {
		v.handleBar(body[len("__bar__ "):])
		return
	}
	v.printLog(line)
}

func (v *progressView) setStepSnapshot(wv *stepView) {
	v.mu.Lock()
	defer v.mu.Unlock()

	running := make(map[string]bool, len(wv.Works))
	for _, w := range wv.Works {
		if w.Status == "running" {
			running[w.Pipeline+"."+w.Stage+"."+w.Name] = true
		}
	}

	kept := v.order[:0:0]
	for _, key := range v.order {
		if running[v.entries[key].workContext] {
			kept = append(kept, key)
		} else {
			delete(v.entries, key)
		}
	}
	v.order = kept

	v.stepLines = formatStepLines(wv)
	v.redraw("")
}

func (v *progressView) handleBar(jsonPart string) {
	var msg barLineMsg
	if err := json.Unmarshal([]byte(jsonPart), &msg); err != nil {
		return
	}
	key := msg.Work + "/" + msg.Name
	entry, ok := v.entries[key]
	if !ok {
		entry = newBarEntry(msg)
		v.entries[key] = entry
		v.order = append(v.order, key)
	} else {
		entry.vmin = msg.Min
		entry.vmax = msg.Max
	}
	updateBarValue(entry, msg)
	if frame := lastFrame(entry.buf); frame != "" {
		entry.lastRender = fmt.Sprintf("[%s/%s]  %s", msg.Work, msg.Name, frame)
	}
	v.redraw("")
}

func (v *progressView) printLog(line string) {
	v.redraw(line)
}

func (v *progressView) redraw(extraLog string) {
	total := v.barCount + v.stepCount
	if total > 0 {
		fmt.Print("\033[" + strconv.Itoa(total) + "A\033[J")
	}
	if extraLog != "" {
		fmt.Println(extraLog)
	}
	topSepCount := 0
	if len(v.order) > 0 || len(v.stepLines) > 0 {
		fmt.Println()
		topSepCount = 1
	}
	for _, line := range v.stepLines {
		fmt.Println(line)
	}
	midSepCount := 0
	if len(v.order) > 0 && len(v.stepLines) > 0 {
		fmt.Println()
		midSepCount = 1
	}
	for _, key := range v.order {
		fmt.Println(v.entries[key].lastRender)
	}
	v.stepCount = len(v.stepLines) + topSepCount
	v.barCount = len(v.order) + midSepCount
}

func newBarEntry(msg barLineMsg) *barEntry {
	buf := &bytes.Buffer{}
	var bar *progressbar.ProgressBar
	if msg.IsInt {
		span := int64(msg.Max - msg.Min)
		if span <= 0 {
			span = 1
		}
		bar = progressbar.NewOptions64(span,
			progressbar.OptionSetWriter(buf),
			progressbar.OptionSetWidth(30),
			progressbar.OptionShowCount(),
			progressbar.OptionSetRenderBlankState(true),
		)
	} else {
		bar = progressbar.NewOptions64(1000,
			progressbar.OptionSetWriter(buf),
			progressbar.OptionSetWidth(30),
			progressbar.OptionSetRenderBlankState(true),
		)
	}
	return &barEntry{
		workContext: msg.Work,
		bar:         bar,
		buf:         buf,
		isInt:       msg.IsInt,
		vmin:        msg.Min,
		vmax:        msg.Max,
	}
}

func updateBarValue(entry *barEntry, msg barLineMsg) {
	if entry.isInt {
		v := int64(msg.Value - msg.Min)
		if v < 0 {
			v = 0
		}
		_ = entry.bar.Set64(v)
		return
	}
	span := msg.Max - msg.Min
	scaled := int64(0)
	if span > 0 {
		scaled = int64((msg.Value - msg.Min) / span * 1000)
	}
	if scaled < 0 {
		scaled = 0
	}
	if scaled > 1000 {
		scaled = 1000
	}
	_ = entry.bar.Set64(scaled)
}

func lastFrame(buf *bytes.Buffer) string {
	s := buf.String()
	buf.Reset()
	if i := strings.LastIndex(s, "\r"); i >= 0 {
		s = s[i+1:]
	}
	return strings.TrimRight(s, "\n ")
}

func pollStepStatus(ctx context.Context, sessionID string, view *progressView) {
	tick := func() {
		resp, err := sendClientCommand("step " + sessionID)
		if err != nil || strings.HasPrefix(resp, "error:") {
			return
		}
		var wv stepView
		if err := json.Unmarshal([]byte(resp), &wv); err != nil {
			return
		}
		view.setStepSnapshot(&wv)
	}

	tick()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}

func formatStepLines(wv *stepView) []string {
	stepLabel := "—"
	if wv.StepCount > 0 {
		idx := wv.CurrentStep + 1
		if idx < 1 {
			idx = 0
		}
		stepLabel = fmt.Sprintf("%d/%d", idx, wv.StepCount)
	}
	lines := []string{fmt.Sprintf("Step %s", stepLabel)}
	for _, w := range wv.Works {
		ctx := w.Pipeline + "." + w.Stage + "." + w.Name
		lines = append(lines, fmt.Sprintf("  %-9s  %-44s  %s",
			w.Status, ctx, formatTiming(w.StartedAt, w.EndedAt, w.Status)))
	}
	return lines
}
