package engine

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// readRecord decodes one /sync/http record (the client's side, http.ts).
func readRecord(t *testing.T, b []byte) (kind byte, payload []byte, rest []byte) {
	t.Helper()
	n := binary.LittleEndian.Uint32(b[1:5])
	kind, payload, rest = b[0], b[5:5+n], b[5+n:]
	if kind&recDeflated != 0 {
		var err error
		payload, err = io.ReadAll(flate.NewReader(bytes.NewReader(payload)))
		if err != nil {
			t.Fatal(err)
		}
	}
	return kind, payload, rest
}

func TestAppendRecord(t *testing.T) {
	small := []byte(`{"type":"beat"}`)
	big := bytes.Repeat([]byte("row body, compressible "), 100)
	b := appendRecord(nil, outMsg{text: true, data: small})
	b = appendRecord(b, outMsg{data: big})
	kind, p, rest := readRecord(t, b)
	if kind != recText || !bytes.Equal(p, small) {
		t.Fatalf("small text: kind %#x payload %q", kind, p)
	}
	kind, p, rest = readRecord(t, rest)
	if kind != recBinary|recDeflated || !bytes.Equal(p, big) || len(rest) != 0 {
		t.Fatalf("big binary: kind %#x, %d bytes, %d left", kind, len(p), len(rest))
	}
	if len(b) > len(big)/4 {
		t.Fatalf("not compressed: %d bytes for %d", len(b), len(big))
	}
}

// loopEngine serves Engine.Do for writeLoop (it asks for more parts).
func loopEngine(t *testing.T) *Engine {
	e := &Engine{requests: make(chan func(), 64)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for {
			select {
			case fn := <-e.requests:
				fn()
			case <-ctx.Done():
				return
			}
		}
	}()
	return e
}

func TestWriteLoopLongPoll(t *testing.T) {
	e := loopEngine(t)
	c := &Client{e: e, out: make(chan outMsg, 16), done: make(chan struct{})}
	msgs := []outMsg{
		{text: true, data: []byte("units")},
		{data: []byte("empty frame")},
		{data: []byte("part 1"), part: true, units: true},
		{text: true, data: []byte("beat")},
		{data: []byte("last part"), units: true},
		{data: []byte("next frame"), units: true},
	}
	for _, m := range msgs {
		c.out <- m
	}
	var got []string
	write := func(m outMsg) error {
		got = append(got, string(m.data))
		return nil
	}
	e.writeLoop(context.Background(), c, write, untilUnits(), nil)
	// Ends after the first frame with units, never inside it.
	if len(got) != 5 || got[4] != "last part" {
		t.Fatalf("wrote %q", got)
	}
}

func TestWriteLoopDeadlineInsideFrame(t *testing.T) {
	e := loopEngine(t)
	c := &Client{e: e, out: make(chan outMsg, 16), done: make(chan struct{})}
	deadline := make(chan time.Time, 1)
	var got []string
	write := func(m outMsg) error {
		got = append(got, string(m.data))
		if m.part && len(got) == 1 {
			deadline <- time.Now()
		}
		return nil
	}
	c.out <- outMsg{data: []byte("part"), part: true, units: true}
	done := make(chan struct{})
	go func() {
		e.writeLoop(context.Background(), c, write, nil, deadline)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("ended inside a frame")
	default:
	}
	c.out <- outMsg{data: []byte("end"), units: true}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("did not end after the frame")
	}
	if len(got) != 2 {
		t.Fatalf("wrote %q", got)
	}
}

func TestMetrics(t *testing.T) {
	e := loopEngine(t)
	e.clients = map[*Client]struct{}{{http: true}: {}, {}: {}, {}: {}}
	e.units = map[string]*Unit{"a": {holds: 1}, "b": {}}
	e.m.txns, e.m.wants = 7, 3
	e.m.sentBytes.Add(1024)
	rec := httptest.NewRecorder()
	e.serveMetrics(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`localsync_clients{transport="websocket"} 2`,
		`localsync_clients{transport="http"} 1`,
		"localsync_units 2\n",
		"localsync_units_held 1\n",
		"localsync_transactions_total 7\n",
		"localsync_wants_total 3\n",
		"localsync_sent_bytes_total 1024\n",
		"# TYPE localsync_sent_bytes_total counter\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in\n%s", want, body)
		}
	}
}
