package engine

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/golang-jwt/jwt/v5"
)

// Handler serves /sync (WebSocket), /sync/http (the same protocol over
// HTTP streaming or long polling), /health and /debug/units.
func (e *Engine) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sync", e.serveSync)
	mux.HandleFunc("/sync/http", e.serveHTTP)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	mux.HandleFunc("/debug/pprof/allocs", pprof.Handler("allocs").ServeHTTP)
	mux.HandleFunc("/metrics", e.serveMetrics)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/debug/members", func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("key")
		done := make(chan []byte)
		e.Do(func() {
			out := map[string]any{}
			for _, u := range e.units {
				if !strings.HasPrefix(u.key, prefix) {
					continue
				}
				m := map[string]string{}
				for k, hs := range u.members {
					var h uint64
					for _, x := range hs {
						h += x
					}
					m[hex.EncodeToString([]byte(k))] = fmt.Sprint(h)
				}
				out[u.key] = map[string]any{"sum": fmt.Sprint(u.sum), "members": m, "pending": len(u.pending), "head": fmt.Sprint(e.head)}
			}
			b, _ := json.Marshal(out)
			done <- b
		})
		_, _ = w.Write(<-done)
	})
	mux.HandleFunc("/debug/units", func(w http.ResponseWriter, r *http.Request) {
		done := make(chan []byte)
		e.Do(func() {
			type info struct {
				Key      string `json:"key"`
				Ref      uint32 `json:"ref"`
				Members  int    `json:"members"`
				Checksum string `json:"checksum"`
				Pending  int    `json:"pending"`
				History  int    `json:"history"`
				Holds    int    `json:"holds"`
			}
			var out []info
			for _, u := range e.units {
				out = append(out, info{u.key, u.ref, len(u.members), fmt.Sprint(u.sum), len(u.pending), len(u.hist), u.holds})
			}
			b, _ := json.Marshal(map[string]any{"epoch": e.epoch, "head": fmt.Sprint(e.head), "units": out,
				"snapshotQueries": e.stats.snapshotQueries, "cacheHits": e.stats.cacheHits, "restarts": e.restarts})
			done <- b
		})
		_, _ = w.Write(<-done)
	})
	return mux
}

// principal is what a verified token says: who, what it grants (rules
// with `claims` need one of them, A22) and until when.
type principal struct {
	sub    string
	grants []string
	exp    time.Time
}

// verify checks an HS256 token for this deployment's audience.
func (e *Engine) verify(token string) (principal, error) {
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		return e.cfg.JWTSecret, nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired(), jwt.WithAudience(e.cfg.Audience))
	if err != nil {
		return principal{}, err
	}
	var p principal
	p.sub, _ = claims["sub"].(string)
	if p.sub == "" {
		return principal{}, errors.New("token has no sub")
	}
	exp, err := claims.GetExpirationTime()
	if err != nil || exp == nil {
		return principal{}, errors.New("token has no exp")
	}
	p.exp = exp.Time
	switch g := claims["grants"].(type) {
	case nil:
	case []any:
		for _, x := range g {
			s, ok := x.(string)
			if !ok {
				return principal{}, errors.New("token grants must be strings")
			}
			p.grants = append(p.grants, s)
		}
	default:
		return principal{}, errors.New("token grants must be a list of strings")
	}
	return p, nil
}

// LOCALSYNC_WS_COMPRESSION=0 turns permessage-deflate off.
var wsCompressionOff = os.Getenv("LOCALSYNC_WS_COMPRESSION") == "0"

func (e *Engine) serveSync(w http.ResponseWriter, r *http.Request) {
	opts := &websocket.AcceptOptions{InsecureSkipVerify: true}
	if !wsCompressionOff {
		// Per-message deflate without context takeover: initial loads and
		// move-ins shrink ~40%, no compressor state is kept per connection,
		// and small deltas (below the threshold, where deflate only adds
		// bytes) go out as they are (docs/bench/deflate.md).
		opts.CompressionMode = websocket.CompressionNoContextTakeover
		opts.CompressionThreshold = 512
	}
	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return
	}
	conn.SetReadLimit(1 << 20)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	c := &Client{e: e, out: make(chan outMsg, 4096), done: make(chan struct{})}
	e.Do(func() { e.addClient(c) })
	defer e.Do(func() { e.removeClient(c) })

	go func() {
		defer cancel()
		write := func(m outMsg) error {
			typ := websocket.MessageBinary
			if m.text {
				typ = websocket.MessageText
			}
			wctx, wcancel := context.WithTimeout(ctx, 30*time.Second)
			defer wcancel()
			return conn.Write(wctx, typ, m.data)
		}
		if e.writeLoop(ctx, c, write, nil, nil) {
			conn.Close(websocket.StatusPolicyViolation, "closed by engine")
		}
	}()

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var m wantMsg
		if err := json.Unmarshal(data, &m); err != nil || m.Type != "want" {
			e.Do(func() { c.fail("bad message") })
			continue
		}
		p, err := e.verify(m.Token)
		if err != nil {
			slog.Info("auth rejected", "err", err)
			e.Do(func() { c.fail("auth: " + err.Error()) })
			continue
		}
		e.Do(func() {
			if !c.closed {
				c.exp, c.grants = p.exp, p.grants
				e.want(c, p.sub, &m)
			}
		})
	}
}

// writeLoop writes the client's queued messages until the connection ends
// (ctx), a write fails, the engine closes the client (it then flushes what
// is queued, e.g. a final error, and returns true), or the response should
// end: stop returns true after a message, or deadline fires. Neither ends
// the loop inside a frame sent in parts: the client applies a frame only
// whole, and a cut one would be wasted.
func (e *Engine) writeLoop(ctx context.Context, c *Client, write func(outMsg) error, stop func(outMsg) bool, deadline <-chan time.Time) bool {
	inFrame, expired := false, false
	for {
		select {
		case m := <-c.out:
			err := write(m)
			if err == nil {
				e.m.sentBytes.Add(int64(len(m.data)))
				e.m.sentMsgs.Add(1)
			}
			if c.queued.Add(-int64(len(m.data))) < lowWater && c.pumping.CompareAndSwap(false, true) {
				// Queue drained: let the engine queue the next parts.
				e.Do(func() {
					c.pumping.Store(false)
					e.pump(c)
				})
			}
			if err != nil {
				return false
			}
			if !m.text {
				inFrame = m.part || m.tail
			} else if m.tail {
				inFrame = false
			}
			if !inFrame && (expired || (stop != nil && stop(m))) {
				return false
			}
		case <-c.done:
			for {
				select {
				case m := <-c.out:
					_ = write(m)
				default:
					return true
				}
			}
		case <-deadline:
			deadline = nil
			if !inFrame {
				return false
			}
			expired = true
		case <-ctx.Done():
			return false
		}
	}
}

// untilUnits ends a long poll once a frame with unit data has gone out
// (writeLoop ends it after the frame's last part).
func untilUnits() func(outMsg) bool {
	units := false
	return func(m outMsg) bool {
		units = units || m.units
		return units
	}
}

const (
	// A streamed response ends after streamMax (the client re-POSTs at
	// once and resumes from its cursor), so no proxy's limit on response
	// duration is ever reached; a long poll after pollMax at most.
	streamMax = 5 * time.Minute
	pollMax   = 20 * time.Second
	// Records of this size or more are deflated, like permessage-deflate
	// on the WebSocket (no context kept between records).
	recordDeflateMin = 512
	recText          = 0
	recBinary        = 1
	recDeflated      = 0x80
)

var flatePool = sync.Pool{New: func() any {
	w, _ := flate.NewWriter(nil, flate.BestSpeed)
	return w
}}

// appendRecord frames one protocol message for /sync/http: a kind byte
// (text or binary, 0x80 set when the payload is raw deflate), the payload
// length as 4 bytes little-endian, the payload.
func appendRecord(dst []byte, m outMsg) []byte {
	kind := byte(recBinary)
	if m.text {
		kind = recText
	}
	payload := m.data
	if len(payload) >= recordDeflateMin {
		var b bytes.Buffer
		fw := flatePool.Get().(*flate.Writer)
		fw.Reset(&b)
		_, _ = fw.Write(payload)
		_ = fw.Close()
		flatePool.Put(fw)
		if b.Len() < len(payload) {
			kind |= recDeflated
			payload = b.Bytes()
		}
	}
	dst = append(dst, kind)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	return append(dst, payload...)
}

// serveHTTP is the protocol over plain HTTP, for networks where WebSockets
// fail (proxies that strip Upgrade or cut long-lived sockets). The request
// body is one WANT; the response streams the same messages a WebSocket
// would carry, as records (appendRecord), and then ends. Each request is a
// connection: a new WANT (new wants, a fresh token) is a new request that
// resumes from the client's cursor, as a WebSocket reconnect does.
//
// ?mode=poll is for proxies that buffer responses: the response ends after
// the first frame with unit data (or pollMax), so buffering only delays it
// until it ends.
func (e *Engine) serveHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	// The token is in the body, never a cookie, so any origin may call.
	h.Set("Access-Control-Allow-Origin", "*")
	if r.Method == http.MethodOptions {
		h.Set("Access-Control-Allow-Methods", "POST")
		h.Set("Access-Control-Allow-Headers", "Content-Type")
		h.Set("Access-Control-Max-Age", "86400")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST a WANT", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	var m wantMsg
	if err == nil {
		err = json.Unmarshal(body, &m)
	}
	if err != nil || m.Type != "want" {
		http.Error(w, "bad WANT", http.StatusBadRequest)
		return
	}
	poll := r.URL.Query().Get("mode") == "poll"
	// octet-stream: the gateways' gzip_types leave it alone (records are
	// already compressed); no-transform for other proxies; nginx streams
	// the response as written when it sees X-Accel-Buffering: no.
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Cache-Control", "no-store, no-transform")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	_ = rc.Flush()

	ctx := r.Context()
	c := &Client{e: e, out: make(chan outMsg, 4096), done: make(chan struct{}), http: true}
	e.Do(func() { e.addClient(c) })
	defer e.Do(func() { e.removeClient(c) })
	p, err := e.verify(m.Token)
	if err != nil {
		slog.Info("auth rejected", "err", err)
		e.Do(func() { c.fail("auth: " + err.Error()) })
	} else {
		e.Do(func() {
			if !c.closed {
				c.exp, c.grants = p.exp, p.grants
				e.want(c, p.sub, &m)
			}
		})
	}

	var buf []byte
	write := func(m outMsg) error {
		buf = appendRecord(buf[:0], m)
		_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := w.Write(buf); err != nil {
			return err
		}
		return rc.Flush()
	}
	var stop func(outMsg) bool
	limit := streamMax
	if poll {
		limit, stop = pollMax, untilUnits()
	}
	timer := time.NewTimer(limit)
	defer timer.Stop()
	e.writeLoop(ctx, c, write, stop, timer.C)
}

// serveMetrics is the Prometheus text format, without a client library:
// a handful of gauges read on the engine loop plus monotonic counters.
func (e *Engine) serveMetrics(w http.ResponseWriter, r *http.Request) {
	type snap struct {
		wsClients, httpClients     int
		units, held, pending       int
		maxClientLag, head         uint64
		txns, changes              int64
		wants, refused             int64
		snapshotQueries, cacheHits int64
		commitLag                  float64
		restarts                   int
	}
	done := make(chan snap)
	e.Do(func() {
		var s snap
		for c := range e.clients {
			if c.http {
				s.httpClients++
			} else {
				s.wsClients++
			}
			if c.holds != nil && e.head > c.cursor {
				s.maxClientLag = max(s.maxClientLag, e.head-c.cursor)
			}
		}
		for _, u := range e.units {
			s.units++
			if u.holds > 0 {
				s.held++
			}
			if len(u.pending) > 0 {
				s.pending++
			}
		}
		s.txns, s.changes = e.m.txns, e.m.changes
		s.wants, s.refused = e.m.wants, e.m.refused
		s.snapshotQueries, s.cacheHits = e.m.snapshotQueries, e.m.cacheHits
		s.commitLag = e.m.commitLag
		s.restarts, s.head = e.restarts, e.head
		done <- s
	})
	s := <-done
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	metric := func(name, kind, help string, v any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, kind, name, v)
	}
	fmt.Fprintf(w, "# HELP localsync_clients Connected clients, by transport.\n# TYPE localsync_clients gauge\n")
	fmt.Fprintf(w, "localsync_clients{transport=\"websocket\"} %d\nlocalsync_clients{transport=\"http\"} %d\n", s.wsClients, s.httpClients)
	metric("localsync_units", "gauge", "Units in memory (held or lingering in the resume window).", s.units)
	metric("localsync_units_held", "gauge", "Units some client holds.", s.held)
	metric("localsync_units_pending", "gauge", "Units with changes awaiting a restricted query.", s.pending)
	metric("localsync_client_lag_bytes_max", "gauge", "WAL bytes the furthest-behind client trails the engine's head.", s.maxClientLag)
	metric("localsync_commit_lag_seconds", "gauge", "Seconds from commit in Postgres to the engine routing it, last transaction.", s.commitLag)
	metric("localsync_head_lsn", "gauge", "The engine's head: the end of the last transaction routed.", s.head)
	metric("localsync_transactions_total", "counter", "Transactions routed.", s.txns)
	metric("localsync_changes_total", "counter", "Row changes routed.", s.changes)
	metric("localsync_wants_total", "counter", "WANT messages handled (connects, want changes, token refreshes).", s.wants)
	metric("localsync_wants_refused_total", "counter", "Wants refused (unknown rule, bad parameter, not allowed).", s.refused)
	metric("localsync_snapshot_queries_total", "counter", "Snapshot queries run to give clients units.", s.snapshotQueries)
	metric("localsync_snapshot_cache_hits_total", "counter", "Snapshots served from the cache instead.", s.cacheHits)
	metric("localsync_restarts_total", "counter", "In-process restarts after the replication stream was lost.", s.restarts)
	metric("localsync_sent_messages_total", "counter", "Protocol messages written to clients.", e.m.sentMsgs.Load())
	metric("localsync_sent_bytes_total", "counter", "Message bytes written to clients (before transport compression).", e.m.sentBytes.Load())
}
