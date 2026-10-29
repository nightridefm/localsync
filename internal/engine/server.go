package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/golang-jwt/jwt/v5"
)

// Handler serves /sync (WebSocket), /health and /debug/units.
func (e *Engine) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sync", e.serveSync)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	mux.HandleFunc("/debug/pprof/allocs", pprof.Handler("allocs").ServeHTTP)
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

// verify checks an HS256 token and returns its subject and expiry.
func (e *Engine) verify(token string) (string, time.Time, error) {
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		return e.cfg.JWTSecret, nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired(), jwt.WithAudience("localsync"))
	if err != nil {
		return "", time.Time{}, err
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", time.Time{}, errors.New("token has no sub")
	}
	exp, err := claims.GetExpirationTime()
	if err != nil || exp == nil {
		return "", time.Time{}, errors.New("token has no exp")
	}
	return sub, exp.Time, nil
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
		for {
			select {
			case m := <-c.out:
				typ := websocket.MessageBinary
				if m.text {
					typ = websocket.MessageText
				}
				wctx, wcancel := context.WithTimeout(ctx, 30*time.Second)
				err := conn.Write(wctx, typ, m.data)
				wcancel()
				if c.queued.Add(-int64(len(m.data))) < lowWater && c.pumping.CompareAndSwap(false, true) {
					// Queue drained: let the engine queue the next parts.
					e.Do(func() {
						c.pumping.Store(false)
						e.pump(c)
					})
				}
				if err != nil {
					return
				}
			case <-c.done:
				// Flush what is queued (e.g. a final error), then close.
				for {
					select {
					case m := <-c.out:
						_ = conn.Write(ctx, websocket.MessageText, m.data)
					default:
						conn.Close(websocket.StatusPolicyViolation, "closed by engine")
						return
					}
				}
			case <-ctx.Done():
				return
			}
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
		sub, exp, err := e.verify(m.Token)
		if err != nil {
			slog.Info("auth rejected", "err", err)
			e.Do(func() { c.fail("auth: " + err.Error()) })
			continue
		}
		e.Do(func() {
			if !c.closed {
				c.exp = exp
				e.want(c, sub, &m)
			}
		})
	}
}
