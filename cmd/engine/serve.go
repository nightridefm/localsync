package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nightridefm/localsync/internal/engine"
	"github.com/nightridefm/localsync/internal/rules"
)

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	pgURL := fs.String("pg", os.Getenv("LOCALSYNC_PG_URL"), "Postgres URL")
	rulesPath := fs.String("rules", "rules.yaml", "rules file")
	listen := fs.String("listen", ":3140", "listen address")
	window := fs.Duration("window", 5*time.Minute, "in-memory resume window")
	workers := fs.Int("workers", 4, "concurrent snapshot queries")
	_ = fs.Parse(args)
	secret := os.Getenv("LOCALSYNC_JWT_SECRET")
	if secret == "" {
		return errors.New("LOCALSYNC_JWT_SECRET is required")
	}
	rs, err := rules.Load(*rulesPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	e, err := engine.New(ctx, engine.Config{PG: *pgURL, Rules: rs, JWTSecret: []byte(secret), Window: *window, Workers: *workers})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: e.Handler()}
	go func() { _ = srv.Serve(ln) }()
	slog.Info("listening", "addr", ln.Addr().String())
	err = e.Run(ctx)
	_ = srv.Close()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
