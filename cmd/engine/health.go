package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"
)

// health checks a running engine's /health, for container healthchecks:
// the image has no shell or curl.
func health(args []string) error {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:3140/health", "engine health URL")
	_ = fs.Parse(args)
	client := http.Client{Timeout: 3 * time.Second}
	res, err := client.Get(*url)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("health: %s", res.Status)
	}
	return nil
}
