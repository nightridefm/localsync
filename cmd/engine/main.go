// Command engine is the localsync hub service: it owns the replication
// slot, rule evaluation, the snapshot splice, frame encoding and client
// connections. See docs/design.md.
//
//	engine serve  --pg URL --rules rules.yaml [--listen :3140]
//	engine plan   --pg URL --rules rules.yaml [--max-tier N] [--run=false]
//	engine health [--url http://127.0.0.1:3140/health]
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "plan":
		err = plan(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	case "health":
		err = health(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "engine:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: engine (serve|plan|health) [flags]")
	os.Exit(2)
}
