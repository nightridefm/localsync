// Command relay is the optional spoke fan-out relay: a byte multiplexer
// that subscribes upstream to the engine once per key and serves clients
// at the edge. See docs/design.md.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "localsync relay: not implemented yet (Phase 0 skeleton)")
	os.Exit(1)
}
