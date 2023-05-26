// vedump should demonstrate connection to a VE.Direct device
//
// Most VE.Direct devices now default to printing a status message
// about once per second, and this utility will parse that and print a
// json message to stdout.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/brianolson/vedirect"
)

var verbose bool

func debug(msg string, args ...interface{}) {
	if !verbose {
		return
	}
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
}

func main() {
	flag.BoolVar(&verbose, "v", false, "verbose debug out")
	flag.Parse()
	argv := flag.Args()
	fname := argv[0]
	recChan := make(chan map[string]string, 10)
	var wg sync.WaitGroup
	dout := os.Stderr
	if !verbose {
		dout = nil
	}
	_, err := vedirect.Open(fname, recChan, &wg, context.Background(), dout)
	maybefail(err, "%s: Vedirect Open, %v", fname, err)
	for rec := range recChan {
		blob, err := json.MarshalIndent(rec, "", "  ")
		maybefail(err, "json encode err, %v", err)
		fmt.Printf("%s\n", string(blob))
	}
	wg.Wait()
}

func maybefail(err error, msg string, args ...interface{}) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}
