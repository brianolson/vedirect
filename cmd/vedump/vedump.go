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

func main() {
	flag.Parse()
	argv := flag.Args()
	fname := argv[0]
	recChan := make(chan map[string]string, 10)
	fin, err := os.Open(fname)
	maybefail(err, "%s: could not open, %v", fname, err)
	var wg sync.WaitGroup
	ve := vedirect.New(fin, recChan, &wg, context.Background())
	err = ve.Start()
	maybefail(err, "%s: start err, %v", fname, err)
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
