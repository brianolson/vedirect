package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/brianolson/vedirect"
)

func recDiff(a, b map[string]string) map[string]string {
	d := make(map[string]string, len(b))
	for ak, av := range a {
		bv, ok := b[ak]
		if ok {
			if av != bv {
				// change
				d[ak] = bv
			} // else no change
		} else {
			// not present in b
			//d[ak] = nil
		}
	}
	for bk, bv := range b {
		_, ok := a[bk]
		if !ok {
			// new value not in a
			d[bk] = bv
		}
	}
	return d
}

func makeDeltas(batch []map[string]string, deltas []map[string]interface{}, sendPeriod int) []map[string]interface{} {
	pos := len(deltas)
	if deltas == nil {
		deltas = make([]map[string]interface{}, 0, len(batch))
	}
	for ; pos < len(batch); pos++ {
		var nrec map[string]string
		if (pos % sendPeriod) == 0 {
			nrec = make(map[string]string, len(batch[pos]))
			for k, v := range batch[pos] {
				nrec[k] = v
			}
		} else {
			nrec = recDiff(batch[pos-1], batch[pos])
		}
		deltas = append(deltas, vedirect.ParseRecord(nrec))
	}
	return deltas
}

type Message struct {
	// Data is differential VE.Direct records
	// Data[0] will be a whole record
	// Data[1:] will only be the fields that changed
	// Record fields are string:string key:value, _except_ "_t" = {int64 milliseconds since 1970-1-1 00:00:00}
	Data []map[string]interface{} `json:"d"`
}

type sendRequest struct {
	msg   *Message
	err   error
	start time.Time
}

var (
	sendPeriod   int
	postUrl      string
	devicePath   string
	retryPeriod  time.Duration
	verbose      bool
	sendJsonGzip bool
)

func main() {
	flag.IntVar(&sendPeriod, "send-period", 1000, "after this number of messages try to send accumulated data")
	flag.StringVar(&postUrl, "post", "", "URL to POST application/json message to (May be \"-\" for stdout)")
	flag.StringVar(&devicePath, "dev", "", "device to read")
	flag.DurationVar(&retryPeriod, "retry", 60*time.Second, "Duration between retries")
	flag.BoolVar(&verbose, "v", false, "verbose debug out")
	flag.BoolVar(&sendJsonGzip, "z", true, "sent application/gzip compress of json")
	flag.Parse()
	if postUrl == "" {
		fmt.Fprintf(os.Stderr, "-post URL is required\n")
		os.Exit(1)
		return
	}
	if devicePath == "" {
		fmt.Fprintf(os.Stderr, "-dev device_path is required\n")
		os.Exit(1)
		return
	}
	dout := os.Stderr
	if !verbose {
		dout = nil
	}
	recChan := make(chan map[string]string, 10)
	var wg sync.WaitGroup
	_, err := vedirect.Open(devicePath, recChan, &wg, context.Background(), dout, vedirect.AddTime)
	maybefail(err, "%s: Open, %v", devicePath, err)
	wg.Add(1)
	go mainThread(recChan, &wg)
	wg.Wait()
}

// receive data from Vedirect parser, sometimes poke the sendThread.
func mainThread(recChan <-chan map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	batch := make([]map[string]string, 0, sendPeriod)
	sendActive := false
	reqStart := make(chan sendRequest, 1)
	reqReturn := make(chan sendRequest, 1)
	wg.Add(1)
	go sendThread(postUrl, reqStart, reqReturn, wg)
	defer close(reqStart)

	for {
		select {
		case rec, ok := <-recChan:
			if !ok {
				break
			}
			now := time.Now()
			batch = append(batch, rec)
			if len(batch) >= sendPeriod && !sendActive {
				debug("try send %d recs", len(batch))
				msg := Message{
					Data: makeDeltas(batch, nil, sendPeriod),
				}
				reqStart <- sendRequest{msg: &msg, start: now}
				sendActive = true
			}
		case req, ok := <-reqReturn:
			if !ok {
				break
			}
			if req.err == nil {
				debug("sent %d recs", len(req.msg.Data))
				// clear away batch data that was successfully sent
				oldLast := len(req.msg.Data)
				newLast := len(batch) - oldLast
				copy(batch, batch[oldLast:])
				batch = batch[:newLast]
				sendActive = false
			} else {
				// grow the batch more, retry
				debug("send err %v", req.err)
				req.msg.Data = makeDeltas(batch, req.msg.Data, sendPeriod)
				debug("retry send %d recs", len(req.msg.Data))
				req.err = nil
				req.start = time.Now()
				reqStart <- req
			}
		}
	}
}

func compress(blob []byte) (zb []byte, err error) {
	var ob bytes.Buffer
	w := gzip.NewWriter(&ob)
	_, err = w.Write(blob)
	if err != nil {
		return
	}
	err = w.Close()
	if err != nil {
		return
	}
	return ob.Bytes(), nil
}

// serialize data as json, http send it, note status
func sendThread(url string, in, out chan sendRequest, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	var client http.Client
	defer close(out)
	for req := range in {
		blob, err := json.Marshal(req.msg)
		maybefail(err, "json err, %v", err)
		if url == "-" {
			fmt.Printf("%s\n", string(blob))
			req.err = nil
		} else {
			contentType := "application/json"
			if sendJsonGzip {
				blob, err = compress(blob)
				maybefail(err, "gzip err, %v", err)
				contentType = "application/gzip"
			}
			br := bytes.NewReader(blob)
			response, err := client.Post(url, contentType, br)
			req.err = err
			if err == nil {
				if response.StatusCode != 200 {
					req.err = fmt.Errorf("Status %s", response.Status)
				}
			}
		}
		if req.err != nil {
			waitTime := req.start.Add(retryPeriod).Sub(time.Now())
			debug("post err %v, sleep %s", req.err, waitTime)
			time.Sleep(waitTime)
		}
		out <- req
	}
}

func maybefail(err error, msg string, args ...interface{}) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func debug(msg string, args ...interface{}) {
	if !verbose {
		return
	}
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
}
