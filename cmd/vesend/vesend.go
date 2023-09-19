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
	"reflect"
	"sort"
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
func recDiffI(a, b map[string]interface{}) map[string]interface{} {
	d := make(map[string]interface{}, len(b))
	for ak, av := range a {
		bv, ok := b[ak]
		if ok {
			if !reflect.DeepEqual(av, bv) {
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
	serveAddr    string
)

func main() {
	flag.IntVar(&sendPeriod, "send-period", 1000, "after this number of messages try to send accumulated data")
	flag.StringVar(&postUrl, "post", "", "URL to POST application/json message to (May be \"-\" for stdout)")
	flag.StringVar(&devicePath, "dev", "", "device to read")
	flag.DurationVar(&retryPeriod, "retry", 60*time.Second, "Duration between retries")
	flag.BoolVar(&verbose, "v", false, "verbose debug out")
	flag.BoolVar(&sendJsonGzip, "z", true, "sent application/gzip compress of json")
	flag.StringVar(&serveAddr, "serve", "", "host:port to serve http API from")
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
	vedirect.DebugEnabled = verbose
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

type Server struct {
	sum vedirect.StreamingSummary
	l   sync.RWMutex
}

func (sums *Server) dataReceiver(recChan <-chan map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		srec, ok := <-recChan
		if !ok {
			debug("dataReceiver exiting")
			return
		}
		rec := vedirect.ParseRecord(srec)
		sums.l.Lock()
		sums.sum.Add(rec)
		sums.l.Unlock()
	}
}

// TODO: make this a method of StreamingSummary
func mergeSummedRaw(sdat, rdat []map[string]interface{}, raw_after int64) []map[string]interface{} {
	if len(sdat) == 0 {
		debug("merge no summary")
		return rdat
	}
	if len(rdat) == 0 {
		debug("merge no raw")
		return sdat
	}
	var sumNewest int64 = 0
	for _, rec := range sdat {
		t := rec["_t"].(int64)
		if t > sumNewest && t < raw_after {
			sumNewest = t
		}
	}
	if sumNewest == 0 {
		debug("merge all raw")
		return rdat
	}
	rawOldest := rdat[0]["_t"].(int64)
	for _, rec := range rdat {
		t := rec["_t"].(int64)
		if t < rawOldest {
			rawOldest = t
		}
	}
	debug("merge sumNewest %d rawOldest %d", sumNewest, rawOldest)
	var alldata []map[string]interface{}
	if sumNewest > rawOldest {
		// find the oldest summary that is newer than rawOldest, then drop raw older than that and summaries newer than that, then join, perfect no overlap
		sumKey := sumNewest
		//sumKeyI := sumKeyI
		for _, rec := range sdat {
			t := rec["_t"].(int64)
			if t < sumKey && t > rawOldest {
				sumKey = t
				//sumKeyI = i
			}
		}
		alldata = make([]map[string]interface{}, 0, len(sdat)+len(rdat))
		for _, rec := range sdat {
			t := rec["_t"].(int64)
			if t <= sumKey {
				alldata = append(alldata, rec)
			}
		}
		for _, rec := range rdat {
			t := rec["_t"].(int64)
			if t > sumKey {
				alldata = append(alldata, rec)
			}
		}
	} else {
		// there is a gap? join and hope for the best
		alldata = make([]map[string]interface{}, len(sdat)+len(rdat))
		copy(alldata, sdat)
		copy(alldata[len(sdat):], rdat)
	}
	// TODO: sort data on _t ascending
	return alldata
}

type ReturnJSON struct {
	Data []map[string]interface{} `json:"d"`
}

type RecTimeSort []map[string]interface{}

// Len is part of sort.Interface
func (rts *RecTimeSort) Len() int {
	return len(*rts)
}
func (rts *RecTimeSort) Less(i, j int) bool {
	a := (*rts)[i]
	b := (*rts)[j]
	return a["_t"].(int64) < b["_t"].(int64)
}
func (rts *RecTimeSort) Swap(i, j int) {
	t := (*rts)[i]
	(*rts)[i] = (*rts)[j]
	(*rts)[j] = t
}

func (sums *Server) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	sums.l.RLock()
	sdat := sums.sum.GetSummedRecent(time.Now().Add(-2*time.Hour).UnixMilli(), 9999)
	raw_after := time.Now().Add(-10 * time.Minute).UnixMilli()
	rdat := sums.sum.GetRawRecent(raw_after, 9999)
	sums.l.RUnlock()
	alldata := mergeSummedRaw(sdat, rdat, raw_after)
	debug("GET %d sums, %d raw => %d merged", len(sdat), len(rdat), len(alldata))
	var sad RecTimeSort = alldata
	sort.Sort(&sad)
	var alldeltas []map[string]interface{} = nil
	if len(alldata) > 0 {
		alldeltas = make([]map[string]interface{}, 1, len(alldata))
		alldeltas[0] = alldata[0]
		for i := 1; i < len(alldata); i++ {
			alldeltas = append(alldeltas, recDiffI(alldata[i-1], alldata[i]))
		}
	}
	rdata := ReturnJSON{Data: alldeltas}
	out.Header().Set("Content-Type", "application/json")
	out.WriteHeader(200)
	enc := json.NewEncoder(out)
	enc.Encode(rdata)
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

	// TODO: if serveAddr != "", start server around a StreamingSummary and post records there
	doServe := serveAddr != ""
	var serv Server
	servChan := make(chan map[string]string, 10)
	if doServe {
		wg.Add(1)
		go serv.dataReceiver(servChan, wg)
		httpServer := http.Server{}
		httpServer.Addr = serveAddr
		httpServer.Handler = &serv
		go httpServer.ListenAndServe()
	}

	for {
		select {
		case rec, ok := <-recChan:
			if !ok {
				close(servChan)
				break
			}
			if doServe {
				select {
				case servChan <- rec:
					// ok
				default:
					// drop, sad, don't let broken server stop us
				}
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
