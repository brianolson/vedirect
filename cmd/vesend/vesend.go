package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/brianolson/vedirect"
)

//go:embed static
var sfs embed.FS

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
	if postUrl == "" && serveAddr == "" {
		fmt.Fprintf(os.Stderr, "one of '-post URL' or '-serve :port' is required\n")
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

type ReturnJSON struct {
	Data []map[string]interface{} `json:"d"`
}

func (sums *Server) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	sums.l.RLock()
	raw_after := time.Now().Add(-10 * time.Minute).UnixMilli()
	alldata := sums.sum.GetData(raw_after)
	sums.l.RUnlock()
	alldeltas := vedirect.ParsedRecordDeltas(alldata)
	rdata := ReturnJSON{Data: alldeltas}
	out.Header().Set("Content-Type", "application/json")
	out.WriteHeader(200)
	enc := json.NewEncoder(out)
	enc.Encode(rdata)
}

type StaticHandler struct {
	stripPrefix string
	newPrefix   string
	fsHandler   http.Handler
}

func (sh *StaticHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, sh.stripPrefix) && ((req.URL.RawPath == "") || strings.HasPrefix(req.URL.RawPath, sh.stripPrefix)) {
		req.URL.Path = strings.Replace(req.URL.Path, sh.stripPrefix, sh.newPrefix, 1)
		if req.URL.RawPath != "" {
			req.URL.RawPath = strings.Replace(req.URL.RawPath, sh.stripPrefix, sh.newPrefix, 1)
		}
		sh.fsHandler.ServeHTTP(rw, req)
	} else {
		log.Printf("Path=%#v RawPath=%#v, prefix=%#v", req.URL.Path, req.URL.RawPath, sh.stripPrefix)
		http.Error(rw, "nope", http.StatusNotFound)
	}
}

// receive data from Vedirect parser, sometimes poke the sendThread.
func mainThread(recChan <-chan map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	batch := make([]map[string]string, 0, sendPeriod)
	sendActive := false
	reqStart := make(chan sendRequest, 1)
	reqReturn := make(chan sendRequest, 1)
	wg.Add(1)
	doPost := false
	if postUrl != "" {
		doPost = true
		go sendThread(postUrl, reqStart, reqReturn, wg)
	}
	defer close(reqStart)

	var serv Server
	servChan := make(chan map[string]string, 10)
	doServe := false
	if serveAddr != "" {
		doServe = true
		wg.Add(1)
		go serv.dataReceiver(servChan, wg)
		mux := http.NewServeMux()
		sh := StaticHandler{stripPrefix: "/s/", newPrefix: "/static/", fsHandler: http.FileServer(http.FS(sfs))}
		mux.Handle("/s/", &sh)
		mux.Handle("/", &serv)
		httpServer := http.Server{
			Addr:    serveAddr,
			Handler: mux,
		}
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
			if doPost {
				now := time.Now()
				batch = append(batch, rec)
				if len(batch) >= sendPeriod && !sendActive {
					debug("try send %d recs", len(batch))
					msg := Message{
						Data: vedirect.StringRecordDeltas(batch, nil, sendPeriod),
					}
					reqStart <- sendRequest{msg: &msg, start: now}
					sendActive = true
				}
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
				req.msg.Data = vedirect.StringRecordDeltas(batch, req.msg.Data, sendPeriod)
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
