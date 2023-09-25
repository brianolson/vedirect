package main

import (
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/brianolson/vedirect"
	"github.com/fsnotify/fsnotify"
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

// type ReturnJSON struct {
// 	Data []map[string]interface{} `json:"d"`
// }

var (
	serveAddr   string
	archiveDir  string
	filePattern string
	verbose     bool

	pathMatcher *regexp.Regexp
)

func main() {
	flag.BoolVar(&verbose, "v", false, "verbose debug out")
	flag.StringVar(&serveAddr, "serve", "", "host:port to serve http API from")
	flag.StringVar(&archiveDir, "dir", "", "archive dir full of .json.gz")
	flag.StringVar(&filePattern, "pat", ".*\\.json\\.gz", "Go regexp to match archive files in dir")
	flag.Parse()
	vedirect.DebugEnabled = verbose

	var err error
	pathMatcher, err = regexp.Compile(filePattern)
	maybefail(err, "bad file pattern regexp: %v", err)

	ctx, cf := context.WithCancel(context.Background())
	defer cf()
	var wg sync.WaitGroup
	created := make(chan string, 10)
	wg.Add(1)
	go watcherThread(ctx, created, &wg)

	readyPaths := make(chan string, 10)
	wg.Add(1)
	go createdWatcher(ctx, created, readyPaths, &wg)

	serv := Server{}
	err = serv.loadDir(archiveDir)
	maybefail(err, "%#v: loaddir, %v", archiveDir, err)

	wg.Add(1)
	go serv.readyThread(ctx, readyPaths, &wg)

	mux := http.NewServeMux()
	sh := StaticHandler{stripPrefix: "/s/", newPrefix: "/static/", fsHandler: http.FileServer(http.FS(sfs))}
	mux.Handle("/s/", &sh)
	mux.Handle("/", &serv)
	httpServer := http.Server{
		Addr:    serveAddr,
		Handler: mux,
	}
	go httpServer.ListenAndServe()

	wg.Wait()
}

type nameMtime struct {
	name  string
	mtime time.Time
}

type nameMtimeNewestFirst []nameMtime

// sort.Interface
func (they *nameMtimeNewestFirst) Len() int {
	return len(*they)
}
func (they *nameMtimeNewestFirst) Less(i, j int) bool {
	return (*they)[i].mtime.Before((*they)[j].mtime)
}
func (they *nameMtimeNewestFirst) Swap(i, j int) {
	t := (*they)[i]
	(*they)[i] = (*they)[j]
	(*they)[j] = t
}

func (sums *Server) loadDir(dirpath string) error {
	now := time.Now()
	newestWeek := now.Add(-3 * 24 * time.Hour)
	ents, err := os.ReadDir(dirpath)
	if err != nil {
		return fmt.Errorf("%#v: readdir, %w", dirpath, err)
	}
	nm := make([]nameMtime, 0, len(ents))
	for _, ent := range ents {
		match := pathMatcher.MatchString(ent.Name())
		if !match {
			debug("re skip: %#v", ent.Name())
			continue
		}
		fi, err := ent.Info()
		if err != nil {
			log.Printf("%#v: stat, %v", ent.Name(), err)
		}
		mtime := fi.ModTime()
		if mtime.After(newestWeek) {
			nm = append(nm, nameMtime{fi.Name(), mtime})
		}
	}
	debug("%s: loading %d files", dirpath, len(nm))
	si := (*nameMtimeNewestFirst)(&nm)
	sort.Sort(si)
	var errs []error
	recCount := 0
	for _, nmi := range nm {
		path := filepath.Join(dirpath, nmi.name)
		rc, err := sums.loadFile(path)
		if err != nil {
			log.Printf("%#v: %v", path, err)
			errs = append(errs, err)
			if len(errs) >= 10 {
				// too many errors
				break
			}
		}
		recCount += rc
	}
	dt := time.Now().Sub(now)
	debug("%s: loaded %d files (%d rec) in %s", dirpath, len(nm), recCount, dt)
	if len(errs) > 0 {
		return fmt.Errorf("%#v: %d errors", dirpath, len(errs))
	}
	return nil
}

type Server struct {
	sum vedirect.StreamingSummary
	l   sync.RWMutex

	loadedPaths []string
}

func (sums *Server) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	sums.l.RLock()
	raw_after := time.Now().Add(-10 * time.Minute).UnixMilli()
	alldata := sums.sum.GetData(raw_after)
	sums.l.RUnlock()
	alldeltas := vedirect.ParsedRecordDeltas(alldata)
	rdata := Message{Data: alldeltas}
	out.Header().Set("Content-Type", "application/json")
	out.WriteHeader(200)
	enc := json.NewEncoder(out)
	enc.Encode(rdata)
}

func parseRecordAnyInPlace(kv map[string]any) {
	for k, v := range kv {
		kv[k] = vedirect.ParseRecordField(k, v)
	}
}

func (sums *Server) alreadyLoaded(path string) bool {
	sums.l.Lock()
	defer sums.l.Unlock()
	for _, v := range sums.loadedPaths {
		if v == path {
			return true
		}
	}
	return false
}
func (sums *Server) loadFile(path string) (int, error) {
	if sums.alreadyLoaded(path) {
		return 0, nil
	}
	ffin, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("%v: open, %w", path, err)
	}
	defer ffin.Close()
	var fin io.Reader
	if strings.HasSuffix(path, ".gz") {
		fin, err = gzip.NewReader(ffin)
		if err != nil {
			return 0, fmt.Errorf("%v: gzip, %w", path, err)
		}
	} else {
		fin = ffin
	}
	dec := json.NewDecoder(fin)
	var msg Message
	err = dec.Decode(&msg)
	if err != nil {
		return 0, fmt.Errorf("%v: json, %w", path, err)
	}
	if len(msg.Data) > 0 {
		for i := range msg.Data {
			parseRecordAnyInPlace(msg.Data[i])
		}
		vedirect.ParsedRecordRebuild(msg.Data)
		sums.l.Lock()
		defer sums.l.Unlock()
		for _, v := range sums.loadedPaths {
			if v == path {
				return 0, nil
			}
		}
		for _, rec := range msg.Data {
			sums.sum.Add(rec)
		}
		sums.loadedPaths = append(sums.loadedPaths, path)
	}
	return len(msg.Data), nil
}
func (sums *Server) readyThread(ctx context.Context, ready <-chan string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	done := ctx.Done()
	for {
		select {
		case <-done:
			return
		case path, ok := <-ready:
			if !ok {
				return
			}
			sums.loadFile(path)
		}
	}
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

func watcherThread(ctx context.Context, created chan<- string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	watcher, err := fsnotify.NewWatcher()
	maybefail(err, "NewWatcher, %v", err)
	defer watcher.Close()

	err = watcher.Add(archiveDir)
	maybefail(err, "watch dir %#v: %v", archiveDir, err)

	done := ctx.Done()
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op == fsnotify.Create {
				match := pathMatcher.MatchString(event.Name)
				if !match {
					debug("re skip created: %#v", event.Name)
					continue
				}
				log.Printf("created: %v", event.Name)
				created <- event.Name
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("watcherr: %v", err)
		case <-done:
			return
		}
	}
}

func createdWatcher(ctx context.Context, created <-chan string, ready chan<- string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	var pending []string
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	done := ctx.Done()
	for {
		select {
		case <-done:
			return
		case path, ok := <-created:
			if !ok {
				return
			}
			fi, err := os.Stat(path)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					// created then disappeared, forget it
				} else {
					log.Printf("%#v: %v", path, err)
				}
				continue
			}
			mtime := fi.ModTime()
			oktime := time.Now().Add(-10 * time.Second)
			if mtime.Before(oktime) {
				ready <- path
			} else {
				pending = append(pending, path)
			}
		case now := <-ticker.C:
			oktime := now.Add(-10 * time.Second)
			// re-stat pending
			i := 0
			for i < len(pending) {
				path := pending[i]
				fi, err := os.Stat(path)
				drop := false
				if err != nil {
					// drop path
					log.Printf("%#v: %v", path, err)
					drop = true
				} else if fi.ModTime().Before(oktime) {
					ready <- path
					drop = true
				}
				if drop {
					if len(pending)-1 != i {
						pending[i] = pending[len(pending)-1]
					}
					pending = pending[:len(pending)-1]
				} else {
					i++
				}
			}
		}
	}
}
