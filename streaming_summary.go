package vedirect

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

const DefaultKeepCount = 20000
const DefaultBinSeconds = 60
const defaultSummaryChunkSize = 500
const defaultRawCache = 10

var DebugEnabled bool = false
var DebugWriter io.Writer = os.Stderr

func debug(x string, args ...interface{}) {
	if DebugEnabled {
		fmt.Fprintf(DebugWriter, x+"\n", args...)
	}
}

var ErrNotANumber = errors.New("could not convert non-number to int64")

func numToInt64(x any) (v int64, err error) {
	switch iv := x.(type) {
	case int:
		v = int64(iv)
		return
	case int32:
		v = int64(iv)
		return
	case int64:
		v = iv
		return
	case uint:
		v = int64(iv)
		return
	case uint32:
		v = int64(iv)
		return
	case uint64:
		v = int64(iv)
		return
	case float32:
		v = int64(iv)
		return
	case float64:
		v = int64(iv)
		return
	case string:
		v, err = strconv.ParseInt(iv, 10, 64)
		return
	default:
		err = ErrNotANumber
		return
	}
}

// Merge VE.Direct records based on time (e.g. 1 minute average of 1 second records), keep the most recent N.
//
// # Different fields are merged on different rules, some are averaged, some are last-value-wins
//
// BinSeconds * KeepCount is the amonut of total time covered. {BinSeconds:60, KeepCount: 24*60} will merge data into 1 minute bins and keep the most recent 24 hours of data.
type StreamingSummary struct {
	// BinSeconds is the number of seconds of raw samples to merge
	BinSeconds int

	// KeepCount is the number of merged samples to keep
	KeepCount int

	// binned summaries is sets of summarized data
	// [N][summaryChunkSize]map[string]interface{}
	binnedSummaries  [][]map[string]interface{}
	summaryChunkSize int // ~500
	numSummaryBins   int

	// rawRecent holds a few bins of raw data
	// [rawCache][BinSeconds]map[string]interface{}
	// rawRecent[0] is currently-building recent records
	rawRecent [][]map[string]interface{}
	rawCache  int // default 10

	// time.Time.UnixMilli() after which the next bin starts
	binLimitUnixMilli int64
}

var ErrNoTime = errors.New("record lacks _t time")
var ErrTimeWrongType = errors.New("_t record wrong type not int64")

func (sum *StreamingSummary) Add(rec map[string]interface{}) error {
	rec_tx, ok := rec["_t"]
	if !ok {
		debug("cannot add record without _t time")
		// WARNING ERROR ETC, cannot add without time
		return ErrNoTime
	}
	rec_t, err := numToInt64(rec_tx)
	if err != nil {
		debug("%v: record _t is not int64, got %T %#v", err, rec_tx, rec_tx)
		// WARNING ERROR ETC, cannot add without time
		return err
	}

	if sum.rawRecent == nil {
		if sum.rawCache == 0 {
			sum.rawCache = defaultRawCache
			debug("rawCache = %d", sum.rawCache)
		}
		sum.rawRecent = make([][]map[string]interface{}, 1, sum.rawCache)
		sum.startRR0(rec, rec_t)
		return nil
	}
	if rec_t > sum.binLimitUnixMilli {
		// next bin!
		sum.addSum(summarize(sum.rawRecent[0]))
		sum.rotateRawRecent()
		sum.startRR0(rec, rec_t)
		return nil
	}
	sum.rawRecent[0] = append(sum.rawRecent[0], rec)
	return nil
}

func (sum *StreamingSummary) startRR0(rec map[string]interface{}, rec_t int64) {
	if sum.BinSeconds == 0 {
		sum.BinSeconds = DefaultBinSeconds
	}
	sum.rawRecent[0] = make([]map[string]interface{}, 1, sum.BinSeconds)
	sum.rawRecent[0][0] = rec
	sum.binLimitUnixMilli = 1000 * int64((math.Floor(float64(rec_t)/(float64(sum.BinSeconds*1000)))+1.0)*float64(sum.BinSeconds))
	debug("startRR0 rec_t %d binLimit %d", rec_t, sum.binLimitUnixMilli)
}

func (sum *StreamingSummary) rotateRawRecent() {
	// if less than rawCache blocks of raw, grow
	if len(sum.rawRecent) < sum.rawCache {
		sum.rawRecent = append(sum.rawRecent, nil)
	}
	// move everything down (drops last if too many)
	for i := len(sum.rawRecent) - 1; i >= 1; i-- {
		sum.rawRecent[i] = sum.rawRecent[i-1]
	}
	// clear next slot, see startRR0
	sum.rawRecent[0] = nil
}

func (sum *StreamingSummary) summaryBins() int {
	if sum.numSummaryBins == 0 {
		if sum.KeepCount == 0 {
			sum.KeepCount = DefaultKeepCount
		}
		summaryBins := sum.KeepCount / sum.summaryChunkSize
		for summaryBins*sum.summaryChunkSize < sum.KeepCount {
			summaryBins += 1
		}
		sum.numSummaryBins = summaryBins + 1
	}
	return sum.numSummaryBins
}

func (sum *StreamingSummary) addSum(rec map[string]interface{}) {
	rec_tx, ok := rec["_t"]
	if !ok {
		// WARNING ERROR ETC, cannot add without time
		return
	}
	rec_t, err := numToInt64(rec_tx)
	if err != nil {
		// WARNING ERROR ETC, cannot add without time
		return
	}

	if sum.binnedSummaries == nil {
		if sum.summaryChunkSize == 0 {
			sum.summaryChunkSize = defaultSummaryChunkSize
		}
		summaryBins := sum.summaryBins()
		sum.binnedSummaries = make([][]map[string]interface{}, 1, summaryBins)
		sum.startBS0(rec, rec_t)
		return
	}
	if len(sum.binnedSummaries[0]) >= sum.summaryChunkSize {
		// next bin!
		sum.rotateBinnedSummaries()
		sum.startBS0(rec, rec_t)
		return
	}
	sum.binnedSummaries[0] = append(sum.binnedSummaries[0], rec)
}

func (sum *StreamingSummary) startBS0(rec map[string]interface{}, rec_t int64) {
	sum.binnedSummaries[0] = make([]map[string]interface{}, 1, sum.summaryChunkSize)
	sum.binnedSummaries[0][0] = rec
}

func (sum *StreamingSummary) rotateBinnedSummaries() {
	summaryBins := sum.summaryBins()
	if len(sum.binnedSummaries) < summaryBins {
		sum.binnedSummaries = append(sum.binnedSummaries, nil)
	}
	for i := len(sum.binnedSummaries) - 1; i >= 1; i-- {
		sum.binnedSummaries[i] = sum.binnedSummaries[i-1]
	}
	sum.binnedSummaries[0] = nil
}

// get the newest records, up to limit.
//
// after is time.Time.UnixMilli()
func (sum *StreamingSummary) GetRawRecent(after int64, limit int) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, limit)
	for _, subset := range sum.rawRecent {
		for i := len(subset) - 1; i >= 0; i-- {
			rec := subset[i]
			rec_t, _ := numToInt64(rec["_t"])
			if rec_t <= after {
				return out
			}
			out = append(out, rec)
			if len(out) >= limit {
				return out
			}
		}
	}
	return out
}

func (sum *StreamingSummary) allRawData() []map[string]interface{} {
	count := 0
	for _, subset := range sum.rawRecent {
		count += len(subset)
	}
	out := make([]map[string]interface{}, 0, count)
	for _, subset := range sum.rawRecent {
		for i := len(subset) - 1; i >= 0; i-- {
			rec := subset[i]
			out = append(out, rec)
		}
	}
	return out
}

// get the newest records, up to limit
// the _t time for a summary is the _last_ time of any sample within the summarized range, thus you can query GetRawRecent after= from a summed _t value
func (sum *StreamingSummary) GetSummedRecent(after int64, limit int) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, limit)
	for _, subset := range sum.binnedSummaries {
		for i := len(subset) - 1; i >= 0; i-- {
			rec := subset[i]
			rec_t, _ := numToInt64(rec["_t"])
			if rec_t <= after {
				return out
			}
			out = append(out, rec)
			if len(out) >= limit {
				return out
			}
		}
	}
	return out
}

func (sum *StreamingSummary) allSumData() []map[string]interface{} {
	count := 0
	for _, subset := range sum.binnedSummaries {
		count += len(subset)
	}
	out := make([]map[string]interface{}, 0, count)
	for _, subset := range sum.binnedSummaries {
		for i := len(subset) - 1; i >= 0; i-- {
			rec := subset[i]
			out = append(out, rec)
		}
	}
	return out
}

type RecTimeSort []map[string]interface{}

// Len is part of sort.Interface
func (rts *RecTimeSort) Len() int {
	return len(*rts)
}
func (rts *RecTimeSort) Less(i, j int) bool {
	a := (*rts)[i]
	b := (*rts)[j]
	at, _ := numToInt64(a["_t"])
	bt, _ := numToInt64(b["_t"])
	return at < bt
}
func (rts *RecTimeSort) Swap(i, j int) {
	t := (*rts)[i]
	(*rts)[i] = (*rts)[j]
	(*rts)[j] = t
}

// GetData returns a merged set of data with merged samples before some time and raw samples after.
func (sum *StreamingSummary) GetData(raw_after int64) []map[string]interface{} {
	// TODO: bring back a split version of this to have shorter mutex shadow as used in vesend.go, do the following to copying data gets, release the lock, then do the merge
	sdat := sum.allSumData()
	rdat := sum.allRawData()
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
		t, _ := numToInt64(rec["_t"])
		if t > sumNewest && t < raw_after {
			sumNewest = t
		}
	}
	if sumNewest == 0 {
		debug("merge all raw")
		return rdat
	}
	rawOldest, _ := numToInt64(rdat[0]["_t"])
	for _, rec := range rdat {
		t, _ := numToInt64(rec["_t"])
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
			t, _ := numToInt64(rec["_t"])
			if t < sumKey && t > rawOldest {
				sumKey = t
				//sumKeyI = i
			}
		}
		alldata = make([]map[string]interface{}, 0, len(sdat)+len(rdat))
		for _, rec := range sdat {
			t, _ := numToInt64(rec["_t"])
			if t <= sumKey {
				alldata = append(alldata, rec)
			}
		}
		for _, rec := range rdat {
			t, _ := numToInt64(rec["_t"])
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
	debug("GetData %d sums, %d raw => %d merged", len(sdat), len(rdat), len(alldata))
	var sad RecTimeSort = alldata
	sort.Sort(&sad)
	return alldata
}

// mean - average of data points
// mode - most common data value
// last - last data value
// max - greatest value
// min - least value
const fieldSummaryModes = `V mean
V2 mean
V3 mean
VS mean
VM mean
DM mean
VPV mean
PPV mean
I mean
I2 mean
I3 mean
IL mean
T mean
P mean
CE last
SOC mean
TTG last
H1 last
H2 last
H3 last
H4 last
H5 last
H6 last
H7 last
H8 last
H9 last
H10 last
H11 last
H12 last
H13 last
H14 last
H15 last
H16 last
H17 last
H18 last
H19 last
H20 last
H21 last
H22 last
H23 last
HSDS last
AC_OUT_V mean
AC_OUT_I mean
AC_OUT_S mean
LOAD mode
Alarm mode
Relay mode
AR mode
OR mode
ERR mode
CS mode
BMV last
FW last
FWE last
PID last
SER# last
MODE mode
WARN mode
MPPT mode
MON mode
_t last
`

var summaryModes map[string]string

func init() {
	summaryModes = make(map[string]string, 30)
	fin := strings.NewReader(fieldSummaryModes)
	sc := bufio.NewScanner(fin)
	for sc.Scan() {
		line := sc.Text()
		if len(line) == 0 {
			continue
		}
		a, b, didCut := strings.Cut(line, " ")
		if didCut {
			summaryModes[a] = b
		} else {
			summaryModes[line] = ""
		}
	}
}

func summarize(they []map[string]interface{}) map[string]interface{} {
	allKeys := make(map[string]bool)
	xcount := 0
	for _, rec := range they {
		for k := range rec {
			allKeys[k] = true
			if k == "_x" {
				xcount++
			}
		}
	}
	hexKeys := make(map[string]bool)
	hexThey := make([]map[string]interface{}, 0, xcount)
	hexModes := make(map[string]string)
	for _, rec := range they {
		xv, ok := rec["_x"]
		if !ok {
			continue
		}
		xs, ok := xv.(string)
		if !ok {
			continue
		}
		value, err := ParseHexRecord(xs)
		if err != nil {
			continue
		}
		if value.Register.SummaryMode == "" {
			continue
		}
		hexModes[value.Register.Name] = value.Register.SummaryMode
		hexKeys[value.Register.Name] = true
		theyrec := make(map[string]interface{}, 1)
		theyrec[value.Register.Name] = value.Value
		hexThey = append(hexThey, theyrec)
	}
	out := make(map[string]interface{}, len(allKeys)+len(hexKeys))
	summaryInner(allKeys, summaryModes, they, out)
	if len(hexKeys) > 0 {
		summaryInner(hexKeys, hexModes, hexThey, out)
	}
	return out
}

func summaryInner(allKeys map[string]bool, modes map[string]string, they []map[string]interface{}, out map[string]interface{}) {
	for k := range allKeys {
		if k == "_x" {
			continue
		}
		smode := modes[k]
		if smode == "mean" {
			doMean(they, k, out)
		} else if smode == "last" {
			doLast(they, k, out)
		} else if smode == "mode" {
			doMode(they, k, out)
		} else if smode == "min" {
			doMin(they, k, out)
		} else if smode == "max" {
			doMax(they, k, out)
		} else {
			debug("key %#v unk sum mode %#v", k, smode)
		}
	}
}

func doMean(they []map[string]interface{}, k string, out map[string]interface{}) {
	isum := int64(0)
	icount := 0
	fsum := float64(0)
	fcount := 0
	for _, rec := range they {
		v, has := rec[k]
		if !has {
			continue
		}
		switch nv := v.(type) {
		case int64:
			isum += nv
			icount += 1
		case int16:
			isum += int64(nv)
			icount += 1
		case int32:
			isum += int64(nv)
			icount += 1
		case uint16:
			isum += int64(nv)
			icount += 1
		case uint32:
			isum += int64(nv)
			icount += 1
		case float32:
			fsum += float64(nv)
			fcount += 1
		case float64:
			fsum += nv
			fcount += 1
		case string:
			debug("doMean %#v got string %#v", k, nv)
		default:
			debug("doMean %#v got %T %#v", k, v, v)
		}
	}
	if icount != 0 {
		if fcount != 0 {
			out[k] = (fsum + float64(isum)) / float64(icount+fcount)
		} else {
			out[k] = float64(isum) / float64(icount)
		}
	} else if fcount != 0 {
		out[k] = fsum / float64(fcount)
	}
}

func doMax(they []map[string]interface{}, k string, out map[string]interface{}) {
	mv := float64(0.0)
	first := true
	for _, rec := range they {
		v, has := rec[k]
		if !has {
			continue
		}
		var fv float64
		has = false
		switch nv := v.(type) {
		case int64:
			fv = float64(nv)
			has = true
		case int32:
			fv = float64(nv)
			has = true
		case float32:
			fv = float64(nv)
			has = true
		case float64:
			fv = float64(nv)
			has = true
		default:
		}
		if has {
			if first {
				mv = fv
				first = false
			} else if fv > mv {
				mv = fv
			}
		}
	}
	if !first {
		out[k] = mv
	}
}

func doMin(they []map[string]interface{}, k string, out map[string]interface{}) {
	mv := float64(0.0)
	first := true
	for _, rec := range they {
		v, has := rec[k]
		if !has {
			continue
		}
		var fv float64
		has = false
		switch nv := v.(type) {
		case int64:
			fv = float64(nv)
			has = true
		case int32:
			fv = float64(nv)
			has = true
		case float32:
			fv = float64(nv)
			has = true
		case float64:
			fv = float64(nv)
			has = true
		default:
		}
		if has {
			if first {
				mv = fv
				first = false
			} else if fv < mv {
				mv = fv
			}
		}
	}
	if !first {
		out[k] = mv
	}
}

func doLast(they []map[string]interface{}, k string, out map[string]interface{}) {
	for i := len(they) - 1; i >= 0; i-- {
		v, has := they[i][k]
		if has {
			out[k] = v
			return
		}
	}
}

func doMode(they []map[string]interface{}, k string, out map[string]interface{}) {
	counts := make(map[interface{}]int)
	maxcount := 0
	var maxv interface{} = nil
	for i := 0; i < len(they); i++ {
		v, has := they[i][k]
		if !has {
			continue
		}
		nc := counts[v] + 1
		if nc > maxcount {
			maxcount = nc
			maxv = v
		}
		counts[v] = nc
	}
	if maxv != nil {
		out[k] = maxv
	}
}
