package vedirect

import (
	"bufio"
	"math"
	"strings"
)

// Merge VE.Direct records based on time (e.g. 1 minute average of 1 second records), keep the most recent N.
//
// Different fields are merged on different rules, some are averaged, some are last-value-wins
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

	// time.Time.Unix() after which the next bin starts
	bs0LimitUnix int64

	// rawRecent holds a few bins of raw data
	// [N][BinSeconds]map[string]interface{}
	// rawRecent[0] is currently-building recent records
	rawRecent [][]map[string]interface{}
	rawCache  int // ~10

	// time.Time.Unix() after which the next bin starts
	binLimitUnix int64
}

func (sum *StreamingSummary) Add(rec map[string]interface{}) {
	rec_tx, ok := rec["_t"]
	if !ok {
		// WARNING ERROR ETC, cannot add without time
		return
	}
	rec_t, ok := rec_tx.(int64)
	if !ok {
		// WARNING ERROR ETC, cannot add without time
		return
	}

	if sum.rawRecent == nil {
		if sum.rawCache == 0 {
			sum.rawCache = 10
		}
		sum.rawRecent = make([][]map[string]interface{}, 1, sum.rawCache)
		sum.startRR0(rec, rec_t)
		return
	}
	if (rec_t / 1000) > sum.binLimitUnix {
		// next bin!
		sum.addSum(summarize(sum.rawRecent[0]))
		sum.rotateRawRecent()
		sum.startRR0(rec, rec_t)
		return
	}
	sum.rawRecent[0] = append(sum.rawRecent[0], rec)
}

func (sum *StreamingSummary) startRR0(rec map[string]interface{}, rec_t int64) {
	sum.rawRecent[0] = make([]map[string]interface{}, 1, sum.BinSeconds)
	sum.rawRecent[0][0] = rec
	sum.binLimitUnix = int64((math.Floor(float64(rec_t)/(float64(sum.BinSeconds)*1000.0)) + 1.0) * float64(sum.BinSeconds))
}

func (sum *StreamingSummary) rotateRawRecent() {
	if len(sum.rawRecent) < sum.rawCache {
		sum.rawRecent = append(sum.rawRecent, nil)
	}
	for i := len(sum.rawRecent) - 1; i >= 1; i-- {
		sum.rawRecent[i] = sum.rawRecent[i-1]
	}
	sum.rawRecent[0] = nil
}

func (sum *StreamingSummary) summaryBins() int {
	summaryBins := sum.KeepCount / sum.summaryChunkSize
	if summaryBins == 0 {
		return 1
	}
	return summaryBins
}

func (sum *StreamingSummary) addSum(rec map[string]interface{}) {
	rec_tx, ok := rec["_t"]
	if !ok {
		// WARNING ERROR ETC, cannot add without time
		return
	}
	rec_t, ok := rec_tx.(int64)
	if !ok {
		// WARNING ERROR ETC, cannot add without time
		return
	}

	if sum.binnedSummaries == nil {
		if sum.summaryChunkSize == 0 {
			sum.summaryChunkSize = 500
		}
		summaryBins := sum.summaryBins()
		sum.binnedSummaries = make([][]map[string]interface{}, 1, summaryBins)
		sum.startBS0(rec, rec_t)
		return
	}
	if (rec_t / 1000) > sum.binLimitUnix {
		// next bin!
		sum.rotateBinnedSummaries()
		sum.startBS0(rec, rec_t)
		return
	}
	sum.binnedSummaries[0] = append(sum.binnedSummaries[0], rec)
}

func (sum *StreamingSummary) startBS0(rec map[string]interface{}, rec_t int64) {
	sum.binnedSummaries[0] = make([]map[string]interface{}, 1, sum.BinSeconds)
	sum.binnedSummaries[0][0] = rec
	sum.binLimitUnix = int64((math.Floor(float64(rec_t)/(float64(sum.BinSeconds)*1000.0)) + 1.0) * float64(sum.BinSeconds))
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

// get the newest records, up to limit
func (sum *StreamingSummary) GetRawRecent(limit int) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, limit)
	for _, subset := range sum.rawRecent {
		for i := len(subset) - 1; i >= 0; i-- {
			out = append(out, subset[i])
			if len(out) >= limit {
				return out
			}
		}
	}
	return out
}

// get the newest records, up to limit
func (sum *StreamingSummary) GetSummedRecent(limit int) []map[string]interface{} {
	return nil
}

// mean - average of data points
// mode - most common data value
// last - last data value
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
	for _, rec := range they {
		for k := range rec {
			allKeys[k] = true
		}
	}
	out := make(map[string]interface{}, len(allKeys))
	for k := range allKeys {
		smode := summaryModes[k]
		if smode == "mean" {
			doMean(they, k, out)
		} else if smode == "last" {
			doLast(they, k, out)
		} else if smode == "mode" {
			doMode(they, k, out)
		}
	}
	return out
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
		case float64:
			fsum += nv
			fcount += 1
		case string:
			// TODO: warning
		default:
			// TODO: error, warning, etc
		}
	}
	if icount != 0 {
		if fcount != 0 {
			// TODO: error, warning, etc
			out[k] = (fsum + float64(isum)) / float64(icount+fcount)
		} else {
			out[k] = float64(isum) / float64(icount)
		}
	} else if fcount != 0 {
		out[k] = fsum / float64(fcount)
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
