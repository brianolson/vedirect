package vedirect

import (
	"encoding/json"
	"testing"
)

// V mV mean
// H1 mAh last
// CS "" mode
var theystrs = []string{
	`{"V":"10000","H1":"10000", "CS":"1"}`,
	`{"V":"20000","H1":"20000", "CS":"2"}`,
	`{"V":"30000","H1":"30000", "CS":"1"}`,
	`{"V":"40000","H1":"40000", "CS":"2"}`,
	`{"V":"50000","H1":"50000", "CS":"1"}`,
}

func TestSummarize(t *testing.T) {
	theys := make([]map[string]string, len(theystrs))
	for i := range theys {
		err := json.Unmarshal([]byte(theystrs[i]), &theys[i])
		if err != nil {
			t.Fatalf("json: %v", err)
		}
	}
	they := make([]map[string]interface{}, len(theys))
	for i, a := range theys {
		they[i] = ParseRecord(a)
	}
	wat := summarize(they)
	if wat["V"] != float64(30000) {
		t.Errorf("wat[V] got %#v", wat["V"])
	}
	if wat["H1"] != int64(50000) {
		t.Errorf("wat[H1] got %#v", wat["H1"])
	}
	if wat["CS"] != "1" {
		t.Errorf("wat[CS] got %#v", wat["CS"])
	}
}

func TestStreamingSummary(t *testing.T) {
	sum := StreamingSummary{}
	for i := 0; i < 66; i++ {
		rec := make(map[string]interface{}, 1)
		// advance each by one second
		rec["_t"] = (int64(i) * 1000) + 1
		// [10000, 30000, repeat...] average 20000 (20V)
		rec["V"] = int64(10000 + (20000 * (i % 2)))
		sum.Add(rec)
		t.Logf("Add %#v", rec)
	}
	raw := sum.GetRawRecent(-1, 99)
	if 66 != len(raw) {
		t.Errorf("GetRawRecent len=%d", len(raw))
	}
	sums := sum.GetSummedRecent(-1, 99)
	eq(t, 1, len(sums))
	v := sums[0]["V"].(float64)
	if (v < 19999) || (v > 20001) {
		t.Errorf("bad V Average %f", v)
	}
	t.Logf("sums[0] %#v", sums[0])

	// after summarizing the first 60 seconds, we should have 6 more raw seconds to read
	raw = sum.GetRawRecent(sums[0]["_t"].(int64), 99)
	if 6 != len(raw) {
		t.Errorf("GetRawRecent 6 len=%d", len(raw))
	}

	// TODO: check at the capacity limit that stuff is getting dropped correctly
}

func eq(t *testing.T, expected, actual interface{}) {
	if expected == actual {
		return
	}
	t.Errorf("wanted %#v, got %#v", expected, actual)
}
