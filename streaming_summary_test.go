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
