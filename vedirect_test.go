package vedirect

import (
	"encoding/json"
	"testing"
)

func TestInit(t *testing.T) {
	if len(IntFields) < 20 {
		t.Errorf("expected more IntFields, got %d", len(IntFields))
	}
	if len(OtherFields) < 10 {
		t.Errorf("expected more OtherFields, got %d", len(IntFields))
	}
}

type wholeUnitsCase struct {
	v  int64
	vu string
	fv float64
	fu string
}

var wuCases = []wholeUnitsCase{
	{10_000, "mV", 10.0, "V"},
	{37, "farts", 37.0, "farts"},
	{10_000, "mAh", 10.0, "Ah"},
	{37, "", 37.0, ""},
	{10_0, "0.1A", 10.0, "A"},
	{10_00, "0.01kWh", 10.0, "kWh"},
	{10_00, "0.01V", 10.0, "V"},
}

func TestFloatWholeUnits(t *testing.T) {
	for i, tc := range wuCases {
		fv, fu := FloatWholeUnits(tc.v, tc.vu)
		if fv != tc.fv || fu != tc.fu {
			t.Errorf("wu[%d] (%d, %s) -> (%f, %s) but wanted (%f, %s)", i, tc.v, tc.vu, fv, fu, tc.fv, tc.fu)
		}
	}
}

func TestHexCommand(t *testing.T) {
	msg := []byte{0xf0, 0xed, 0x00}
	command := string(formatHexCommand(Get, msg))
	expected := ":7F0ED0071\n"
	if command != expected {
		t.Errorf("got command %#v, wanted %#v", command, expected)
	}
}

func TestParsedRecordDeltas(t *testing.T) {
	jsons := []string{
		`{"a":"a1","b":"b1","_t":1}`,
		`{"a":"a1","b":"b2","_t":2}`,
		`{"_x":"AABBCCEEFF","_t":3}`,
		`{"a":"a2","b":"b2","_t":4}`,
	}
	expected := []string{
		"{\"_t\":1,\"a\":\"a1\",\"b\":\"b1\"}",
		"{\"_t\":2,\"b\":\"b2\"}",
		"{\"_t\":3,\"_x\":\"AABBCCEEFF\"}",
		"{\"_t\":4,\"a\":\"a2\"}",
	}
	data := make([]map[string]interface{}, len(jsons))
	for i, jsoni := range jsons {
		rec := make(map[string]interface{})
		json.Unmarshal([]byte(jsoni), &rec)
		data[i] = rec
	}
	out := ParsedRecordDeltas(data)
	actual := make([]string, len(out))
	for i, drec := range out {
		blob, _ := json.Marshal(drec)
		actual[i] = string(blob)
	}
	for i, ev := range expected {
		av := actual[i]
		if av != ev {
			t.Errorf("expected[i] = %#v, got %#v", ev, av)
		}
	}
}

func TestStringRecordDeltas(t *testing.T) {
	jsons := []string{
		`{"a":"a1","b":"b1","_t":"1"}`,
		`{"a":"a1","b":"b2","_t":"2"}`,
		`{"_x":"AABBCCEEFF","_t":"3"}`,
		`{"a":"a2","b":"b2","_t":"4"}`,
	}
	expected := []string{
		"{\"_t\":1,\"a\":\"a1\",\"b\":\"b1\"}",
		"{\"_t\":2,\"b\":\"b2\"}",
		"{\"_t\":3,\"_x\":\"AABBCCEEFF\"}",
		"{\"_t\":4,\"a\":\"a2\"}",
	}
	data := make([]map[string]string, len(jsons))
	for i, jsoni := range jsons {
		rec := make(map[string]string)
		json.Unmarshal([]byte(jsoni), &rec)
		data[i] = rec
	}
	out := StringRecordDeltas(data, nil, 9999)
	actual := make([]string, len(out))
	for i, drec := range out {
		blob, _ := json.Marshal(drec)
		actual[i] = string(blob)
	}
	for i, ev := range expected {
		av := actual[i]
		if av != ev {
			t.Errorf("expected[i] = %#v, got %#v", ev, av)
		}
	}
}
