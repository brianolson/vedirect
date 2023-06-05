package vedirect

import "testing"

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
