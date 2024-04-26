package vedirect

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAllRegs(t *testing.T) {
	regs := allRegs()
	if len(regs) == 0 {
		t.Fail()
	}
	ns := strings.Builder{}
	enc := json.NewEncoder(&ns)
	enc.SetIndent("", "  ")
	err := enc.Encode(regs)
	if err != nil {
		t.Fatalf("json enc err: %s", err)
	}
	t.Log(ns.String())
}
