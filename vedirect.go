// go get github.com/brianolson/vedirect
//
// with learnings from https://github.com/karioja/vedirect by Janne Kario

package vedirect

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tarm/serial"
)

type vedState int

const (
	waitHeader vedState = iota
	hex
	inKey
	inValue
	inChecksum
)

const (
	hexmarker = ':'
	delimiter = '\t'
)

type VedirectOption int

const (
	AddTime VedirectOption = 1
)

type Vedirect struct {
	// AddTime if true will add to each record {"_t": time.Now().UnixMilli()}
	AddTime bool

	fin io.Reader

	dout io.Writer

	ctx context.Context

	bytesSum uint

	data map[string]string

	out chan<- map[string]string

	key []byte

	value []byte

	state vedState

	wg *sync.WaitGroup
}

// Open a VE.Direct serial device (starts a thread).
// path is the serial device.
// out chan receives data.
// wg may be nil.
// debugOut may be nil.
func Open(path string, out chan<- map[string]string, wg *sync.WaitGroup, ctx context.Context, debugOut io.Writer, options ...VedirectOption) (v *Vedirect, err error) {
	v = new(Vedirect)
	v.dout = debugOut
	st, err := os.Stat(path)
	if err != nil {
		v.debug("%s: could not stat, %v", path, err)
		return nil, err
	}
	var fin io.Reader
	if (st.Mode() & charDevice) == charDevice {
		v.debug("%s: is char device", path)
		sc := serial.Config{Name: path, Baud: 19200}
		fin, err = serial.OpenPort(&sc)
	} else {
		v.debug("%s: is not char device, assuming debug file capture")
		fin, err = os.OpenFile(path, os.O_RDONLY, 0777)
	}
	if err != nil {
		v.debug("%s: could not open, %v", path, err)
		return nil, err
	}
	v.ctx = ctx
	if v.ctx == nil {
		v.ctx = context.Background()
	}
	v.fin = fin
	v.out = out
	v.wg = wg
	if v.wg == nil {
		v.wg = new(sync.WaitGroup)
	}
	v.wg.Add(1)
	for _, opt := range options {
		if opt == AddTime {
			v.AddTime = true
		}
	}
	go v.readThread()
	return v, nil
}

func (v *Vedirect) debug(msg string, args ...interface{}) {
	if v.dout == nil {
		return
	}
	fmt.Fprintf(v.dout, msg+"\n", args...)
}

type fdFile interface {
	Fd() uintptr // os.File has this
}

type statFile interface {
	Stat() (os.FileInfo, error) // os.File has this
}

const charDevice = fs.ModeDevice | fs.ModeCharDevice

// handle achieves parsing VE.Direct status messages
// lines are mostly text except Checksum byte:
// "{key}\t{value}\r\n"
// "Checksum\t{cs byte}\r\n"
func (v *Vedirect) handle(b uint8) {
	if b == hexmarker && v.state != inChecksum {
		v.state = hex
	}

	v.bytesSum += uint(b)
	switch v.state {
	case waitHeader:
		if b == '\n' {
			v.state = inKey
		}
	case inKey:
		if b == delimiter {
			if string(v.key) == "Checksum" {
				v.state = inChecksum
			} else {
				v.state = inValue
			}
		} else {
			v.key = append(v.key, b)
		}
	case inValue:
		if b == '\r' {
			v.state = waitHeader
			if v.data == nil {
				v.data = make(map[string]string)
			}
			v.data[string(v.key)] = string(v.value)
			v.key = nil
			v.value = nil
		} else {
			v.value = append(v.value, b)
		}
	case inChecksum:
		v.key = nil
		v.value = nil
		v.state = waitHeader
		if v.bytesSum%256 == 0 {
			if v.AddTime {
				v.data["_t"] = strconv.FormatInt(time.Now().UnixMilli(), 10)
			}
			v.out <- v.data
			v.data = nil
		}
		v.bytesSum = 0
	case hex:
		v.bytesSum = 0
		if b == '\n' {
			v.state = waitHeader
		}
	default:
		panic("bad state")
	}
}

func (v *Vedirect) readThread() {
	fc, ok := v.fin.(io.Closer)
	if ok {
		defer fc.Close()
	}
	if v.wg != nil {
		defer v.wg.Done()
	}
	done := v.ctx.Done()
	buf := make([]byte, 4096)
	for {
		select {
		case <-done:
			return
		default:
		}
		n, err := v.fin.Read(buf)
		if err != nil {
			// TODO: capture error back to Vedirect.err or something?
			log.Printf("ve read err: %v", err)
			close(v.out)
			return
		}
		for i := 0; i < n; i++ {
			v.handle(buf[i])
		}
	}
}

// from "VE.Direct Protocol" text protocol spec
const intFieldsBlob = `V mV
V2 mV
V3 mV
VS mV
VM mV
DM ‰
VPV mV
PPV W
I mA
I2 mA
I3 mA
IL mA
T °C
P W
CE mAh
SOC ‰
TTG Minutes
H1 mAh
H2 mAh
H3 mAh
H4
H5
H6 mAh
H7 mV
H8 mV
H9 Seconds
H10
H11
H12
H13
H14
H15 mV
H16 mV
H17 0.01kWh
H18 0.01kWh
H19 0.01kWh
H20 0.01kWh
H21 W
H22 0.01kWh
H23 W
HSDS Day sequence number
AC_OUT_V 0.01V
AC_OUT_I 0.1A
AC_OUT_S VA
`

const otherFieldsBlob = `LOAD
Alarm
Relay
AR
OR
ERR
CS
BMV
FW
FWE
PID
SER#
MODE
WARN
MPPT
MON
`

// IntFields map field name to unit description (if any).
//
// Some fields will have empty string for unit description.
var IntFields map[string]string

// OtherFields is a set of known VE.Direct fields that are not int.
//
//  isKnownOtherField := vedirect.OtherFields["BLAH"]
var OtherFields map[string]bool

func init() {
	IntFields = make(map[string]string, 30)
	fin := strings.NewReader(intFieldsBlob)
	sc := bufio.NewScanner(fin)
	for sc.Scan() {
		line := sc.Text()
		if len(line) == 0 {
			continue
		}
		a, b, didCut := strings.Cut(line, " ")
		if didCut {
			IntFields[a] = b
		} else {
			IntFields[line] = ""
		}
	}
	OtherFields = make(map[string]bool, 20)
	fin = strings.NewReader(otherFieldsBlob)
	sc = bufio.NewScanner(fin)
	for sc.Scan() {
		line := sc.Text()
		if len(line) == 0 {
			continue
		}
		OtherFields[line] = true
	}
}

var ParseRecordDebug io.Writer

// ParseRecord will parse some field values to int64
func ParseRecord(rec map[string]string) map[string]interface{} {
	nrec := make(map[string]interface{}, len(rec))
	for k, v := range rec {
		_, isInt := IntFields[k]
		if isInt {
			iv, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				nrec[k] = iv
			} else {
				debug := ParseRecordDebug
				if debug != nil {
					fmt.Fprintf(debug, "bad int [%s]=%#v (%v)", k, v, err)
				}
				nrec[k] = v
			}
		} else {
			knownOther := OtherFields[k]
			if !knownOther {
				debug := ParseRecordDebug
				if debug != nil {
					fmt.Fprintf(debug, "unknown field [%s]=%#v", k, v)
				}
			}
			nrec[k] = v
		}
	}
	return nrec
}

// FloatWholeUnits uses the unit string from IntFields to convert some values into float64 of their whole unit.
// e.g. (10_000, "mV)" -> (10.0, "V")
//
// If there was no conversion, unit returned is same as passed in.
func FloatWholeUnits(v int64, unit string) (float64, string) {
	// "mV" "mA" "mAh" "0.01kWh" "0.01V" "0.1A"
	if unit == "" {
		return float64(v), unit
	}
	if unit[0] == 'm' {
		return float64(v) / 1000.0, unit[1:]
	}
	if strings.HasPrefix(unit, "0.1") {
		return float64(v) / 10.0, unit[3:]
	}
	if strings.HasPrefix(unit, "0.01") {
		return float64(v) / 100.0, unit[4:]
	}
	return float64(v), unit
}
