// go get github.com/brianolson/vedirect
//
// with learnings from https://github.com/karioja/vedirect by Janne Kario

package vedirect

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"sync"

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

type Vedirect struct {
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

// Open
// wg may be nil
// debugOut may be nil
func Open(path string, out chan<- map[string]string, wg *sync.WaitGroup, ctx context.Context, debugOut io.Writer) (v *Vedirect, err error) {
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
