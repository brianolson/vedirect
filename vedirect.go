// go get github.com/brianolson/vedirect

package vedirect

import (
	"context"
	"io"
	"io/fs"
	"log"
	"os"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
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

	ctx context.Context

	bytesSum uint

	data map[string]string

	out chan<- map[string]string

	key []byte

	value []byte

	state vedState

	wg *sync.WaitGroup
}

func New(fin io.Reader, out chan<- map[string]string, wg *sync.WaitGroup, ctx context.Context) *Vedirect {
	v := new(Vedirect)
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
	return v
}

func (v *Vedirect) Start() error {
	v.wg.Add(1)
	go v.readThread()
	return nil
}

type fdFile interface {
	Fd() uintptr // os.File has this
}

type statFile interface {
	Stat() (os.FileInfo, error) // os.File has this
}

const charDevice = fs.ModeDevice | fs.ModeCharDevice

func (v *Vedirect) start() error {
	fds, ok := v.fin.(statFile)
	if !ok {
		// cannot stat, don't try to ioctl()
		return nil
	}
	st, err := fds.Stat()
	if err != nil {
		return err
	}
	if st.Mode().IsRegular() {
		return nil
	}
	if (st.Mode() & charDevice) == charDevice {
		fdf, ok := v.fin.(fdFile)
		if !ok {
			// nothing to do
			return nil
		}
		fd := int(fdf.Fd())
		tt, err := unix.IoctlGetTermios(fd, unix.TCGETA)
		if err != nil {
			return err
		}
		tt.Ispeed = syscall.B19200
		tt.Ospeed = syscall.B19200
		return unix.IoctlSetTermios(fd, unix.TCSETA, tt)
	}
	return nil
}

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
	//defer v.fin.Close()
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
			log.Printf("ve read err: %v", err)
			close(v.out)
			return
		}
		for i := 0; i < n; i++ {
			v.handle(buf[i])
		}
	}
}
