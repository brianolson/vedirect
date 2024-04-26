package vedirect

import (
	_ "embed"
	"encoding/binary"
	"encoding/csv"
	ehex "encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

//go:embed mppt_regs.csv
var mppt_regs_csv string

//go:embed phoenix_inverter_regs.csv
var phoenix_inverter_regs_csv string

type VERegister struct {
	Address     uint16   `json:"a"`
	Name        string   `json:"n"`
	Scale       *float64 `json:"s,omitempty"`
	Size        RegType  `json:"f"`
	Unit        string   `json:"u,omitempty"`
	SummaryMode string   `json:"m,omitempty"`
}

type RegType int

// rt = list(zip('u8 u16 u32 s8 s16 s32 unk'.split(' '), range(1,8)))
// print("const (\n" + "\n".join([f"\tRegType_{rn} RegType = {ri}" for rn,ri in rt]) + "\n)")
// print("var RegTypeNameToRegType map[string]RegType = map[string]RegType{\n" + "\n".join([f"\t\"{rn}\":RegType_{rn}," for rn,ri in rt]) + "\n}")

const (
	RegType_u8  RegType = 1
	RegType_u16 RegType = 2
	RegType_u32 RegType = 3
	RegType_s8  RegType = 4
	RegType_s16 RegType = 5
	RegType_s32 RegType = 6
	RegType_unk RegType = 7
)

var RegTypeNameToRegType map[string]RegType = map[string]RegType{
	"u8":  RegType_u8,
	"u16": RegType_u16,
	"u32": RegType_u32,
	"s8":  RegType_s8,
	"s16": RegType_s16,
	"s32": RegType_s32,
	"unk": RegType_unk,
}

var ErrHexDataShort = errors.New("VE HEX data too short for desired register")
var ErrHexTypeUnknown = errors.New("VE HEX data not a known register value type")

// try to parse remaining message content (hbytes[4:]) for a register value
// TODO: make public?
func parseByRegType(hbytes []byte, rt RegType) (value any, err error) {
	switch rt {
	case RegType_u8:
		value = hbytes[0]
	case RegType_u16:
		if len(hbytes) < 2 {
			err = ErrHexDataShort
			return
		}
		value = binary.LittleEndian.Uint16(hbytes[:2])
	case RegType_u32:
		if len(hbytes) < 4 {
			err = ErrHexDataShort
			return
		}
		value = binary.LittleEndian.Uint32(hbytes[:4])
	case RegType_s8:
		value = int8(hbytes[0])
	case RegType_s16:
		if len(hbytes) < 2 {
			err = ErrHexDataShort
			return
		}
		value = int16(binary.LittleEndian.Uint16(hbytes[:2]))
	case RegType_s32:
		if len(hbytes) < 4 {
			err = ErrHexDataShort
			return
		}
		value = int32(binary.LittleEndian.Uint32(hbytes[:4]))
	default:
		err = ErrHexTypeUnknown
		return
	}
	err = nil
	return
}

func readRegsCsv(x string) ([]VERegister, error) {
	out := make([]VERegister, 0, 50) // TODO: count the lines before allocating?
	fin := strings.NewReader(x)
	reader := csv.NewReader(fin)
	reader.Comment = '#'
	reader.TrimLeadingSpace = true
	reader.ReuseRecord = true

	for true {
		parts, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			err = fmt.Errorf("regs csv parse err, %w", err)
			return nil, err
		}
		addrStr := parts[0]
		if strings.HasPrefix(addrStr, "0x") {
			addrStr = addrStr[2:]
		}
		addr, err := strconv.ParseUint(addrStr, 16, 16)
		if err != nil {
			return nil, err
		}
		name := parts[1]
		var scale *float64 = nil
		if parts[2] != "" {
			scalev, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				return nil, err
			}
			scale = new(float64)
			*scale = scalev
		}
		regsize, ok := RegTypeNameToRegType[parts[3]]
		if !ok {
			regsize = RegType_unk
		}
		unit := parts[4]
		summaryMode := parts[5]
		out = append(
			out,
			VERegister{
				uint16(addr),
				name,
				scale,
				regsize,
				unit,
				summaryMode,
			},
		)
	}
	return out, nil
}

func VE_MPPT_Registers() []VERegister {
	out, err := readRegsCsv(mppt_regs_csv)
	if err != nil {
		panic(err)
	}
	return out
}

func VE_PhoenixInverter_Registers() []VERegister {
	out, err := readRegsCsv(phoenix_inverter_regs_csv)
	if err != nil {
		panic(err)
	}
	return out
}

var cachedMPPTRegisters []VERegister

// local versioun that does caching, and we're sure we won't corrupt this copy
func mpptRegs() []VERegister {
	if cachedMPPTRegisters == nil {
		cachedMPPTRegisters = VE_MPPT_Registers()
	}
	return cachedMPPTRegisters
}

var cachedPhoenixInverterRegisters []VERegister

// local versioun that does caching, and we're sure we won't corrupt this copy
func invRegs() []VERegister {
	if cachedPhoenixInverterRegisters == nil {
		cachedPhoenixInverterRegisters = VE_PhoenixInverter_Registers()
	}
	return cachedPhoenixInverterRegisters
}

var cachedAllRegs [][]VERegister

func allRegs() [][]VERegister {
	if cachedAllRegs == nil {
		cachedAllRegs = make([][]VERegister, 2)
		cachedAllRegs[0] = mpptRegs()
		cachedAllRegs[1] = invRegs()
	}
	return cachedAllRegs
}

var ErrNotData error = errors.New("VE HEX message is not 07 or 0A data")

type VERegValue struct {
	Register VERegister
	Value    any
}

// Parse register value from a VE.HEX message.
// Not fully general, this filters on 0x7 and 0xA messages which are:
//   * 0x7 response to register get
//   * 0xA asynchronously volunteered register update
func ParseHexRecord(x string) (value *VERegValue, err error) {
	blen := len(x) / 2
	hbytes := make([]byte, blen)
	count, err := ehex.Decode(hbytes, []byte(x))
	if err != nil {
		err = fmt.Errorf("VE HEX bad hex, %w", err)
		return
	}
	var hexSum uint
	for _, c := range hbytes[:count] {
		hexSum += uint(c)
	}
	if hexSum&0x0ff != 0x055 {
		err = fmt.Errorf("VE HEX bad checksum, 0x%02x != 0x55", hexSum&0x0ff)
		return
	}
	if hbytes[0] == 0x0a || hbytes[0] == 0x07 {
		// okay
		// 0x0a asynchronously volunteered register data update
		// 0x07 respeonse to register get
	} else {
		err = ErrNotData
		return
	}
	if hbytes[3] != 0 {
		// TODO: somewhere I have docs on at least what bits 0x07 mean, make nice messages
		err = fmt.Errorf("VE HEX message has error flag 0x%02x", hbytes[3])
		return
	}
	register := binary.LittleEndian.Uint16(hbytes[1:3])
	for _, regs := range allRegs() {
		for _, reg := range regs {
			if reg.Address == register {
				var rv any
				rv, err = parseByRegType(hbytes[4:], reg.Size)
				if err != nil {
					return
				}
				value = &VERegValue{
					Register: reg,
					Value:    rv,
				}
				return
			}
		}
	}
	err = fmt.Errorf("VE HEX unknown register 0x%04x", register)
	return
}
