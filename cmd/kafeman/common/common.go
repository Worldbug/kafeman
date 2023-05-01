package common

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

var (
	// TODO: remove
	OutWriter io.Writer = os.Stdout
	ErrWriter io.Writer = os.Stderr
)

func ExitWithErr(format string, a ...interface{}) {
	fmt.Fprintf(ErrWriter, format+"\n", a...)
	os.Exit(1)
}

// nolint
func InTTY() bool {
	fi, _ := os.Stdout.Stat()
	return fi.Mode()&os.ModeCharDevice != 0
}

// TODO: перенести все на этот метод
func PrintJson(model any) {
	raw, err := json.Marshal(model)
	if err != nil {
		ExitWithErr("%+v", err)
	}

	fmt.Fprintln(OutWriter, string(raw))
}

func NewPrettyPrintFlags() PrettyPrintFlags {
	return PrettyPrintFlags{
		NoHeader: false,

		MinWidth:       6,
		MinWidthNested: 2,
		Width:          4,
		Padding:        3,
		PadChar:        ' ',
		Flags:          0,
	}
}

type PrettyPrintFlags struct {
	NoHeader bool

	MinWidth       int
	MinWidthNested int
	Width          int
	Padding        int
	PadChar        byte
	Flags          uint
}

const (
	NewestOffset = -1
	OldestOffset = -2
)

func GetOffsetFromFlag(offsetFlag string) int64 {
	var offset int64
	switch offsetFlag {
	case "oldest":
		offset = OldestOffset
	case "newest":
		offset = NewestOffset
	default:
		o, err := strconv.ParseInt(offsetFlag, 10, 64)
		if err != nil {
			ExitWithErr("Could not parse '%s' to int64: %v", offsetFlag, err)
		}
		offset = o
	}

	return offset
}

func IsJSON(data []byte) bool {
	var i interface{}
	if err := json.Unmarshal(data, &i); err == nil {
		return true
	}
	return false
}

func ParseTime(str string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05", str)
	if err != nil {
		return time.Unix(0, 0)
	}

	return t.UTC()
}
