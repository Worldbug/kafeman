package command

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

var (
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
