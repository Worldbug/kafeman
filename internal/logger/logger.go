package logger

import (
	"fmt"
	"os"
)

var (
	Quiet         bool
	FailTolerance bool
)

func Infof(format string, a ...any) {
	if Quiet {
		return
	}

	fmt.Fprintf(os.Stdout, format, a...)
}

func Errorf(format string, a ...any) {
	if Quiet {
		return
	}

	fmt.Fprintf(os.Stderr, format, a...)
}

func Fatalf(format string, a ...any) {
	Errorf(format, a...)
	if !FailTolerance {
		os.Exit(1)
	}
}
