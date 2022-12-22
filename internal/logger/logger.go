package logger

import (
	"fmt"
	"os"
)

var (
	failTolerance = false
	quiet         = false
)

func InitLogger(f, q bool) {
	failTolerance = f
	quiet = q
}

func Infof(format string, a ...any) {
	if quiet {
		return
	}

	fmt.Fprintf(os.Stdout, format, a...)
}

func Errorf(format string, a ...any) {
	if quiet {
		return
	}

	fmt.Fprintf(os.Stderr, format, a...)
}

func Fatalf(format string, a ...any) {
	Errorf(format, a...)
	if !failTolerance {
		os.Exit(1)
	}
}
