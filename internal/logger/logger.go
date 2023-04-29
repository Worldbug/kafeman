package logger

import (
	"fmt"
	"os"

	"github.com/worldbug/kafeman/internal/config"
)

func Infof(format string, a ...any) {
	if config.Config.Quiet {
		return
	}

	fmt.Fprintf(os.Stdout, format, a...)
}

func Errorf(format string, a ...any) {
	if config.Config.Quiet {
		return
	}

	fmt.Fprintf(os.Stderr, format, a...)
}

func Fatalf(format string, a ...any) {
	Errorf(format, a...)
	if !config.Config.FailTolerance {
		os.Exit(1)
	}
}
