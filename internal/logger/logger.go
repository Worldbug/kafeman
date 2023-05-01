package logger

import (
	"fmt"
	"os"

	"github.com/worldbug/kafeman/cmd/kafeman/command/global_config"
)

func Infof(format string, a ...any) {
	if global_config.Config.Quiet {
		return
	}

	fmt.Fprintf(os.Stdout, format, a...)
}

func Errorf(format string, a ...any) {
	if global_config.Config.Quiet {
		return
	}

	fmt.Fprintf(os.Stderr, format, a...)
}

func Fatalf(format string, a ...any) {
	Errorf(format, a...)
	if !global_config.Config.FailTolerance {
		os.Exit(1)
	}
}
