package logger

import (
	"fmt"
	"io"
	"log"
	"os"
)

var (
	errOutput    io.Writer = os.Stderr
	stdOutput    io.Writer = os.Stdout
	fatalOnError           = true
	printInfo              = true
)

func InitLogger(err, out io.Writer, fatal bool) {
	errOutput = err
	stdOutput = out
	fatalOnError = fatal
}

func Infof(format string, a ...any) {
	if !printInfo {
		return
	}

	fmt.Fprintf(stdOutput, format, a...)
}

func Fatal(a ...any) {
	log.Fatal(a...)
}

func OptionalFatal(a ...any) {
	if fatalOnError {
		Fatal(a...)
	}

	fmt.Fprintf(errOutput, "%+v", a...)
}
