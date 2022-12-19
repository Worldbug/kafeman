package main

import (
	"fmt"
	"os"

	"github.com/worldbug/kafeman/internal/app"
)

func main() {
	if err := app.App().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "err: %+v", err)
		os.Exit(1)
	}
}
