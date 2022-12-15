package main

import (
	"fmt"
	"os"

	"github.com/worldbug/kafeman/internal/app"
)

func main() {
	if err := app.App().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
