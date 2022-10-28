package main

import (
	"fmt"
	"kafeman/internal/app"
	"os"
)

func main() {
	if err := app.App().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
