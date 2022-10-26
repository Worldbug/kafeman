package main

import (
	"fmt"
	"os"
	"protokaf/internal/app"
)

func main() {
	if err := app.App().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
