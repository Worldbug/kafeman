package main

import (
	"fmt"
	"gkat/internal/app"
	"os"
)

func main() {
	if err := app.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
