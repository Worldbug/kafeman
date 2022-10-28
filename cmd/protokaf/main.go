package main

import (
	"fmt"
	"os"
	"protokaf/internal/app"
)

func main() {
	// cfg, err := config.LoadConfig("/Users/ktikhomirov/.protokaf/config.yml")

	// _, _ = cfg, err
	if err := app.App().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
