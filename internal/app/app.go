package app

import (
	"kafeman/internal/command"

	"github.com/spf13/cobra"
)

func App() *cobra.Command {
	return command.RootCMD
}
