package app

import (
	"github.com/worldbug/kafeman/internal/command"

	"github.com/spf13/cobra"
)

func App() *cobra.Command {
	return command.RootCMD
}
