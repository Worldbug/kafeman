package command

import (
	"github.com/spf13/cobra"
)

func init() {
	RootCMD.AddCommand(completionCmd)
}

const completionDoc = `To load completions:
Bash:
	$ source <(kafeman completion bash)
	# To load completions for each session, execute once:
	
	Linux:
	$ kafeman completion bash > /etc/bash_completion.d/kafeman
	
	MacOS:
	$ kafeman completion bash > /usr/local/etc/bash_completion.d/kafeman

Zsh:
	# To load completions for each session, execute once:
	$ kafeman completion zsh > "${fpath[1]}/_kafeman"
	# You will need to start a new shell for this setup to take effect.

Fish:
	$ kafeman completion fish | source
	# To load completions for each session, execute once:
	$ kafeman completion fish > ~/.config/fish/completions/kafeman.fish

PowerShell:
	PS> %[1]s completion powershell | Out-String | Invoke-Expression
	# To load completions for every new session, run:
	PS> %[1]s completion powershell > %[1]s.ps1
	# and source this file from your PowerShell profile.
`

var completionCmd = &cobra.Command{
	Use:                   "completion [SHELL]",
	Short:                 "Generate completion script for bash, zsh, fish or powershell",
	Long:                  completionDoc,
	DisableFlagsInUseLine: true,
	Args:                  cobra.ExactValidArgs(1),
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			err := RootCMD.GenBashCompletion(outWriter)
			if err != nil {
				errorExit("Failed to generate bash completion: %w", err)
			}
		case "zsh":
			if err := RootCMD.GenZshCompletion(outWriter); err != nil {
				errorExit("Failed to generate zsh completion: %w", err)
			}
		case "fish":
			if err := RootCMD.GenFishCompletion(outWriter, true); err != nil {
				errorExit("Failed to generate fish completion: %w", err)
			}
		case "powershell":
			err := RootCMD.GenPowerShellCompletion(outWriter)
			if err != nil {
				errorExit("Failed to generate powershell completion: %w", err)
			}
		}
	},
}
