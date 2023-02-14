package completion_cmd

import (
	"io"
	"os"

	"github.com/spf13/cobra"
)

type completionFunc func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective)

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

func NewCompletion(kafeman *cobra.Command) *cobra.Command {

	cmd := &cobra.Command{
		Use:                   "completion [SHELL]",
		Short:                 "Generate completion script for bash, zsh, fish or powershell",
		Long:                  completionDoc,
		DisableFlagsInUseLine: true,
		Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		Run: func(cmd *cobra.Command, args []string) {
			// TODO: arg
			run, found := supportedCompletions[args[0]]
			if !found {
				// TODO:
				return
			}

			// TODO: stdout
			run(kafeman, os.Stdout)
		},
	}

	return cmd
}

var supportedCompletions = map[string]func(*cobra.Command, io.Writer) error{
	"bash":       runCompletionBash,
	"zsh":        runCompletionZsh,
	"fish":       runCompletionFish,
	"powershell": runCompletionPwsh,
}

func runCompletionBash(kafeman *cobra.Command, out io.Writer) error {
	return kafeman.GenBashCompletion(out)
}

func runCompletionFish(kafeman *cobra.Command, out io.Writer) error {
	return kafeman.GenFishCompletion(out, true)
}

func runCompletionZsh(kafeman *cobra.Command, out io.Writer) error {
	return kafeman.GenZshCompletion(out)
}

func runCompletionPwsh(kafeman *cobra.Command, out io.Writer) error {
	return kafeman.GenPowerShellCompletion(out)
}
