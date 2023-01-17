package command

import (
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/serializers"
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

func clusterCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	clusters := make([]string, 0, len(conf.Clusters))

	for _, cluster := range conf.Clusters {
		clusters = append(clusters, cluster.Name)
	}

	return clusters, cobra.ShellCompDirectiveNoFileComp
}

func encodingCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	return serializers.SupportedSerializers, cobra.ShellCompDirectiveNoFileComp
}

func groupCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	list, _ := kafeman.Newkafeman(conf).GetGroupsList(cmd.Context())
	return list, cobra.ShellCompDirectiveNoFileComp
}

func offsetCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	return []string{
		"newest", "oldest",
	}, cobra.ShellCompDirectiveNoFileComp
}

func partitionerCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	return []string{
		"jvm", "rand", "rr", "hash",
	}, cobra.ShellCompDirectiveNoFileComp
}

func timeCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	return []string{
		time.Now().Format("2006-01-02T15:04:05"),
	}, cobra.ShellCompDirectiveNoFileComp
}

func topicCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	topicsSuggest := make([]string, 0)

	topics, err := kafeman.Newkafeman(conf).ListTopics(cmd.Context())
	if err != nil {
		return topicsSuggest, cobra.ShellCompDirectiveNoFileComp
	}

	for _, topic := range topics {
		topicsSuggest = append(topicsSuggest, topic.Name)
	}

	return topicsSuggest, cobra.ShellCompDirectiveNoFileComp
}

// TODO: refactor
func replicationCompletion(cmd *cobra.Command, args []string, toComplete string) (
	[]string, cobra.ShellCompDirective) {
	suggest := make([]string, 0)

	commands := strings.Split(toComplete, "/")
	if len(commands) == 1 {
		for _, cluster := range conf.Clusters {
			suggest = append(suggest, cluster.Name+"/")
		}

		return suggest, cobra.ShellCompDirectiveNoFileComp
	}

	conf.CurrentCluster = strings.TrimRight(toComplete, "/")
	topics, err := kafeman.Newkafeman(conf).ListTopics(cmd.Context())
	if err != nil {
		return suggest, cobra.ShellCompDirectiveNoFileComp
	}

	for _, topic := range topics {
		// TODO: если строка частично дописана
		// то не дополняет
		suggest = append(suggest, toComplete+topic.Name)
	}

	return suggest, cobra.ShellCompDirectiveNoFileComp
}
