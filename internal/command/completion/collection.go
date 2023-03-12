package completion_cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/serializers"
)

func NewClusterCompletion(config *config.Config) completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {
		clusters := make([]string, 0, len(config.Clusters))

		for _, cluster := range config.Clusters {
			clusters = append(clusters, cluster.Name)
		}

		return clusters, cobra.ShellCompDirectiveNoFileComp
	}
}

func NewEncodingCompletion() completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {
		return serializers.SupportedSerializers, cobra.ShellCompDirectiveNoFileComp
	}
}

func NewGroupCompletion(config *config.Config) completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {
		list, _ := kafeman.Newkafeman(config).GetGroupsList(cmd.Context())
		return list, cobra.ShellCompDirectiveNoFileComp
	}
}

func NewOffsetCompletion() completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {
		return []string{
			"newest", "oldest",
		}, cobra.ShellCompDirectiveNoFileComp
	}
}

func NewPartitionerCompletion() completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {
		return []string{
			"jvm", "rand", "rr", "hash",
		}, cobra.ShellCompDirectiveNoFileComp
	}
}

func NewTimeCompletion() completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {
		return []string{
			time.Now().Format("2006-01-02T15:04:05"),
		}, cobra.ShellCompDirectiveNoFileComp
	}
}

func NewTopicCompletion(config *config.Config) completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {
		topicsSuggest := make([]string, 0)

		topics, err := kafeman.Newkafeman(config).ListTopics(cmd.Context())
		if err != nil {
			return topicsSuggest, cobra.ShellCompDirectiveNoFileComp
		}

		for _, topic := range topics {
			topicsSuggest = append(topicsSuggest, topic.Name)
		}

		return topicsSuggest, cobra.ShellCompDirectiveNoFileComp
	}
}

// TODO: refactor
func NewReplicationCompletion(config *config.Config) completionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) (
		[]string, cobra.ShellCompDirective) {

		suggest := make([]string, 0)

		if firstPart(args, toComplete) {
			for _, cluster := range config.Clusters {
				suggest = append(suggest, cluster.Name+"/")
			}

			return suggest, cobra.ShellCompDirectiveNoFileComp
		}

		if secondPart(args, toComplete) {
			cluster := strings.Split(toComplete, "/")[0]
			config.CurrentCluster = cluster

			topics, err := kafeman.Newkafeman(config).ListTopics(cmd.Context())
			if err != nil {
				return suggest, cobra.ShellCompDirectiveNoFileComp
			}

			for _, topic := range topics {
				suggest = append(suggest, fmt.Sprint(cluster+"/"+topic.Name))
			}

			return suggest, cobra.ShellCompDirectiveNoFileComp
		}

		return suggest, cobra.ShellCompDirectiveNoFileComp
	}
}

func firstPart(args []string, toComplete string) bool {
	if !strings.Contains(toComplete, "/") {
		return true
	}

	return false
}

func secondPart(args []string, toComplete string) bool {
	if strings.Contains(toComplete, "/") {
		return true
	}

	return false
}
