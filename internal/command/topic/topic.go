package topic_cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/worldbug/kafeman/internal/command"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/logger"
	"github.com/worldbug/kafeman/internal/models"

	"github.com/spf13/cobra"
)

func NewTopicCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Create and describe topics.",
	}

	cmd.AddCommand(NewDescribeCMD())
	cmd.AddCommand(NewTopicConsumersCMD())
	cmd.AddCommand(NewDeleteTopicCMD())
	cmd.AddCommand(NewLSTopicsCMD())
	cmd.AddCommand(NewCreateTopicCmd())
	cmd.AddCommand(NewAddConfigCmd())
	cmd.AddCommand(NewTopicSetConfig())
	cmd.AddCommand(NewUpdateTopicCmd())

	return cmd
}

func newDescribeOptions(config *config.Configuration) *describeOptions {
	return &describeOptions{
		out:              os.Stdout,
		config:           config,
		PrettyPrintFlags: command.NewPrettyPrintFlags(),
	}
}

func NewDescribeCMD() *cobra.Command {
	options := newDescribeOptions(config.Config)

	cmd := &cobra.Command{
		Use:               "describe",
		Short:             "Describe topic info",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")
	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")

	return cmd
}

type describeOptions struct {
	config *config.Configuration

	command.PrettyPrintFlags
	out    io.Writer
	asJson bool
}

func (d *describeOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(d.config)
	topicInfo, err := k.DescribeTopic(cmd.Context(), args[0])
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	if d.asJson {
		d.describeTopicPrintJson(topicInfo)
		return
	}

	d.describeTopicPrint(topicInfo)
}

func (d *describeOptions) describeTopicPrintJson(topicInfo models.TopicInfo) {
	raw, err := json.Marshal(topicInfo)
	if err != nil {
		return
	}

	fmt.Fprintln(d.out, string(raw))
}

func (d *describeOptions) describeTopicPrint(topicInfo models.TopicInfo) {
	w := tabwriter.NewWriter(d.out, d.MinWidth, d.Width, d.Padding, d.PadChar, d.Flags)
	defer w.Flush()

	fmt.Fprintf(w, "Topic:\t%s\n", topicInfo.TopicName)
	w.Flush()

	fmt.Fprintf(w, "Partitions:\n")
	w.Flush()

	if !d.NoHeader {
		fmt.Fprintf(w, "\tPartition\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
		fmt.Fprintf(w, "\t---------\t--------------\t------\t--------\t---\t\n")
	}
	sort.Slice(topicInfo.Partitions, func(i, j int) bool {
		return topicInfo.Partitions[i].Partition < topicInfo.Partitions[j].Partition
	})
	for _, p := range topicInfo.Partitions {
		fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t%v\t\n", p.Partition, p.HightWatermark, p.Leader, p.Replicas, p.ISR)
	}
	w.Flush()

	if !d.NoHeader {
		fmt.Fprintf(w, "Config:\n")
		fmt.Fprintf(w, "\tKey\tValue\tRead only\tSensitive\n")
		fmt.Fprintf(w, "\t---\t-----\t---------\t---------\n")
	}

	for _, c := range topicInfo.Config {
		fmt.Fprintf(w, "\t%s\t%s\t%v\t%v\n", c.Name, c.Value, c.ReadOnly, c.Sensitive)
	}

}

func newLSTopicsOptions(config *config.Configuration) *lsTopicsOptions {
	return &lsTopicsOptions{
		config:           config,
		out:              os.Stdout,
		PrettyPrintFlags: command.NewPrettyPrintFlags(),
	}
}

type lsTopicsOptions struct {
	config *config.Configuration
	command.PrettyPrintFlags
	out    io.Writer
	asJson bool
}

func (l *lsTopicsOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(l.config)

	topics, err := k.ListTopics(cmd.Context())
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	if l.asJson {
		command.PrintJson(topics)
		return
	}

	l.lsTopicsPrint(topics)
}

func (l *lsTopicsOptions) lsTopicsPrint(topics []models.Topic) {
	w := tabwriter.NewWriter(l.out, l.MinWidth, l.Width, l.Padding, l.PadChar, l.Flags)
	w.Flush()

	if !l.NoHeader {
		fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
	}

	for _, topic := range topics {
		fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.Name, topic.Partitions, topic.Replicas)
	}
	w.Flush()
}

func NewTopicsCMD() *cobra.Command {
	cmd := NewLSTopicsCMD()
	cmd.Use = "topics"

	return cmd
}

func NewLSTopicsCMD() *cobra.Command {
	options := newLSTopicsOptions(config.Config)

	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List topics",
		Args:    cobra.ExactArgs(0),
		Run:     options.run,
	}

	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")
	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")

	return cmd
}

func newTopicConsumersOptions(config *config.Configuration) *topicConsumersOptions {
	return &topicConsumersOptions{
		config:           config,
		out:              os.Stdout,
		PrettyPrintFlags: command.NewPrettyPrintFlags(),
	}
}

type topicConsumersOptions struct {
	config *config.Configuration
	asJson bool
	out    io.Writer

	command.PrettyPrintFlags
}

func (t *topicConsumersOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(t.config)
	consumers, err := k.ListTopicConsumers(cmd.Context(), args[0])
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	if t.asJson {
		command.PrintJson(consumers)
		return
	}

	t.topicConsumersPrint(consumers)
}

func NewTopicConsumersCMD() *cobra.Command {
	options := newTopicConsumersOptions(config.Config)

	cmd := &cobra.Command{
		Use:               "consumers",
		Short:             "List topic consumers",
		Args:              cobra.ExactArgs(1),
		Example:           "kafeman topic consumers topic_name",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")
	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")

	return cmd
}

func (t *topicConsumersOptions) topicConsumersPrint(consumers models.TopicConsumers) {
	w := tabwriter.NewWriter(t.out, t.MinWidth, t.Width, t.Padding, t.PadChar, t.Flags)
	defer w.Flush()

	if !t.NoHeader {
		fmt.Fprintf(w, "Consumers:\n")
		fmt.Fprintf(w, "\tName\tMembers\n")
		fmt.Fprintf(w, "\t----\t-------\n")
	}
	for _, c := range consumers.Consumers {
		fmt.Fprintf(w, "\t%s\t%d\n", c.Name, c.MembersCount)
	}
}

func newDeleteTopicOptions(config *config.Configuration) *deleteTopicOptions {
	return &deleteTopicOptions{
		config: config,
		out:    os.Stdout,
	}
}

type deleteTopicOptions struct {
	config *config.Configuration
	out    io.Writer
}

func (d *deleteTopicOptions) run(cmd *cobra.Command, args []string) {
	topic := args[0]

	k := kafeman.Newkafeman(d.config)
	err := k.DeleteTopic(cmd.Context(), topic)
	if err != nil {
		os.Exit(1)
	}

	fmt.Fprintf(d.out, "\xE2\x9C\x85 Deleted topic %v!\n", topic)
}

func NewDeleteTopicCMD() *cobra.Command {
	options := newDeleteTopicOptions(config.Config)

	cmd := &cobra.Command{
		Use:               "delete TOPIC",
		Short:             "Delete a topic",
		Args:              cobra.ExactArgs(1),
		Example:           "kafeman topic delete topic_name",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	return cmd
}

func newTopicSetOptions(config *config.Configuration) *topicSetOptions {
	return &topicSetOptions{
		config: config,
	}
}

type topicSetOptions struct {
	config *config.Configuration
}

func (t *topicSetOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(t.config)

	topic := args[0]

	splt := strings.Split(args[1], ",")
	configs := make(map[string]string)

	for _, kv := range splt {
		s := strings.Split(kv, "=")

		if len(s) != 2 {
			continue
		}

		configs[s[0]] = s[1]
	}

	if len(configs) < 1 {
		logger.Errorf("No valid configs found")
	}

	err := k.SetConfigValueTopic(cmd.Context(), kafeman.SetConfigValueTopicCommand{
		Topic:  topic,
		Values: configs,
	})
	if err != nil {
		os.Exit(1)
	}

	fmt.Printf("\xE2\x9C\x85 Updated config.")
}

func NewTopicSetConfig() *cobra.Command {
	options := newTopicSetOptions(config.Config)

	cmd := &cobra.Command{
		Use:               "set-config",
		Short:             "set topic config. requires Kafka >=2.3.0 on broker side and kafeman cluster config.",
		Example:           "kafeman topic set-config topic.name cleanup.policy=delete",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	return cmd
}

func newUpdateTopicOptions(config *config.Configuration) *updateTopicOptions {
	return &updateTopicOptions{
		config: config,
	}
}

type updateTopicOptions struct {
	config *config.Configuration

	partitionAssignments string
	// TODO:
	compact    bool
	partitions int32
}

func (u *updateTopicOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(u.config)
	topic := args[0]

	if u.partitions == -1 && u.partitionAssignments == "" {
		command.ExitWithErr("Number of partitions and/or partition assignments must be given")
	}

	var assignments [][]int32
	if u.partitionAssignments != "" {
		if err := json.Unmarshal([]byte(u.partitionAssignments), &assignments); err != nil {
			command.ExitWithErr("Invalid partition assignments: %v", err)
		}
	}

	err := k.UpdateTopic(cmd.Context(), kafeman.UpdateTopicCommand{
		Topic:           topic,
		PartitionsCount: u.partitions,
		Assignments:     assignments,
	})
	if err != nil {
		os.Exit(1)
	}

	fmt.Printf("\xE2\x9C\x85 Updated topic!\n")
}

func NewUpdateTopicCmd() *cobra.Command {
	options := newUpdateTopicOptions(config.Config)

	cmd := &cobra.Command{
		Use:               "update",
		Short:             "Update topic",
		Example:           "kafeman topic update topic_name -p 5 --partition-assignments '[[1,2,3],[1,2,3]]'",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	cmd.Flags().Int32VarP(&options.partitions, "partitions", "p", int32(1), "Number of partitions")
	cmd.Flags().BoolVar(&options.compact, "compact", false, "Enable topic compaction")
	cmd.Flags().StringVar(&options.partitionAssignments, "partition-assignments", "", "Partition Assignments. Optional. If set in combination with -p, an assignment must be provided for each new partition. Example: '[[1,2,3],[1,2,3]]' (JSON Array syntax) assigns two new partitions to brokers 1,2,3. If used by itself, a reassignment must be provided for all partitions.")
	return cmd
}

func newCreateTopicOptions(config *config.Configuration) *createTopicOptions {
	return &createTopicOptions{
		config:           config,
		PrettyPrintFlags: command.NewPrettyPrintFlags(),
		out:              os.Stdout,
	}
}

type createTopicOptions struct {
	config *config.Configuration
	command.PrettyPrintFlags
	out io.Writer

	// TODO:
	noHeader   bool
	compact    bool
	replicas   int16
	partitions int32
}

func (c *createTopicOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(c.config)
	topic := args[0]

	cleanupPolicy := "delete"
	if c.compact {
		cleanupPolicy = "compact"
	}

	err := k.CreateTopic(cmd.Context(), kafeman.CreateTopicCommand{
		TopicName:         topic,
		PartitionsCount:   c.partitions,
		ReplicationFactor: c.replicas,
		CleanupPolicy:     cleanupPolicy,
	})
	if err != nil {
		command.ExitWithErr("Could not create topic %v: %v\n", topic, err.Error())
	}
	w := tabwriter.NewWriter(c.out, c.MinWidth, c.Width, c.Padding, c.PadChar, c.Flags)
	defer w.Flush()

	fmt.Fprintf(w, "\xE2\x9C\x85 Created topic!\n")
	fmt.Fprintln(w, "\tTopic Name:\t", topic)
	fmt.Fprintln(w, "\tPartitions:\t", c.partitions)
	fmt.Fprintln(w, "\tReplication Factor:\t", c.replicas)
	fmt.Fprintln(w, "\tCleanup Policy:\t", cleanupPolicy)
}

func NewCreateTopicCmd() *cobra.Command {
	options := newCreateTopicOptions(config.Config)
	cmd := &cobra.Command{
		Use:     "create TOPIC",
		Short:   "Create a topic",
		Example: "kafeman topic create topic_name --partitions 6",
		Args:    cobra.ExactArgs(1),
		Run:     options.run,
	}

	cmd.Flags().Int32VarP(&options.partitions, "partitions", "p", int32(1), "Number of partitions")
	cmd.Flags().Int16VarP(&options.replicas, "replicas", "r", int16(1), "Number of replicas")
	cmd.Flags().BoolVar(&options.compact, "compact", false, "Enable topic compaction")

	return cmd
}

func newAddConfigOptions(config *config.Configuration) *addConfigOptions {
	return &addConfigOptions{
		config: config,
	}
}

type addConfigOptions struct {
	config *config.Configuration
}

func (a *addConfigOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(a.config)

	topic := args[0]
	key := args[1]
	value := args[2]

	err := k.AddConfigRecord(cmd.Context(), kafeman.AddConfigRecordCommand{
		Topic: topic,
		Key:   key,
		Value: value,
	})
	if err != nil {
		os.Exit(1)
	}

	fmt.Printf("Added config %v=%v to topic %v.\n", key, value, topic)
}

func NewAddConfigCmd() *cobra.Command {
	options := newAddConfigOptions(config.Config)

	cmd := &cobra.Command{
		Use:               "add-config TOPIC KEY VALUE",
		Short:             "Add config key/value pair to topic",
		Example:           "kafeman topic add-config topic_name compression.type gzip",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Args:              cobra.ExactArgs(3),
		Run:               options.run,
	}

	return cmd
}
