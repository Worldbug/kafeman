package cluster_cmd

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/models"
)

func NewInfoCMD() *cobra.Command {
	options := newInfoOptions()

	cmd := &cobra.Command{
		Use:     "info",
		Short:   "Print cluster info",
		Example: "kafeman cluster info --json",
		Run:     options.run,
	}

	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")

	return cmd
}

func newInfoOptions() *infoOptions {
	return &infoOptions{
		asJson:           false,
		out:              os.Stdout,
		PrettyPrintFlags: common.NewPrettyPrintFlags(),
	}
}

type infoOptions struct {
	asJson bool

	out io.Writer
	common.PrettyPrintFlags
}

func (o *infoOptions) run(cmd *cobra.Command, args []string) {
	admin := admin.NewAdmin(run_configuration.Config)

	info, err := admin.GetClusterInfo(cmd.Context())
	if err != nil {
		common.ExitWithErr("%+v", err)
	}

	if o.asJson {
		common.PrintJson(info)
		return
	}

	o.printClusterInfo(info)
	return
}

func (o *infoOptions) printClusterInfo(clusterInfo models.ClusterInfo) {
	w := tabwriter.NewWriter(o.out, o.MinWidth, o.Width, o.Padding, o.PadChar, o.Flags)
	defer w.Flush()

	fmt.Fprintln(w, "\tBroker ID\tAddress\tIs controller")
	fmt.Fprintln(w, "\t---------\t------\t--------------")
	for _, broker := range clusterInfo.Brokers {
		fmt.Fprintf(w, "\t%v\t%v\t%v\n", broker.ID, broker.Addr, broker.IsController)
	}
}
