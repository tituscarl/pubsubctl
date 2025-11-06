package cmds

import (
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/subcmds/topic"
)

func TopicsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Manage pubsub topics",
		Long:  `Create, delete, and list pubsub topics.`,
	}
	cmd.AddCommand(topic.PurgeCmd())
	cmd.AddCommand(topic.ListCmd())
	cmd.AddCommand(topic.GetCmd())
	return cmd
}
