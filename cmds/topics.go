package cmds

import (
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/subcmds/topics"
)

func TopicsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "Manage pubsub topics",
		Long:  `Create, delete, and list pubsub topics.`,
	}
	cmd.AddCommand(topics.ListCmd())
	return cmd
}
