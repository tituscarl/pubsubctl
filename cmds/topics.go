package cmds

import (
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/subcmds/topics"
)

func TopicsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "Manage Pub/Sub topics",
		Long:  `Create, delete, and list Pub/Sub topics.`,
	}
	cmd.AddCommand(topics.ListCmd())
	return cmd
}
