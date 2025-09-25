package cmds

import (
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/subcmds/queue"
)

func QueueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Manage pubsub queues",
		Long:  `Manage pubsub message queues.`,
	}
	cmd.AddCommand(queue.InQueueCmd())
	return cmd
}
