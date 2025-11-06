package cmds

import (
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/subcmds/subscription"
)

func SubscriptionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscription",
		Short: "Manage pubsub subscriptions",
		Long:  `Create, delete, and list pubsub subscriptions.`,
	}
	cmd.AddCommand(subscription.ListCmd())
	cmd.AddCommand(subscription.GetCmd())
	return cmd
}
