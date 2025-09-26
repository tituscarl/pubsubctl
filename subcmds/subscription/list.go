package subscription

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/alphauslabs/pubsub-sdk-go"
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/logger"
)

func ListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all pubsub subscriptions",
		Long:  `List all pubsub subscriptions in topic/subscription format.`,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := pubsub.New(pubsub.WithLogger(log.New(io.Discard, "", 0)))
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to create pubsub client: %v", err))
				return
			}
			defer client.Close()
			subscriptions, err := client.ListSubscriptions(context.Background())
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to list pubsub subscriptions: %v", err))
				return
			}

			for _, subscription := range subscriptions {
				logger.Info(subscription)
			}
		},
	}
}
