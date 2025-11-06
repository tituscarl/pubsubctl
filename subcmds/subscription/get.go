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

func GetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Get a pubsub subscription",
		Long:  `Get details of a specific pubsub subscription.`,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := pubsub.New(pubsub.WithLogger(log.New(io.Discard, "", 0)))
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to create pubsub client: %v", err))
				return
			}
			defer client.Close()
			subscriptionID := args[0]
			res, err := client.GetSubscriptionInfo(context.Background(), subscriptionID)
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to get pubsub subscription: %v", err))
				return
			}

			logger.Info(fmt.Sprintf("name: %v, topic: %v, isAutoExtended: %v", res.Name, res.Topic, res.IsAutoExtend))
		},
	}
}
