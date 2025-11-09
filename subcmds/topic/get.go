package topic

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
		Use:   "get <topic>",
		Short: "Get a pubsub topic",
		Long:  `Get details of a specific pubsub topic.`,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := pubsub.New(pubsub.WithLogger(log.New(io.Discard, "", 0)))
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to create pubsub client: %v", err))
				return
			}
			defer client.Close()
			topicID := args[0]
			res, err := client.GetTopicInfo(context.Background(), topicID)
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to get pubsub topic: %v", err))
				return
			}

			logger.Info(fmt.Sprintf("name: %v, subscriptions: %v", res.Name, res.Subscriptions))

		},
	}
}
