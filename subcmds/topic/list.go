package topic

import (
	"context"
	"fmt"

	"github.com/alphauslabs/pubsub-sdk-go"
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/logger"
)

func ListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all pubsub topics",
		Long:  `List all pubsub topics.`,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := pubsub.New()
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to create pubsub client: %v", err))
				return
			}
			defer client.Close()
			topics, err := client.ListTopics(context.Background())
			if err != nil {
				logger.Fail(fmt.Sprintf("Failed to list pubsub topics: %v", err))
				return
			}

			for _, topic := range topics {
				logger.Info(topic)
			}
		},
	}
}
