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

var topic string

func PurgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "purge",
		Short: "Purge all messages from a pubsub topic",
		Long:  `Purge all messages from a pubsub topic.`,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := pubsub.New(pubsub.WithLogger(log.New(io.Discard, "", 0)))
			if err != nil {
				cmd.Println("Error creating pubsub client:", err)
				return
			}
			defer client.Close()
			c, err := client.PurgeTopic(context.Background(), topic)
			if err != nil {
				logger.Fail("Failed to purge topic:", err)
				return
			}
			logger.Info(fmt.Sprintf("Successfully purged %d messages from topic: %s", c, topic))
		},
	}

	cmd.Flags().StringVar(&topic, "topic", "", "The topic to purge messages from (required)")
	cmd.MarkFlagRequired("topic")
	return cmd
}
