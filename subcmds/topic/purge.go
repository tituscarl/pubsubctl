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
		Use:   "purge <topic>",
		Short: "Purge all messages from a pubsub topic",
		Long:  `Purge all messages from a pubsub topic.`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 || args[0] == "" {
				logger.Fail("Topic is required")
				return
			}
			client, err := pubsub.New(pubsub.WithLogger(log.New(io.Discard, "", 0)))
			if err != nil {
				logger.Fail("Failed to create pubsub client:", err)
				return
			}
			defer client.Close()
			c, err := client.PurgeTopic(context.Background(), args[0])
			if err != nil {
				logger.Fail("Failed to purge topic:", err)
				return
			}

			logger.Info(fmt.Sprintf("Successfully purged %d messages from topic: %s", c, args[0]))
		},
	}

	return cmd
}
