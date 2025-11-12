package queue

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/alphauslabs/pubsub-sdk-go"
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/logger"
)

func CountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "count <topic> <subscription>",
		Short: "Count number of messages per subscriptions in queue, optionally you can filter it by subscription or topic.",
		Run: func(cmd *cobra.Command, args []string) {
			client, err := pubsub.New(pubsub.WithLogger(log.New(io.Discard, "", 0)))
			if err != nil {
				logger.Fail("Failed to create pubsub client:", err)
				return
			}
			defer client.Close()
			ctx := context.Background()
			sub := ""
			topic := ""
			if len(args) > 0 {
				topic = args[0]
			}
			if len(args) > 1 {
				sub = args[1]
			}
			res, err := client.GetNumberOfMessages(ctx, topic, sub)
			if err != nil {
				logger.Fail("Error getting number of messages:", err)
				return
			}
			for _, r := range res {
				parts := strings.Split(r.Subscription, "|")
				logger.Info(fmt.Sprintf("%s -> %d", parts[1], r.CurrentMessagesAvailable))
			}
		},
	}

	return cmd
}
