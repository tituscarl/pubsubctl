package queue

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/alphauslabs/pubsub-sdk-go"
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/logger"
)

var (
	topic        string
	subscription string
)

func CountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "count",
		Short: "Count number of messages per subscriptions in queue",
		Run: func(cmd *cobra.Command, args []string) {
			client, err := pb.New()
			if err != nil {
				logger.Fail("Failed to create pubsub client:", err)
				return
			}
			defer client.Close()
			ctx := context.Background()
			res, err := client.GetNumberOfMessages(ctx, topic, subscription)
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
	cmd.Flags().StringVar(&topic, "topic", "", "Filter by topic name")
	cmd.Flags().StringVar(&subscription, "subscription", "", "Filter by subscription name")
	return cmd
}
