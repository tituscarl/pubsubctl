package topics

import (
	"context"
	"log"
	"strings"

	pb "github.com/alphauslabs/pubsub-sdk-go"
	"github.com/spf13/cobra"
)

func ListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all Pub/Sub topics",
		Long:  `Retrieve and display a list of all Pub/Sub topics in the project.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Listing all Pub/Sub topics...")
			client, err := pb.New()
			if err != nil {
				log.Fatal("Failed to create Pub/Sub client:", err)
			}
			res, err := client.GetNumberOfMessages(context.Background(), "")
			if err != nil {
				log.Println("Error getting number of messages:", err)
				return
			}
			for _, r := range res {
				parts := strings.Split(r.Subscription, "|")
				log.Printf("sub: %s, msg_count: %d", parts[1], r.CurrentMessagesAvailable)
			}
		},
	}
}
