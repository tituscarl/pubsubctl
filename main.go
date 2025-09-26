package main

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/cmds"
)

func main() {
	cmd := &cobra.Command{
		Use:   "pubsubctl",
		Short: "A CLI tool for managing internal pubsub resources",
		Long:  `pubsubctl is a command-line interface tool for managing internal pubsub resources`,
	}

	cmd.AddCommand(cmds.TopicsCmd())
	cmd.AddCommand(cmds.SubscriptionCmd())
	cmd.AddCommand(cmds.QueueCmd())
	err := cmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
