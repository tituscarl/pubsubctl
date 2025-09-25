package topics

import (
	"github.com/spf13/cobra"
	"github.com/tituscarl/pubsubctl/logger"
)

func ListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all Pub/Sub topics",
		Long:  `Retrieve and display a list of all Pub/Sub topics in the project.`,
		Run: func(cmd *cobra.Command, args []string) {
			logger.Info("Comming soon...")
		},
	}
}
