package bundle

import (
	"context"
	"fmt"
	"io"

	"github.com/databricks/cli/libs/sync"
	"github.com/pterm/pterm"
)

func ptermOutputHandler(ctx context.Context, c <-chan sync.Event, writer io.Writer) {
	spinner, _ := pterm.DefaultSpinner.
		WithWriter(writer).
		Start("Deploying bundle...")
	defer spinner.Stop()

	for event := range c {
		if startEvent, ok := event.(*sync.EventStart); ok {
			if len(startEvent.Put) > 0 {
				spinner.UpdateText("Uploading files...")
			} else if len(startEvent.Delete) > 0 {
				spinner.UpdateText("Deleting files...")
			}
		}
		if progressEvent, ok := event.(*sync.EventSyncProgress); ok {
			if progressEvent.Action == sync.EventActionPut {
				spinner.UpdateText(fmt.Sprintf("Uploading: %s", progressEvent.Path))
			} else {
				spinner.UpdateText(fmt.Sprintf("Deleting: %s", progressEvent.Path))
			}
		}
		if completeEvent, ok := event.(*sync.EventSyncComplete); ok {
			spinner.Success("Deployment complete")
			fmt.Fprintf(writer, "\nDeployment summary:\n")
			for _, p := range completeEvent.Put {
				fmt.Fprintf(writer, "  [+] %s\n", p)
			}
			for _, p := range completeEvent.Delete {
				fmt.Fprintf(writer, "  [-] %s\n", p)
			}
			return
		}
	}
}