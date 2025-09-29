package bundle

import (
	"context"
	"fmt"
	"io"

	"github.com/databricks/cli/libs/sync"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func ptermOutputHandler(ctx context.Context, c <-chan sync.Event, writer io.Writer) {
	p := mpb.New(mpb.WithWidth(64), mpb.WithOutput(writer))
	uploadsBar := p.AddBar(0,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Uploading: "),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.OnComplete(
				decor.Percentage(decor.WCSyncWidth), "done",
			),
		),
	)
	deletesBar := p.AddBar(0,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Deleting:  "),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.OnComplete(
				decor.Percentage(decor.WCSyncWidth), "done",
			),
		),
	)

	uploads := 0
	deletes := 0

	for event := range c {
		if startEvent, ok := event.(*sync.EventStart); ok {
			uploadsBar.SetTotal(int64(len(startEvent.Put)), false)
			deletesBar.SetTotal(int64(len(startEvent.Delete)), false)
		}
		if progressEvent, ok := event.(*sync.EventSyncProgress); ok {
			if progressEvent.Action == sync.EventActionPut {
				uploadsBar.Increment()
				uploads++
			} else {
				deletesBar.Increment()
				deletes++
			}
		}
		if completeEvent, ok := event.(*sync.EventSyncComplete); ok {
			uploadsBar.SetTotal(int64(uploads), true)
			deletesBar.SetTotal(int64(deletes), true)
			p.Wait()
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