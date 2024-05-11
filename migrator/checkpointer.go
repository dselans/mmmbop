package migrator

import (
	"context"

	"github.com/sirupsen/logrus"
)

// runCheckpointer is responsible for writing checkpoints to disk and for
// reporting progress to the user.
//
// NOTE: This is a custom ctx that is created by Run() - it will only be closed
// once all workers have exited.
func (m *Migrator) runCheckpointer(ctx context.Context, cpChan <-chan *Checkpoint) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runCheckpointer",
	})

	llog.Debug("start")
	defer llog.Debug("exit")

	// TODO: Read from checkpoint ch and write to file
MAIN:
	for {
		select {
		case <-ctx.Done():
			llog.Debug("received shutdown signal")
			break MAIN
		}
	}

	return nil
}
