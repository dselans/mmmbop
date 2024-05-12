package migrator

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

func (m *Migrator) runReader(shutdownCtx context.Context, workCh chan<- *Job) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runReader",
	})
	llog.Debug("start")
	defer llog.Debug("exit")

MAIN:
	for {
		select {
		case <-shutdownCtx.Done():
			llog.Debug("received shutdown signal")
			break MAIN
		default:
			id := time.Now().UnixNano()

			llog.Debug("sending job with ID '%d'", id)
			workCh <- &Job{
				ID: id,
			}

			time.Sleep(time.Second)
		}
	}

	return nil
}
