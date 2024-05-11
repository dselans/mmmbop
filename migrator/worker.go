package migrator

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func (m *Migrator) runWorker(
	shutdownCtx context.Context,
	id int,
	jobCh <-chan *Job,
	cpChan chan<- *Checkpoint,
) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runWorker",
		"id":     id,
	})

	llog.Debug("start")
	defer llog.Debug("exit")

MAIN:
	for {
		select {
		case <-shutdownCtx.Done():
			llog.Debug("received shutdown signal")
			break MAIN
		case job := <-jobCh:
			llog.Debugf("received job: %v", job)
			if err := m.processJob(job); err != nil {
				return errors.Wrap(err, "error processing job")
			}

			llog.Debug("finished processing, sending checkpoint")
			cpChan <- &Checkpoint{}
		}
	}

	return nil
}

func (m *Migrator) processJob(j *Job) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "processWork",
	})

	llog.Debugf("processing job: %v", j)

	// TODO: Implement
	return nil
}
