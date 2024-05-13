package migrator

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func (m *Migrator) runWorker(
	shutdownCtx context.Context,
	id int,
	jobCh <-chan *WorkerJob,
	cpChan chan<- *CheckpointJob,
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
		case job, open := <-jobCh:
			if !open {
				llog.Debug("job channel closed - exiting worker")
				break MAIN
			}

			llog.Debugf("received job at offset '%v'", job.Offset)

			if err := m.processJob(job); err != nil {
				return errors.Wrap(err, "error processing job")
			}

			cpChan <- &CheckpointJob{
				Offset:   job.Offset,
				WorkerID: id,
			}
		}
	}

	return nil
}

func (m *Migrator) processJob(j *WorkerJob) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "processWork",
	})

	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(j.Data)))

	llog.Debugf("processing job at offset '%v'", j.Offset)

	m.checksumsMu.Lock()
	defer m.checksumsMu.Unlock()

	if _, ok := m.checksums[checksum]; ok {
		llog.Debugf("CHECKSUM %s ALREADY IN MAP (offset '%d')", checksum, j.Offset)
		return nil
	}

	m.checksums[checksum] = struct{}{}

	// TODO: Implement
	return nil
}
