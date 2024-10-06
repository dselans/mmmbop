package migrator

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func (m *Migrator) runProcessor(
	shutdownCtx context.Context,
	id int,
	jobCh <-chan *ProcessorJob,
	wjCh chan<- *WriterJob,
) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runProcessor",
		"id":     id,
	})

	llog.Debug("Start")
	defer llog.Debug("Exit")

	var numProcessed int

MAIN:
	for {
		select {
		case <-shutdownCtx.Done():
			llog.Debug("Received shutdown signal")
			break MAIN
		case job, open := <-jobCh:
			if !open {
				llog.Debug("Job channel closed - exiting worker")
				break MAIN
			}

			llog.Debugf("Received job at offset '%v'", job.Offset)

			wj, err := m.processJob(job)
			if err != nil {
				return errors.Wrap(err, "error processing job")
			}

			// Send job in goroutine to avoid blocking
			go func() {
				wjCh <- wj
			}()
		}
	}

	llog.Debugf("Handled '%d' jobs", numProcessed)

	return nil
}

func (m *Migrator) processJob(j *ProcessorJob) (*WriterJob, error) {
	llog := m.log.WithFields(logrus.Fields{
		"method": "processWork",
	})

	llog.Debugf("Processing job at offset '%v'", j.Offset)

	// BEGIN Temporary dupe checks
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(j.Data)))

	m.checksumsMu.Lock()
	defer m.checksumsMu.Unlock()

	if _, ok := m.checksums[checksum]; ok {
		llog.Debugf("CHECKSUM %s ALREADY IN MAP (offset '%d')", checksum, j.Offset)
		return nil, errors.New("checksum already in map")
	}

	m.checksums[checksum] = struct{}{}

	// END Temporary dupe checks

	// TODO: Verify that src contains all fields in mapping
	// TODO: Convert src fields to dst fields
	// TODO: Add writer job

	return &WriterJob{
		Offset: j.Offset,
	}, nil
}
