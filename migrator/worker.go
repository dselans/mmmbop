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

	llog.Debug("start")
	defer llog.Debug("exit")

	var numProcessed int

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

			wj, err := m.processJob(job)
			if err != nil {
				return errors.Wrap(err, "error processing job")
			}

			wjCh <- wj
		}
	}

	llog.Debugf("handled '%d' jobs", numProcessed)

	return nil
}

func (m *Migrator) processJob(j *ProcessorJob) (*WriterJob, error) {
	llog := m.log.WithFields(logrus.Fields{
		"method": "processWork",
	})

	llog.Debugf("processing job at offset '%v'", j.Offset)

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
