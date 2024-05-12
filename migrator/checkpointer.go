package migrator

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// runCheckpointer is responsible for writing checkpoints to disk and for
// reporting progress to the user.
//
// NOTE: This is a custom ctx that is created by Run() - it will only be closed
// once all workers have exited.
func (m *Migrator) runCheckpointer(ctx context.Context, cpChan <-chan *CheckpointJob) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runCheckpointer",
	})

	llog.Debug("start")
	defer llog.Debug("exit")

MAIN:
	for {
		select {
		case <-ctx.Done():
			llog.Debug("received shutdown signal")
			break MAIN
		case cp, ok := <-cpChan:
			if !ok {
				llog.Debug("checkpoint channel closed - exiting checkpointer")
				break MAIN
			}

			llog.Debugf("received checkpoint at offset '%v' worker id '%v'", cp.Offset, cp.WorkerID)

			if err := m.saveCheckpoint(cp); err != nil {
				llog.Errorf("error saving checkpoint for offset '%v' worker id '%d': %v", cp.Offset, cp.WorkerID, err)
			}
		}
	}

	return nil
}

func (m *Migrator) saveCheckpoint(cp *CheckpointJob) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "saveCheckpoint",
	})

	// Skip checkpoint if it's NOT zero/unset OR we haven't passed CheckpointInterval
	if !m.last.IsZero() && m.last.Add(time.Duration(m.cfg.TOML.Config.CheckpointInterval)).After(time.Now()) {
		llog.Debugf("skipping checkpoint save, last save was %v ago", time.Since(m.last))
		return nil
	}

	llog.Debugf("saving checkpoint to '%s'", m.cfg.TOML.Config.CheckpointFile)

	// Update checkpoint
	m.cp.Lock()
	m.cp.IndexOffset = cp.Offset
	m.cp.LastUpdated = time.Now()
	m.cp.Unlock()

	// Save checkpoint to disk
	if err := m.cp.Save(m.cfg.TOML.Config.CheckpointFile); err != nil {
		return errors.Wrap(err, "unable to save checkpoint")
	}

	// Note that a checkpoint save has occurred
	m.last = time.Now()

	return nil
}
