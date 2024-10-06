package migrator

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// runCheckpointer is responsible for writing checkpoints to disk and for
// reporting progress to the user.
//
// NOTE: This is a custom ctx that is created by Run() - it will only be closed
// once all workers have exited.
func (m *Migrator) runCheckpointer(cpControlCh <-chan bool, cpChan <-chan *CheckpointJob) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runCheckpointer",
	})

	llog.Debug("Start")
	defer llog.Debug("Exit")

	var (
		// We need the last job so that when checkpointer exits it is able to
		// write the final checkpoint data to disk.
		lastJob *CheckpointJob

		// Indicates whether this was a clean exit (ie. completed) or we were interrupted
		exitState bool
	)

MAIN:
	for {
		select {
		case state := <-cpControlCh:
			// If true, clean exit (ie. we are completed); if false, we were interrupted (ie. ctrl-c)
			exitState = state
			llog.Debug("Received shutdown signal")
			break MAIN
		case cp := <-cpChan:
			llog.Debugf("Received checkpoint at offset '%v' worker id '%v'", cp.Offset, cp.WorkerID)

			if err := m.saveCheckpoint(cp); err != nil {
				llog.Errorf("Error saving checkpoint for offset '%v' worker id '%d': %v", cp.Offset, cp.WorkerID, err)
			}

			lastJob = cp
		}
	}

	return m.saveCheckpoint(lastJob, exitState)
}

func (m *Migrator) saveCheckpoint(cp *CheckpointJob, cleanExit ...bool) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "saveCheckpoint",
	})

	// Skip checkpoint if it's NOT zero/unset OR we haven't passed CheckpointInterval
	if !m.last.IsZero() && m.last.Add(time.Duration(m.cfg.TOML.Config.CheckpointInterval)).After(time.Now()) {
		// Do NOT skip if an exitState is set
		if len(cleanExit) == 0 {
			llog.Debugf("Skipping checkpoint save, last save was %v ago", time.Since(m.last))
			return nil
		}
	}

	if cp == nil {
		llog.Debug("Received nil checkpoint - skipping save")
		return nil
	}

	llog.Debugf("Saving checkpoint to '%s'", m.cfg.TOML.Config.CheckpointFile)

	// Update checkpoint
	m.cp.Lock()

	m.cp.IndexOffset = cp.Offset
	m.cp.LastUpdated = time.Now()

	if len(cleanExit) > 0 && cleanExit[0] {
		llog.Debug("Clean shutdown detected - updating CompletedAt time")
		completedAt := time.Now()
		m.cp.CompletedAt = &completedAt
	}

	m.cp.Unlock()

	// Save checkpoint to disk
	if err := m.cp.Save(m.cfg.TOML.Config.CheckpointFile); err != nil {
		return errors.Wrap(err, "unable to save checkpoint")
	}

	// Note that a checkpoint save has occurred
	m.last = time.Now()

	return nil
}
