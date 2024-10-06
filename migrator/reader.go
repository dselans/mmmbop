package migrator

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/timpalpant/gzran"
)

func (m *Migrator) runReader(shutdownCtx context.Context, workCh chan<- *ProcessorJob) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runReader",
	})
	llog.Debug("Start")
	defer llog.Debug("Exit")

	f, err := os.Open(m.cfg.TOML.Source.File)
	if err != nil {
		return errors.Wrap(err, "unable to open source file")
	}

	reader, err := gzran.NewReader(f)
	if err != nil {
		return errors.Wrap(err, "unable to create reader")
	}

	reader.Index = m.cp.Index
	defer reader.Close()

	scanner := bufio.NewScanner(reader)

	// Where to start reading from
	offset := m.cp.IndexOffset
	numProcessed := 0

MAIN:
	for scanner.Scan() {
		select {
		case <-shutdownCtx.Done():
			llog.Debug("Received shutdown signal")
			break MAIN
		default:
			line := scanner.Text()

			// Determine where we are in the file for checkpointing
			offset, err = reader.Seek(0, io.SeekCurrent)
			if err != nil {
				if err == io.EOF {
					// Once reader exits, migrator will signal workers and
					// checkpointer to exit.
					llog.Debug("EOF reached")
					break MAIN
				}

				return errors.Wrap(err, "unable to seek to current offset")
			}

			llog.Debugf("Sending job at offset: %d", offset)
			workCh <- &ProcessorJob{
				Data:   line,
				Offset: offset,
			}

			numProcessed += 1

			llog.Debugf("Proccessed '%d' jobs", numProcessed)
		}
	}

	return nil
}
