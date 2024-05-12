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

func (m *Migrator) runReader(shutdownCtx context.Context, workCh chan<- *WorkerJob) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runReader",
	})
	llog.Debug("start")
	defer llog.Debug("exit")

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
			llog.Debug("received shutdown signal")
			break MAIN
		default:
			line := scanner.Text()

			// Determine where we are in the file for checkpointing
			offset, err = reader.Seek(0, io.SeekCurrent)
			if err != nil {
				if err == io.EOF {
					// TODO: Need to get everyone to understand completion -- cause goroutines to exit gracefully
					llog.Debug("EOF reached")
					break MAIN
				}

				return errors.Wrap(err, "unable to seek to current offset")
			}

			llog.Debugf("sending job at offset: %d", offset)
			workCh <- &WorkerJob{
				Line:   line,
				Offset: offset,
			}

			numProcessed += 1

			llog.Debugf("proccessed '%d' jobs", numProcessed)
		}
	}

	return nil
}
