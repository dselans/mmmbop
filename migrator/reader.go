package migrator

import (
	"context"

	"github.com/sirupsen/logrus"
)

func (m *Migrator) runReader(shutdownCtx context.Context, workCh chan<- *Job) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runReader",
	})
	llog.Debug("start")
	defer llog.Debug("exit")

	return nil
}
