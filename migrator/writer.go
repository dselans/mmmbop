package migrator

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type WriterJob struct {
	Offset int64
}

func (m *Migrator) runWriter(shutdownCtx context.Context, id int, writerCh <-chan *WriterJob, cpChan chan<- *CheckpointJob) error {
	llog := m.log.WithFields(logrus.Fields{
		"method": "runWriter",
		"id":     id,
	})

	llog.Debug("start")
	defer llog.Debug("exit")

	// Create connection pool
	pool, err := m.createPGPool(shutdownCtx)
	if err != nil {
		return errors.Wrap(err, "error creating postgres connection pool")
	}

	// Ensure that destination tables and columns exist + have correct types
	if err := m.validateDestinationMappings(shutdownCtx, pool); err != nil {
		return errors.Wrap(err, "error validating destination mappings")
	}

	var numWritten int

MAIN:
	for {
		select {
		case <-shutdownCtx.Done():
			llog.Debug("received shutdown signal")
			break MAIN
		case job, open := <-writerCh:
			if !open {
				llog.Debug("writer channel closed - exiting writer")
				break MAIN
			}

			if err := m.writeJob(shutdownCtx, pool, job); err != nil {
				llog.Errorf("error writing job: %v", err)
				return errors.Wrap(err, "error writing job")
			}

			// Write checkpoint
			cpChan <- &CheckpointJob{
				Offset: job.Offset,
			}

			numWritten += 1
		}
	}

	llog.Debugf("handled '%d' jobs", numWritten)

	return nil
}

// TODO: Implement
func (m *Migrator) writeJob(shutdownCtx context.Context, pool *pgxpool.Pool, j *WriterJob) error {
	return nil
}

func (m *Migrator) createPGPool(shutdownCtx context.Context) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(m.cfg.TOML.Destination.DSN)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing postgres dsn")
	}

	config.ConnConfig.ConnectTimeout = 5 * time.Second

	pool, err := pgxpool.ConnectConfig(shutdownCtx, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating connecting to postgres")
	}

	return pool, nil
}

func (m *Migrator) validateDestinationMappings(shutdownCtx context.Context, pool *pgxpool.Pool) error {
	// Validate that destination tables exist
	if err := m.validateDstTables(shutdownCtx, pool); err != nil {
		return errors.Wrap(err, "error validating destination tables")
	}

	// Validate that destination columns exist + have correct types
	if err := m.validateDstColumns(shutdownCtx, pool); err != nil {
		return errors.Wrap(err, "error validating destination columns")
	}

	return nil
}

// TODO: Implement
func (m *Migrator) validateDstTables(shutdownCtx context.Context, pool *pgxpool.Pool) error {
	// Go through all mappings, parse the destination table and check if it exists
	for mName, mEntry := range m.cfg.TOML.Mapping.Mapping {
		// TODO: Implement
	}

	return nil
}

// TODO: Implement
func (m *Migrator) validateDstColumns(shutdownCtx context.Context, pool *pgxpool.Pool) error {
	return nil
}

func checkTableExists(conn *pgx.Conn, tableName string) (bool, error) {
	var exists bool

	err := conn.QueryRow(
		context.Background(),
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=$1)", tableName,
	).Scan(&exists)

	return exists, err
}
