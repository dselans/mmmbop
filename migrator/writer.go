package migrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/dselans/mmmbop/config"
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
	if err := m.validateDstColumns(pool); err != nil {
		return errors.Wrap(err, "error validating destination columns")
	}

	return nil
}

func parseDestination(dst string) (string, string) {
	// dst is in the format "table:column"
	parts := strings.Split(dst, ":")
	if len(parts) != 2 {
		return "", ""
	}

	return parts[0], parts[1]
}

type Table string

type Column struct {
	Name string
	Conv string
}

func getDestinationMappings(input *config.TOMLMapping) (map[Table][]Column, error) {
	mappings := make(map[Table][]Column)

MAIN:
	for mName, mEntries := range *input {
		for _, entry := range mEntries {
			tStr, cStr := parseDestination(entry.Dst)
			if tStr == "" || cStr == "" {
				return nil, errors.Errorf("unable to determine destination table or column for mapping '%s'", mName)
			}

			t := Table(tStr)

			if _, ok := mappings[t]; !ok {
				mappings[t] = make([]Column, 0)
			}

			// Get rid of dupes
			for _, col := range mappings[t] {
				if col.Name == cStr {
					continue MAIN
				}
			}

			// Dupe not detected, add it to map
			mappings[t] = append(mappings[t], Column{
				Name: cStr,
				Conv: entry.Conv,
			})
		}
	}

	return mappings, nil
}

func (m *Migrator) validateDstTables(shutdownCtx context.Context, pool *pgxpool.Pool) error {
	dstMappings, err := getDestinationMappings(m.cfg.TOML.Mapping)
	if err != nil {
		return errors.Wrap(err, "error getting destination mappings")
	}

	for table, _ := range dstMappings {
		exists, err := checkTableExists(shutdownCtx, pool, table)
		if err != nil {
			return errors.Wrapf(err, "error checking if table '%s' exists", table)
		}

		if !exists {
			return errors.Errorf("destination table '%s' does not exist", table)
		}
	}

	return nil
}

// TODO: Implement
func (m *Migrator) validateDstColumns(pool *pgxpool.Pool) error {
	dstMappings, err := getDestinationMappings(m.cfg.TOML.Mapping)
	if err != nil {
		return errors.Wrap(err, "error getting destination mappings")
	}

	for table, columns := range dstMappings {
		for _, c := range columns {
			if err := checkColumn(pool, table, c); err != nil {
				return errors.Wrapf(err, "error during column check for '%s.%s'", table, c.Name)
			}
		}
	}

	return nil
}

func checkColumn(pool *pgxpool.Pool, t Table, c Column) error {
	var dtype string
	query := `
        SELECT data_type FROM information_schema.columns 
        WHERE table_name=$1 AND column_name=$2
    `
	err := pool.QueryRow(context.Background(), query, t, c).Scan(&dtype)
	if err != nil {
		return errors.Wrap(err, "error querying information_schema.columns")
	}

	// Check if column type matches
	fmt.Println("our dtype is: ", dtype)

	return errors.New("tmp error return")
}

func checkTableExists(shutdownCtx context.Context, pool *pgxpool.Pool, t Table) (bool, error) {
	var exists bool

	err := pool.QueryRow(
		shutdownCtx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=$1)", string(t),
	).Scan(&exists)

	return exists, err
}
