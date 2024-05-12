package migrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/dselans/mmmbop/checkpoint"
	"github.com/dselans/mmmbop/config"
)

type WorkerJob struct {
	Line   string
	Offset int64
}

type CheckpointJob struct {
	WorkerID int
	Offset   int64
}

type Migrator struct {
	cfg  *config.Config
	log  *logrus.Entry
	cp   *checkpoint.Checkpoint
	last time.Time
}

func New(cfg *config.Config) (*Migrator, error) {
	if err := config.Validate(cfg); err != nil {
		return nil, errors.New("error validating config")
	}

	// Load checkpoint (or create if it doesn't exist)
	cp, err := checkpoint.Load(cfg.TOML.Config.CheckpointFile, cfg.TOML.Source.File, cfg.TOML.Source.FileType)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load checkpoint file")
	}

	return &Migrator{
		cfg:  cfg,
		cp:   cp,
		last: time.Time{},
		log:  logrus.WithField("pkg", "migrator"),
	}, nil
}

func (m *Migrator) Run(shutdownCtx context.Context) error {
	wWg := &sync.WaitGroup{}
	errCh := make(chan error, m.cfg.TOML.Config.NumWorkers)
	workCh := make(chan *WorkerJob, m.cfg.TOML.Config.NumWorkers)
	cpWg := &sync.WaitGroup{}
	cpCtx, cpCancel := context.WithCancel(context.Background())
	cpCh := make(chan *CheckpointJob, 1000)
	defer cpCancel()

	// Launch workers
	for i := 0; i < m.cfg.TOML.Config.NumWorkers; i++ {
		wWg.Add(1)

		go func() {
			m.log.Debugf("worker %d start", i)
			defer m.log.Debugf("worker %d exit", i)
			defer wWg.Done()

			if err := m.runWorker(shutdownCtx, i, workCh, cpCh); err != nil {
				errCh <- fmt.Errorf("error in worker %d: %v", i, err)
			}
		}()
	}

	// Launch reader
	go func() {
		m.log.Debug("reader start")
		defer m.log.Debug("reader exit")

		if err := m.runReader(shutdownCtx, workCh); err != nil {
			errCh <- fmt.Errorf("error in reader: %v", err)
		}

		// Reader has finished
		m.log.Debug("reader finished, nothing else to do")
	}()

	// Launch checkpointer
	go func() {
		m.log.Debug("checkpointer start")
		defer m.log.Debug("checkpointer exit")

		cpWg.Add(1)
		defer cpWg.Done()

		if err := m.runCheckpointer(cpCtx, cpCh); err != nil {
			errCh <- fmt.Errorf("error in checkpointer: %v", err)
		}
	}()

	// Read from errCh to detect errors
	select {
	case <-shutdownCtx.Done():
		m.log.Debug("received context done, waiting for workers to stop")
		return m.shutdown(wWg, cpWg, cpCancel)
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("received error: %v", err)
		}

		m.log.Debug("Migrator run completed - shutting down")

		return m.shutdown(wWg, cpWg, cpCancel)

	}
}

func (m *Migrator) shutdown(wg, cpWg *sync.WaitGroup, cpCancel context.CancelFunc) error {
	if err := timeout(func() {
		wg.Wait()
	}, 5*time.Second); err != nil {
		return errors.New("timed out waiting for workers to exit")
	}

	// Workers are stopped, wait for checkpointer to stop
	if err := timeout(func() {
		cpCancel()
		cpWg.Wait()
	}, 5*time.Second); err != nil {
		return errors.New("timed out waiting for checkpointer to exit")
	}

	return nil
}

// Wrapper for executing func with a timeout
func timeout(f func(), t time.Duration) error {
	fin := make(chan bool, 1)

	go func() {
		f()
		fin <- true
	}()

	select {
	case <-fin:
		return nil
	case <-time.After(t):
		return errors.New("timeout")
	}
}
