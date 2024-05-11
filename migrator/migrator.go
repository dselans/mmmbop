package migrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/dselans/mmmbop/config"
)

type Job struct {
}

type Checkpoint struct {
}

type Migrator struct {
	cfg *config.Config
	log *logrus.Entry
}

func New(cfg *config.Config) (*Migrator, error) {
	if err := config.Validate(cfg); err != nil {
		return nil, errors.New("error validating config")
	}

	return &Migrator{
		cfg: cfg,
		log: logrus.WithField("pkg", "migrator"),
	}, nil
}

func (m *Migrator) Run(shutdownCtx context.Context, cfg *config.Config) error {
	wg := &sync.WaitGroup{}
	errCh := make(chan error, m.cfg.TOML.Config.NumWorkers)
	workCh := make(chan *Job, m.cfg.TOML.Config.NumWorkers)
	cpWg := &sync.WaitGroup{}
	cpCtx, cpCancel := context.WithCancel(context.Background())
	cpCh := make(chan *Checkpoint, 1000)

	// Launch workers
	for i := 0; i < m.cfg.TOML.Config.NumWorkers; i++ {
		wg.Add(1)

		go func() {
			m.log.Debugf("worker %d start", i)
			defer m.log.Debugf("worker %d exit", i)
			defer wg.Done()

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
		m.log.Debug("received context done")
		return m.waitWorkers(wg, cpWg, cpCancel)
	case err := <-errCh:
		cpCancel()

		if err != nil {
			return fmt.Errorf("received error: %v", err)
		}

		m.log.Debug("Migrator run completed")

		return m.waitWorkers(wg, cpWg, cpCancel)

	}
}

func (m *Migrator) waitWorkers(wg, cpWg *sync.WaitGroup, cpCancel context.CancelFunc) error {
	exitCh := make(chan bool, 1)

	go func() {
		wg.Wait()
		exitCh <- true
	}()

	select {
	case <-exitCh:
		// Workers have exited, can stop the checkpointer
		cpCancel()
		cpWg.Wait()

		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for workers and/or checkpointer to exit")
	}
}
