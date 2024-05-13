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
	Data   string
	Offset int64
}

type CheckpointJob struct {
	WorkerID int
	Offset   int64
}

type Migrator struct {
	cfg         *config.Config
	log         *logrus.Entry
	cp          *checkpoint.Checkpoint
	last        time.Time
	checksums   map[string]struct{}
	checksumsMu *sync.Mutex
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
		cfg:         cfg,
		cp:          cp,
		last:        time.Time{},
		log:         logrus.WithField("pkg", "migrator"),
		checksums:   make(map[string]struct{}),
		checksumsMu: &sync.Mutex{},
	}, nil
}

func (m *Migrator) Run(shutdownCtx context.Context, shutdownCancel context.CancelFunc) error {
	wWg := &sync.WaitGroup{}
	errCh := make(chan error, m.cfg.TOML.Config.NumWorkers)
	workCh := make(chan *WorkerJob, m.cfg.TOML.Config.NumWorkers)
	cpWg := &sync.WaitGroup{}
	cpCh := make(chan *CheckpointJob, 10_000)
	cpControlCh := make(chan bool, 1)
	finCh := make(chan bool, 1)

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
		finCh <- true
	}()

	// Launch checkpointer
	go func() {
		m.log.Debug("checkpointer start")
		defer m.log.Debug("checkpointer exit")

		cpWg.Add(1)
		defer cpWg.Done()

		if err := m.runCheckpointer(cpControlCh, cpCh); err != nil {
			errCh <- fmt.Errorf("error in checkpointer: %v", err)
		}
	}()

	// Read from errCh to detect errors
	select {
	case <-shutdownCtx.Done():
		m.log.Debug("received context done, waiting for workers to stop")
		return m.shutdown(wWg, cpWg, shutdownCancel, cpControlCh, true)
	case <-finCh:
		m.log.Debug("received completion signal, stopping workers and checkpointer")
		m.log.Info("Migrator run completed")
		return m.shutdown(wWg, cpWg, shutdownCancel, cpControlCh)
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("received error: %v", err)
		}

		return m.shutdown(wWg, cpWg, shutdownCancel, cpControlCh, true)
	}
}

func (m *Migrator) shutdown(wWg, cpWg *sync.WaitGroup, shutdownCancel context.CancelFunc, cpControlCh chan<- bool, interrupt ...bool) error {
	if err := timeout(func() {
		shutdownCancel()
		wWg.Wait()
	}, 5*time.Second); err != nil {
		return errors.New("timed out waiting for workers to exit")
	}

	// Workers are stopped, wait for checkpointer to stop
	if err := timeout(func() {
		if len(interrupt) > 0 {
			cpControlCh <- true
		} else {
			cpControlCh <- false
		}
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
