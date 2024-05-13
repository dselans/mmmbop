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

type ProcessorJob struct {
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

// Run is the main entry point for the migrator.
//
// [Overview]
//
// Run will launch 1 reader, N processors, 1 writer and 1 checkpointer.
//
// The reader will read data from the source and send the data in a *ProcessorJob
// to the processor goroutines via the 'jobCh' channel.
//
// The processors will process the source data and send it to the writer as a
// *WriterJob via the 'writerCh' channel.
//
// The writer will write the data to the destination and upon completion will
// send a *CheckpointJob to the checkpointer via the 'cpCh' channel.
//
// [Shutdown]
//
// Shutdown is complex. Here's the gist:
//
// A shutdown can be initiated in 3 ways:
//  1. CTRL-C or sent SIGKILL
//  2. The reader has finished reading all data
//  3. An unrecoverable error has occurred
//
// For (1): main() catches signals and cancels the main context which is passed
// to Run(). Reader, processors and writer listen to the context.Done() channel
// and will exit when it's closed.
//
// However! The checkpointer is a bit special. It can only shutdown once it knows
// that reader is finished and all processors are done working (as in, there won't
// be anything else sent down the checkpoint channel). For this reason, checkpointer
// has its own shutdown context that is triggered only when reader AND all processors
// have finished.
//
// For (2): Reader will hit EOF and upon exit will send a signal on the finCh.
// Run() listens on finCh and will trigger shutdown of processors, writer and
// lastly, the checkpointer. This is considered a "clean" exit.
//
// For (3): If any of the goroutines encounter an unrecoverable error, they will
// exit and write to the errCh that Run() listens on. Run() will then trigger
// a shutdown of all components.
//
// ^ Take all of that with a grain of salt. That's the general idea. Actual
// implementation may vary. GLHF!
func (m *Migrator) Run(shutdownCtx context.Context, shutdownCancel context.CancelFunc) error {
	// processor wait group
	pWg := &sync.WaitGroup{}

	// writer wait group
	wWg := &sync.WaitGroup{}

	// checkpointer wait group
	cpWg := &sync.WaitGroup{}

	// Error channel that Run() listens on; all goroutines launched by Run()
	// write to this channel if they encounter an unrecoverable error.
	errCh := make(chan error, m.cfg.TOML.Config.NumProcessors)

	// processor job channel
	pjCh := make(chan *ProcessorJob, m.cfg.TOML.Config.NumProcessors)

	// writer job channel
	wjCh := make(chan *WriterJob, m.cfg.TOML.Config.NumWriters)

	// checkpoint job channel
	cpjCh := make(chan *CheckpointJob, 10_000)

	// special channel for checkpointer used for shutdown
	cpControlCh := make(chan bool, 1)

	// "clean exit" shutdown channel
	finCh := make(chan bool, 1)

	// Launch processors
	for i := 0; i < m.cfg.TOML.Config.NumProcessors; i++ {
		pWg.Add(1)

		go func() {
			m.log.Debugf("worker %d start", i)
			defer m.log.Debugf("worker %d exit", i)
			defer pWg.Done()

			if err := m.runProcessor(shutdownCtx, i, pjCh, wjCh); err != nil {
				errCh <- fmt.Errorf("error in worker %d: %v", i, err)
			}
		}()
	}

	// Launch reader
	go func() {
		m.log.Debug("reader start")
		defer m.log.Debug("reader exit")

		if err := m.runReader(shutdownCtx, pjCh); err != nil {
			errCh <- fmt.Errorf("error in reader: %v", err)
		}

		// Reader has finished
		m.log.Debug("reader finished, nothing else to do")
		finCh <- true
	}()

	// TODO: Launch writers
	for i := 0; i < m.cfg.TOML.Config.NumWriters; i++ {
		wWg.Add(1)

		go func() {
			m.log.Debugf("writer %d start", i)
			defer m.log.Debugf("writer %d exit", i)
			defer pWg.Done()

			if err := m.runWriter(shutdownCtx, i, wjCh, cpjCh); err != nil {
				errCh <- fmt.Errorf("error in writer %d: %v", i, err)
			}
		}()
	}

	// Launch checkpointer
	go func() {
		m.log.Debug("checkpointer start")
		defer m.log.Debug("checkpointer exit")

		cpWg.Add(1)
		defer cpWg.Done()

		if err := m.runCheckpointer(cpControlCh, cpjCh); err != nil {
			errCh <- fmt.Errorf("error in checkpointer: %v", err)
		}
	}()

	// Read from errCh to detect errors
	select {
	case <-shutdownCtx.Done():
		m.log.Debug("received context done, waiting for workers to stop")
		return m.shutdown(pWg, cpWg, shutdownCancel, cpControlCh, false)
	case <-finCh:
		m.log.Debug("received completion signal, stopping workers and checkpointer")
		m.log.Info("Migrator run completed")
		return m.shutdown(pWg, cpWg, shutdownCancel, cpControlCh, true)
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("received error: %v", err)
		}

		return m.shutdown(pWg, cpWg, shutdownCancel, cpControlCh, false)
	}
}

// shutdown
func (m *Migrator) shutdown(wWg, cpWg *sync.WaitGroup, shutdownCancel context.CancelFunc, cpControlCh chan<- bool, cleanExit bool) error {
	if err := timeout(func() {
		shutdownCancel()
		wWg.Wait()
	}, 5*time.Second); err != nil {
		return errors.New("timed out waiting for workers to exit")
	}

	// Workers are stopped, tell checkpointer to stop
	if err := timeout(func() {
		cpControlCh <- cleanExit
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
