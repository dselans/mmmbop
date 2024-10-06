package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/sirupsen/logrus"

	"github.com/dselans/mmmbop/config"
	"github.com/dselans/mmmbop/migrator"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}

	//if cfg.CLI.Debug {
	//	logrus.Info("debug mode enabled")
	//	logrus.SetLevel(logrus.DebugLevel)
	//}

	logrus.SetLevel(logrus.DebugLevel)

	displayConfig(cfg)

	logrus.Info("Starting migrator...")

	// Load config, checkpoint file, generate/load index etc.
	m, err := migrator.New(cfg)
	if err != nil {
		logrus.Errorf("unable to create migrator: %s", err)
		os.Exit(1)
	}

	// Context used for facilitating shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Detect ctrl-c and kill signals for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)

	go func() {
		sig := <-c
		logrus.Debugf("Received system call: %+v", sig)
		logrus.Debug("Telling migrator to stop...")
		cancel()
	}()

	// Run the migrator
	if err := m.Run(ctx, cancel); err != nil {
		logrus.Errorf("error during migrator run: %s", err)
		os.Exit(1)
	}
}

func displayConfig(cfg *config.Config) {
	if cfg == nil {
		return
	}

	logrus.Infof("Loaded TOML config from '%s':", cfg.CLI.ConfigFile)
	logrus.Info("  [CLI]")
	logrus.Infof("  version: %s", config.VERSION)
	logrus.Infof("  debug: %v", cfg.CLI.Debug)
	logrus.Infof("  config file: %s", cfg.CLI.ConfigFile)
	logrus.Infof("  report output: %s", cfg.CLI.ReportOutput)
	logrus.Infof("  report interval: %s", cfg.CLI.ReportInterval)
	logrus.Infof("  dry run: %v", cfg.CLI.DryRun)
	logrus.Infof("  migrate: %v", cfg.CLI.Migrate)
	logrus.Infof("  disable resume: %v", cfg.CLI.DisableResume)
	logrus.Infof("  disable color: %v", cfg.CLI.DisableColor)
	logrus.Infof("  quiet: %v", cfg.CLI.Quiet)
	logrus.Info("")
	logrus.Info("  [CONFIG]")
	logrus.Infof("  config.num_workers: %d", cfg.TOML.Config.NumProcessors)
	logrus.Infof("  config.batch_size: %d", cfg.TOML.Config.BatchSize)
	logrus.Infof("  config.checkpoint_file: %s", cfg.TOML.Config.CheckpointFile)
	logrus.Infof("  config.checkpoint_index: %s", cfg.TOML.Config.CheckpointIndex)
	logrus.Info("")
	logrus.Info("  [SOURCE]")
	logrus.Infof("  source.file: %s", cfg.TOML.Source.File)
	logrus.Infof("  source.file_type: %s", cfg.TOML.Source.FileType)
	logrus.Infof("  source.file_contents: %s", cfg.TOML.Source.FileContents)
	logrus.Info("")
	logrus.Info("  [DESTINATION]")
	logrus.Infof("  destination.type: %s", cfg.TOML.Destination.Type)
	logrus.Infof("  destination.dsn: %s", cfg.TOML.Destination.DSN)
	logrus.Info("")
	logrus.Info("  [MAPPING]")

	for k, v := range *cfg.TOML.Mapping {
		logrus.Infof("  mapping.%s:", k)

		for i, m := range v {
			logrus.Infof("    [%d] src: %s ", i, m.Src)
			logrus.Infof("    [%d] dst: %s ", i, m.Dst)
			logrus.Infof("    [%d] conv: %s ", i, m.Conv)

			if m.Required != nil {
				logrus.Infof("    [%d] required: %v ", i, *m.Required)
			}

			if m.DupeCheck != nil {
				logrus.Infof("    [%d] dupe_check: %v ", i, *m.DupeCheck)
			}

			// If NOT last entry, print separator
			if i != len(v)-1 {
				logrus.Info("    ---")
			}
		}
	}
}
