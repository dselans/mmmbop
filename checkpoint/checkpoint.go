package checkpoint

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/timpalpant/gzran"

	"github.com/dselans/mmmbop/checkpoint/types"
	"github.com/dselans/mmmbop/validate"
)

const (
	IndexSuffix = ".index"
)

func Load(checkpointFile, sourceFile, sourceFileType string) (*types.Checkpoint, error) {
	startedAt := time.Now()
	logrus.Debugf("checkpoint loading started at '%s'", startedAt)

	defer func() {
		endedAt := time.Now()
		logrus.Debugf("checkpoint loading completed at '%s'", endedAt)
		logrus.Debugf("checkpoint loading took '%s'", endedAt.Sub(startedAt))
	}()

	var createCheckpoint bool

	// Check if checkpoint file exists; if it does not exist - create it,
	// otherwise, try to load it.
	if _, err := os.Stat(checkpointFile); err != nil {
		if os.IsNotExist(err) {
			createCheckpoint = true
		} else {
			return nil, errors.Wrap(err, "unable to stat checkpoint file")
		}
	}

	if createCheckpoint {
		logrus.Debugf("creating checkpoint file '%s'", checkpointFile)
		return create(checkpointFile, sourceFile, sourceFileType)
	} else {
		logrus.Debugf("loading checkpoint file '%s'", checkpointFile)
		return load(checkpointFile)
	}
}

func load(checkpointFile string) (*types.Checkpoint, error) {
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read checkpoint file")
	}

	cp := &types.Checkpoint{
		Mutex: &sync.Mutex{},
	}

	if err := json.Unmarshal(data, cp); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal checkpoint file")
	}

	if err := validate.Checkpoint(cp); err != nil {
		return nil, errors.Wrap(err, "failed checkpoint validation")
	}

	if cp.CompletedAt != nil && !cp.CompletedAt.IsZero() {
		return nil, errors.New("migration already completed")
	}

	// Open index file
	indexFile, err := os.Open(cp.IndexFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open checkpoint index file")
	}
	defer indexFile.Close()

	// Load index
	index, err := readGzipIndex(indexFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read gzip index")
	}

	cp.Index = index

	// Re-create mutex
	cp.Mutex = &sync.Mutex{}

	return cp, nil
}

func create(checkpointFile, sourceFile, sourceFileType string) (*types.Checkpoint, error) {
	// Create the index
	index, err := generateIndex(sourceFileType, sourceFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate gzip index")
	}

	indexFilename := checkpointFile + IndexSuffix

	indexFile, err := os.Create(indexFilename)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create checkpoint index file %s", indexFilename)
	}
	defer indexFile.Close()

	// Write index to file
	if err = index.WriteTo(indexFile); err != nil {
		return nil, errors.Wrap(err, "error writing index to file")
	}

	// Generate checkpoint JSON file
	cp := &types.Checkpoint{
		IndexFile:   checkpointFile + IndexSuffix,
		IndexOffset: 0,
		SourceFile:  sourceFile,
		StartedAt:   time.Now(),
		LastUpdated: time.Now(),
		Index:       index,
		Mutex:       &sync.Mutex{},
	}

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal checkpoint file")
	}

	// Try to write checkpoint file
	if err := os.WriteFile(checkpointFile, data, 0644); err != nil {
		return nil, errors.Wrap(err, "unable to write checkpoint file")
	}

	return cp, nil
}

func readGzipIndex(f *os.File) (gzran.Index, error) {
	index, err := gzran.LoadIndex(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load index")
	}

	return index, nil
}

func generateIndex(sourceFileType, sourceFile string) (gzran.Index, error) {
	switch sourceFileType {
	case "gzip":
		return generateGzipIndex(sourceFile)
	default:
		return nil, errors.Errorf("unsupported source file type '%s'", sourceFileType)
	}
}

func generateGzipIndex(sourceFile string) (gzran.Index, error) {
	f, err := os.Open(sourceFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open source file")
	}

	// Create a Reader that builds the index as it reads
	reader, err := gzran.NewReaderInterval(f, 4096)
	//reader, err := gzran.NewReader(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create gzip reader")
	}

	// Read through the file to build the index
	_, err = io.Copy(io.Discard, reader)
	if err != nil {
		return nil, errors.Wrap(err, "error reading through file to build index")
	}

	return reader.Index, nil
}
