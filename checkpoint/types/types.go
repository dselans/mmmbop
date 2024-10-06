package types

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/timpalpant/gzran"
)

// Checkpoint contains checkpoint info
type Checkpoint struct {
	IndexFile   string     `json:"index_file"`
	IndexOffset int64      `json:"index_offset"`
	SourceFile  string     `json:"source_file"`
	StartedAt   time.Time  `json:"started_at"`
	LastUpdated time.Time  `json:"last_updated"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Not marshalled
	Index gzran.Index `json:"-"`

	*sync.Mutex
}

func (cp *Checkpoint) Save(checkpointFile string) error {
	cp.Lock()
	defer cp.Unlock()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return errors.Wrap(err, "unable to marshal checkpoint file")
	}

	// TODO: Improve writing to file by first writing to temp file and renaming
	if err := os.WriteFile(checkpointFile, data, 0644); err != nil {
		return errors.Wrap(err, "unable to write checkpoint file")
	}

	return nil
}
