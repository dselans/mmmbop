package validate

import (
	"github.com/pkg/errors"

	"github.com/dselans/mmmbop/checkpoint/types"
)

func Checkpoint(cp *types.Checkpoint) error {
	if cp == nil {
		return errors.New("checkpoint is nil")
	}

	// TODO: Additional validation needed in future?

	return nil
}
