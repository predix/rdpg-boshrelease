package backups_test

import (
	"testing"

	"github.com/starkandwayne/rdpg-acceptance-tests/helpers"
)

func TestService(t *testing.T) {
	helpers.PrepareAndRunTests("Backups", t, false)
}
