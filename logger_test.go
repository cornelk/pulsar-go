//go:build integration

package pulsar

import (
	"io"
	"log"
	"testing"
)

type testLogger struct {
	logger *log.Logger
	testing.TB
}

// newTestLogger returns a new logger that logs to the provided testing.TB.
func newTestLogger(tb testing.TB) testLogger {
	tb.Helper()
	return testLogger{
		logger: log.New(io.Discard, "[Pulsar] ", log.LstdFlags),
		TB:     tb,
	}
}

func (l testLogger) Debugf(format string, args ...interface{}) {
	l.TB.Logf(format, args...)
}

func (l testLogger) Errorf(format string, args ...interface{}) {
	l.TB.Errorf(format, args...)
}
