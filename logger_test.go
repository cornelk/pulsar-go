// +build integration

package pulsar

import (
	"log"
	"testing"
)

// newTestLogger returns a new logger that logs to the provided testing.T.
func newTestLogger(tb testing.TB) *log.Logger {
	tb.Helper()
	return log.New(testWriter{TB: tb}, tb.Name()+" ", log.LstdFlags|log.Lshortfile)
}

type testWriter struct {
	testing.TB
}

func (tw testWriter) Write(p []byte) (int, error) {
	tw.Helper()
	tw.Logf("%s", p)
	return len(p), nil
}
