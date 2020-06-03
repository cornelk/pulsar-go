// +build integration

package pulsar

import (
	"log"
	"testing"
)

// newTestLogger returns a new logger that logs to the provided testing.T.
func newTestLogger(t testing.TB) *log.Logger {
	t.Helper()
	return log.New(testWriter{TB: t}, t.Name()+" ", log.LstdFlags|log.Lshortfile)
}

type testWriter struct {
	testing.TB
}

func (tw testWriter) Write(p []byte) (int, error) {
	tw.Helper()
	tw.Logf("%s", p)
	return len(p), nil
}
