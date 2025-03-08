package pulsar

import (
	"io"
	"log"
)

// Logger ...
type Logger interface {
	Debugf(format string, args ...any)
	Errorf(format string, args ...any)
}

type logger struct {
	logger *log.Logger
}

func newLogger() logger {
	return logger{
		logger: log.New(io.Discard, "[Pulsar] ", log.LstdFlags),
	}
}

func (l logger) Debugf(format string, args ...any) {
	l.logger.Printf(format, args...)
}

func (l logger) Errorf(format string, args ...any) {
	l.logger.Printf(format, args...)
}
