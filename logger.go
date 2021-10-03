package pulsar

import (
	"io/ioutil"
	"log"
)

// Logger ...
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type logger struct {
	logger *log.Logger
}

func newLogger() Logger {
	return logger{
		logger: log.New(ioutil.Discard, "[Pulsar] ", log.LstdFlags),
	}
}

func (l logger) Debugf(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}

func (l logger) Infof(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}

func (l logger) Warnf(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}

func (l logger) Errorf(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}
