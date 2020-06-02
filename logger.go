package pulsar

import (
	"io/ioutil"
	"log"
)

// Logger ...
type Logger interface {
	Printf(format string, v ...interface{})
}

func newLogger() Logger {
	return log.New(ioutil.Discard, "[Pulsar] ", log.LstdFlags)
}
