package util

import (
	"io"
	"log"

	mapper "github.com/birkirb/loggers-mapper-logrus"
	"gopkg.in/birkirb/loggers.v1"
)

// NewStdLogger creates a separate logger for services
// that specifically require log.Logger instance
// The writer for that logger still needs to be closed manually!
func NewStdLogger(loggerInterface loggers.Contextual) (*log.Logger, *io.PipeWriter) {
	logger := loggerInterface.(*mapper.Logger)
	logrus := logger.Logger
	writer := logrus.Writer()
	return log.New(writer, "", 0), writer
}
