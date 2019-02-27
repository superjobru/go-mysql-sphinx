package util

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-log/log"
	"gopkg.in/birkirb/loggers.v1"
)

// SetupLogAdapter sets up the pseudo-writer that reroutes logging to logrus
func SetupLogAdapter(logger loggers.Contextual, level string, fields ...interface{}) {
	log.SetDefaultLogger(newLogAdapter(logger, fields...))
	log.SetLevelByName(level)
}

// LogAdapter This abomination pretends to be `log.Handler`,
// while actually it detects message level by parsing message prefix,
// and then tosses that message to the appropriate logger implementation,
type LogAdapter struct {
	methodMap map[string]func(...interface{})
}

func newLogAdapter(logger loggers.Contextual, fields ...interface{}) *log.Logger {
	impl := logger.WithFields(fields...)
	a := LogAdapter{
		methodMap: map[string]func(...interface{}){
			levelToPrefix(log.LevelFatal): impl.Fatal,
			levelToPrefix(log.LevelError): impl.Error,
			levelToPrefix(log.LevelWarn):  impl.Warn,
			levelToPrefix(log.LevelInfo):  impl.Info,
			levelToPrefix(log.LevelDebug): impl.Debug,
			levelToPrefix(log.LevelTrace): impl.Debug,
		},
	}
	return log.New(a, log.Llevel)
}

// Write an implementation of log.Handler
func (l LogAdapter) Write(p []byte) (n int, err error) {
	msg := string(p)
	for prefix, method := range l.methodMap {
		if strings.HasPrefix(msg, prefix) {
			msg = strings.TrimPrefix(msg, prefix)
			msg = strings.TrimSuffix(msg, "\n")
			method(msg)
			break
		}
	}
	return len(p), nil
}

// Close an implementation of log.Handler
func (l LogAdapter) Close() error {
	return nil
}

func levelToPrefix(level log.Level) string {
	return fmt.Sprintf("[%s] ", level)
}
