package splmysql

import (
	"os"

	"github.com/Sirupsen/logrus"
)

func (sr *Runner) initLogger() {
	if sr.Logger != nil {
		return
	}

	sr.Logger = logrus.New()
	sr.Logger.Formatter = &logrus.TextFormatter{
		FullTimestamp: false,
	}
	sr.Logger.Out = os.Stdout

}

const (
	// LogSuppressLevel is loglevel of no logger messages
	LogSuppressLevel = 0
	// LogErrorLevel is loglevel to log error level messages
	LogErrorLevel = 1
	// LogWarnLevel is loglevel to log warning and error level message
	LogWarnLevel = 2
	// LogInfoLevel is loglevel to log information level messages
	LogInfoLevel = 3
	// LogDebugLevel is loglevel to log debug information level messages
	LogDebugLevel = 4
	// LogTraceLevel is loglevel to log trace information level messages
	LogTraceLevel = 5
	// LogDefaultLevel is this library default level, use LogSuppressLevel.
	LogDefaultLevel = LogSuppressLevel
)

func (sr *Runner) tracef(s string, v ...interface{}) {
	//sr.Logger.Printf("trace: "+s, v...)
	if sr.LogLevel >= LogTraceLevel {
		sr.Logger.Debugf(s, v...)
	}
}

func (sr *Runner) debugf(s string, v ...interface{}) {
	//sr.Logger.Printf("debug: "+s, v...)
	sr.Logger.Debugf(s, v...)
}

func (sr *Runner) infof(s string, v ...interface{}) {
	//sr.Logger.Printf("info: "+s, v...)
	sr.Logger.Infof(s, v...)
}

func (sr *Runner) warnf(s string, v ...interface{}) {
	//sr.Logger.Printf("error: "+s, v...)
	sr.Logger.Warnf(s, v...)
}

func (sr *Runner) errorf(s string, v ...interface{}) {
	//sr.Logger.Printf("error: "+s, v...)
	sr.Logger.Errorf(s, v...)
}
