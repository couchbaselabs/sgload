package sgload

import (
	"sync"

	"github.com/inconshreveable/log15"
)

var (
	// Package wide logger instance
	logger      log15.Logger
	loggerMutex sync.Mutex
)

func init() {

	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	// This will initialize a default logger for the package
	logger = log15.New()
}

// Allow clients of this package to set the package-wide logger.
// This should be done before any other calls to functions/methods in the package
func SetLogger(loggerToUse log15.Logger) {

	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	logger = loggerToUse

}
