package sgload

import "github.com/inconshreveable/log15"

var (
	// Package wide logger instance
	logger log15.Logger
)

func init() {

	// This will initialize a default logger for the package
	logger = log15.New()

}

func Logger() log15.Logger {
	return logger
}

func SetLogLevel(level log15.Lvl) {
	logger.Info("Setting loglevel", "level", level)
	filteredHandler := log15.LvlFilterHandler(level, logger.GetHandler())
	logger.SetHandler(filteredHandler)
}
