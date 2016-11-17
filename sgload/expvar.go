package sgload

import "expvar"

var (
	writersProgressStats  *expvar.Map
	readersProgressStats  *expvar.Map
	updatersProgressStats *expvar.Map
)

func init() {
	writersProgressStats = expvar.NewMap("writers")
	readersProgressStats = expvar.NewMap("readers")
	updatersProgressStats = expvar.NewMap("updaters")
}

// Since sometimes we want to just ignore any calls to update expvarstats
// depending on CLI args, wrap up expvar.Map behind an interface and offer
// a "no-op" version
type ExpVarStatsCollector interface {
	Set(key string, av expvar.Var)
	Add(key string, delta int64)
}

type NoOpExpvarStatsCollector struct{}

func (e NoOpExpvarStatsCollector) Set(key string, av expvar.Var) {}

func (e NoOpExpvarStatsCollector) Add(key string, delta int64) {}
