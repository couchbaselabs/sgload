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
