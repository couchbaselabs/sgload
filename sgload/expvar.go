package sgload

import "expvar"

var (
	writers  *expvar.Map
	readers  *expvar.Map
	updaters *expvar.Map
)

func init() {
	writers = expvar.NewMap("writers")
	readers = expvar.NewMap("readers")
	updaters = expvar.NewMap("updaters")
}
