package main

import (
	"fmt"
	"net/http"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/couchbaselabs/sgload/cmd"
	"github.com/couchbaselabs/sgload/sgload"
)

func main() {

	exposeExpvars(9876)

	cmd.Execute()
}

// Exposes expvars on a port.  Starts with startingPort, but will
// keep incrementing until it finds one.  After a certain number of failed
// attempts it panics
func exposeExpvars(startingPort int) {

	logger := sgload.Logger()

	maxAttempts := 100
	go func() {
		for i := 0; i < maxAttempts; i++ {
			portToTry := startingPort + i
			listenUrl := fmt.Sprintf(":%v", portToTry) // eg: :9876
			logger.Info("Attempting to expose expvars", "port", portToTry)
			err := http.ListenAndServe(listenUrl, http.DefaultServeMux)
			if err != nil {
				logger.Warn("Unable to listen on port.  Will try another port", "port", portToTry)
			}
		}

		panic(fmt.Sprintf("Unable to bind to any ports for expvars"))

	}()

}
