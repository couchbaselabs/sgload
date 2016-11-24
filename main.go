package main

import (
	"net/http"

	_ "expvar"

	"github.com/couchbaselabs/sgload/cmd"
	"github.com/couchbaselabs/sgload/sgload"
)

func main() {

	logger := sgload.Logger()

	// Expose expvars via http -- needed by mobile-testkit to figure out
	// when the process is finished.
	go func() {
		err := http.ListenAndServe(":9876", http.DefaultServeMux)
		if err != nil {
			logger.Warn("Unable to listen on port 9876.  Expvars won't be exposed", "err", err)
		}
	}()

	cmd.Execute()
}
