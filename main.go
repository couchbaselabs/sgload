package main

import (
	"net/http"

	_ "expvar"

	"github.com/couchbaselabs/sgload/cmd"
)

func main() {

	// Expose expvars via http -- needed by mobile-testkit to figure out
	// when the process is finished.
	go func() {
		http.ListenAndServe(":9876", http.DefaultServeMux)
	}()

	cmd.Execute()
}
