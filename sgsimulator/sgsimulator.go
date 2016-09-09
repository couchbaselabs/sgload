package sgsimulator

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type SGSimulator struct{}

func HomeHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("HomeHandler called with req: %+v", req)
	w.Write([]byte("Sync Gateway Simulator\n"))
}

func DBHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("DBHandler called with req: %+v", req)
	w.Write([]byte("Sync Gateway Simulator DB\n"))
}

func NewSGSimulator() *SGSimulator {
	return &SGSimulator{}
}

func (sg *SGSimulator) Run() {

	// TODO: parameterize via CLI
	dbName := "db"
	port := 8000

	r := mux.NewRouter()
	r.HandleFunc("/", HomeHandler)
	r.HandleFunc(fmt.Sprintf("/%v", dbName), DBHandler)
	r.HandleFunc(fmt.Sprintf("/%v/_user/", dbName), DBHandler)
	http.Handle("/", r)

	srv := &http.Server{
		Handler:      r,
		Addr:         fmt.Sprintf("127.0.0.1:%d", port),
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Printf("Listening on %v", srv.Addr)

	log.Fatal(srv.ListenAndServe())

}
