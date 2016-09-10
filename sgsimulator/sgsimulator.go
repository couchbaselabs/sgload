package sgsimulator

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type SGSimulator struct{}

type BulkDocsResponse struct {
	Name    string
	Hobbies []string
}

func HomeHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("HomeHandler called with req: %+v", req)
	w.Write([]byte("Sync Gateway Simulator\n"))
}

func DoNothingHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("DoNothingHandler called with req: %+v", req)
	w.Write([]byte("Sync Gateway Simulator DB\n"))
}

func BulkDocsHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("BulkDocsHandler called with req: %+v", req)

	bulkDocsResponseSlice := []map[string]string{}
	bulkDocResponse := map[string]string{
		"id":  "1",
		"rev": "1-34243",
	}
	bulkDocsResponseSlice = append(bulkDocsResponseSlice, bulkDocResponse)
	js, err := json.Marshal(bulkDocsResponseSlice)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)

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
	r.HandleFunc("/", DoNothingHandler)
	dbRouter := r.PathPrefix(fmt.Sprintf("/%v", dbName)).Subrouter()
	dbRouter.Path("/_user/").HandlerFunc(DoNothingHandler)
	dbRouter.Path("/_bulk_docs").HandlerFunc(BulkDocsHandler)

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
