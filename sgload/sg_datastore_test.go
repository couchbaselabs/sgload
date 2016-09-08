package sgload

import (
	"log"
	"strings"
	"testing"
)

func TestSGAdminURLExplicitPort(t *testing.T) {

	sgDataStore := SGDataStore{
		SyncGatewayAdminPort: 4985,
		SyncGatewayUrl:       "http://localhost:4984/db",
	}
	adminUrl, err := sgDataStore.sgAdminURL()
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if !strings.Contains(adminUrl, "4985") {
		t.Fatalf("Expected to find port 4985 in  %v", adminUrl)
	}

}

func TestSGAdminURLImplicitPort80(t *testing.T) {

	log.Printf("TestSGAdminURLImplicitPort80")
	sgDataStore := SGDataStore{
		SyncGatewayAdminPort: 4985,
		SyncGatewayUrl:       "http://localhost/db",
	}
	adminUrl, err := sgDataStore.sgAdminURL()
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if !strings.Contains(adminUrl, "4985") {
		t.Fatalf("Expected to find port 4985 in %v", adminUrl)
	}

}
