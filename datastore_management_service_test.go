package dstimes

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/favclip/testerator"
	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
)

func TestDatastoreManagementService_Post(t *testing.T) {
	t.SkipNow()

	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	bkMux := ucon.DefaultMux
	defer func() {
		ucon.DefaultMux = bkMux
	}()
	ucon.DefaultMux = ucon.NewServeMux()

	s := NewDatastoreManagementService(
		ManagementWithURLs("/api/datastore-management", "/tq/datastore-management", "/tq/datastore-management/unit"),
		ManagementWithQueueName("datastore-management"),
		ManagementWithExpireDuration(30*24*time.Hour),
	)

	s.SetupWithUconSwagger(swagger.NewPlugin(nil))

	ucon.Middleware(UseAppengineContext)
	ucon.Orthodox()
	ucon.DefaultMux.Prepare()
	http.Handle("/", ucon.DefaultMux)

	body := bytes.NewReader([]byte(`{}`))
	r, err := inst.NewRequest("POST", "/api/gcs/object-change-notification", body)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json;charset=utf-8")

	w := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Fatalf("unexpected %d, expected 200", w.Code)
	}
}
