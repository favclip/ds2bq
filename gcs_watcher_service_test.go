package ds2bq

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/favclip/testerator"
	"github.com/favclip/ucon"
)

func TestGCSWatcherService_Post(t *testing.T) {
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

	s, err := NewGCSWatcherService(
		GCSWatcherWithURLs("/api/gcs/object-change-notification", "/tq/gcs/object-change-notification"),
		GCSWatcherWithQueueName("datastore-to-bq"),
		GCSWatcherWithBackupBucketName("example-datastore-backup"),
		GCSWatcherWithTargetKinds("Test"),
		GCSWatcherWithDatasetID("backup-ds"),
	)
	if err != nil {
		t.Fatal(err.Error())
	}
	s.SetupWithUcon()

	ucon.Middleware(UseAppengineContext)
	ucon.Orthodox()
	ucon.DefaultMux.Prepare()
	http.Handle("/", ucon.DefaultMux)

	payload := `
{
 "kind": "storage#object",
 "id": "BucketName/ObjectName",
 "selfLink": "https://www.googleapis.com/storage/v1/b/BucketName/o/ObjectName",
 "name": "ObjectName",
 "bucket": "BucketName",
 "generation": "1367014943964000",
 "metageneration": "1",
 "contentType": "application/octet-stream",
 "updated": "2013-04-26T22:22:23.832Z",
 "size": "10",
 "md5Hash": "xHZY0QLVuYng2gnOQD90Yw==",
 "mediaLink": "https://www.googleapis.com/storage/v1/b/BucketName/o/ObjectName?generation=1367014943964000&alt=media",
 "owner": {
  "entity": "user-007b2a38086590de0a47c786e54b1d0a21c02d062fcf3ebbaf9b63edb9c8db0c",
  "entityId": "007b2a38086590de0a47c786e54b1d0a21c02d062fcf3ebbaf9b63edb9c8db0c"
 },
 "crc32c": "C7+82w==",
 "etag": "COD2jMGv6bYCEAE="
}
	`
	body := bytes.NewReader([]byte(payload))
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

func TestGCSWatcherService_extractKindName(t *testing.T) {
	s := gcsWatcherService{}

	{
		kind := s.extractKindName("agtzfnN0Zy1jaGFvc3JACxIcX0FFX0RhdGFzdG9yZUFkbWluX09wZXJhdGlvbhjx52oMCxIWX0FFX0JhY2t1cF9JbmZvcm1hdGlvbhgBDA.Article.backup_info")
		if e, g := "Article", kind; e != g {
			t.Fatalf("expected kind %s; got %s", e, g)
		}
	}
	{
		kind := s.extractKindName("2017-11-14T06:47:01_23208/all_namespaces/kind_Item/all_namespaces_kind_Item.export_metadata")
		if e, g := "Item", kind; e != g {
			t.Fatalf("expected kind %s; got %s", e, g)
		}
	}
}

func TestGCSWatcherService_extractKindNameForDatastoreAdmin(t *testing.T) {
	s := gcsWatcherService{}
	kind := s.extractKindNameForDatastoreAdmin("agtzfnN0Zy1jaGFvc3JACxIcX0FFX0RhdGFzdG9yZUFkbWluX09wZXJhdGlvbhjx52oMCxIWX0FFX0JhY2t1cF9JbmZvcm1hdGlvbhgBDA.Article.backup_info")
	if e, g := "Article", kind; e != g {
		t.Fatalf("expected kind %s; got %s", e, g)
	}
}

func TestGCSWatcherService_extractKindNameForDatastoreExport(t *testing.T) {
	s := gcsWatcherService{}
	kind := s.extractKindNameForDatastoreExport("2017-11-14T06:47:01_23208/all_namespaces/kind_Item/all_namespaces_kind_Item.export_metadata")
	if e, g := "Item", kind; e != g {
		t.Fatalf("expected kind %s; got %s", e, g)
	}
}
