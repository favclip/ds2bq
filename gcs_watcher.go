package ds2bq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/urlfetch"
)

// ExtractKindName extracts kind name from the object name.
func (obj *GCSObject) ExtractKindName() string {
	id := obj.Name
	if v := strings.LastIndex(id, "/"); v != -1 {
		id = id[v:]
	}
	vs := strings.Split(id, ".")
	if len(vs) != 3 {
		return ""
	}
	if vs[2] != "backup_info" {
		return ""
	}
	return vs[1]
}

// IsRequiredKind reports whether the GCSObject is related required kind.
func (obj *GCSObject) IsRequiredKind(requires []string) bool {
	kindName := obj.ExtractKindName()
	for _, k := range requires {
		if k == kindName {
			return true
		}
	}
	return false
}

// IsImportTarget reports whether the GCSObject is an import target.
func (obj *GCSObject) IsImportTarget(c context.Context, r *http.Request, bucketName string, kindNames []string) bool {
	if bucketName != "" && obj.Bucket != bucketName {
		log.Infof(c, "ds2bq: %s is unexpected bucket", obj.Bucket)
		return false
	}
	gcsHeader := NewGCSHeader(r)
	if gcsHeader.ResourceState != "exists" {
		log.Infof(c, "ds2bq: %s is unexpected state", gcsHeader.ResourceState)
		return false
	}
	if obj.ExtractKindName() == "" {
		log.Infof(c, "ds2bq: this is not backup file: %s", obj.Name)
		return false
	}
	if !obj.IsRequiredKind(kindNames) {
		log.Infof(c, "ds2bq: %s is not required kind", obj.ExtractKindName())
		return false
	}
	log.Infof(c, "ds2bq: %s should imports", obj.Name)
	return true
}

// ToBQJobReq creates a new GCSObjectToBQJobReq from the object.
func (obj *GCSObject) ToBQJobReq() *GCSObjectToBQJobReq {
	return &GCSObjectToBQJobReq{
		Bucket:      obj.Bucket,
		FilePath:    obj.Name,
		KindName:    obj.ExtractKindName(),
		TimeCreated: obj.TimeCreated,
	}
}

// GCSHeader is a header in OCN.
// see https://cloud.google.com/storage/docs/object-change-notification
type GCSHeader struct {
	ChannelID     string
	ClientToken   string
	ResourceID    string
	ResourceState string
	ResourceURI   string
}

// NewGCSHeader returns the header from r.
func NewGCSHeader(r *http.Request) *GCSHeader {
	return &GCSHeader{
		ChannelID:     r.Header.Get("X-Goog-Channel-Id"),
		ClientToken:   r.Header.Get("X-Goog-Channel-Token"),
		ResourceID:    r.Header.Get("X-Goog-Resource-Id"),
		ResourceState: r.Header.Get("X-Goog-Resource-State"),
		ResourceURI:   r.Header.Get("X-Goog-Resource-Uri"),
	}
}

func receiveOCN(c context.Context, obj *GCSObject, queueName, path string) error {
	req := obj.ToBQJobReq()
	b, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return err
	}

	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	t := &taskqueue.Task{
		Path:    path,
		Payload: b,
		Header:  h,
		Method:  "POST",
	}

	_, err = taskqueue.Add(c, t, queueName)
	if err != nil {
		return err
	}

	return nil
}

func insertImportJob(c context.Context, req *GCSObjectToBQJobReq, datasetID string) error {
	log.Infof(c, "ds2bq: bucket: %s, filePath: %s, timeCreated: %s", req.Bucket, req.FilePath, req.TimeCreated)

	if req.Bucket == "" || req.FilePath == "" || req.KindName == "" {
		log.Warningf(c, "ds2bq: unexpected parameters %#v", req)
		return nil
	}

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(c, bigquery.BigqueryScope),
			Base:   &urlfetch.Transport{Context: c},
		},
	}

	bqs, err := bigquery.New(client)
	if err != nil {
		return err
	}

	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: &bigquery.JobConfigurationLoad{
				SourceUris: []string{
					fmt.Sprintf("gs://%s/%s", req.Bucket, req.FilePath),
				},
				DestinationTable: &bigquery.TableReference{
					ProjectId: appengine.AppID(c),
					DatasetId: datasetID,
					TableId:   req.KindName,
				},
				SourceFormat:     "DATASTORE_BACKUP",
				WriteDisposition: "WRITE_TRUNCATE",
			},
		},
	}

	_, err = bqs.Jobs.Insert(appengine.AppID(c), job).Do()
	if err != nil {
		log.Warningf(c, "ds2bq: unexpected error in HandleBackupToBQJob: %s", err)
		return nil
	}

	return nil
}
