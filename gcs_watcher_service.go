package dstimes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/favclip/ucon"
	"github.com/mjibson/goon"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/urlfetch"
)

// GCSWatcherOption provides option value of GCSWatcherService.
type GCSWatcherOption interface {
	implements(s *gcsWatcherService)
}

type gcsWatcherURLOption struct {
	APIObjectChangeNotificationURL string
	ObjectToBigQueryURL            string
}

func (o *gcsWatcherURLOption) implements(s *gcsWatcherService) {
	if v := o.APIObjectChangeNotificationURL; v != "" {
		s.OCNReceiveURL = v
	}
	if v := o.ObjectToBigQueryURL; v != "" {
		s.GCSObjectToBQJobURL = v
	}
}

// GCSWatcherWithURLs provies API endpoint URL.
func GCSWatcherWithURLs(apiURL, tqURL string) GCSWatcherOption {
	return &gcsWatcherURLOption{
		APIObjectChangeNotificationURL: apiURL,
		ObjectToBigQueryURL:            tqURL,
	}
}

type gcsWatcherQueueNameOption struct {
	QueueName string
}

func (o *gcsWatcherQueueNameOption) implements(s *gcsWatcherService) {
	s.QueueName = o.QueueName
}

// GCSWatcherWithQueueName provides queue name of TaskQueue.
func GCSWatcherWithQueueName(queueName string) GCSWatcherOption {
	return &gcsWatcherQueueNameOption{
		QueueName: queueName,
	}
}

type gcsWatcherBucketNameOption struct {
	BackupBucketName string
}

func (o *gcsWatcherBucketNameOption) implements(s *gcsWatcherService) {
	s.BackupBucketName = o.BackupBucketName
}

// GCSWatcherWithBackupBucketName provides bucket name of datastatore backup target.
func GCSWatcherWithBackupBucketName(bucketName string) GCSWatcherOption {
	return &gcsWatcherBucketNameOption{
		BackupBucketName: bucketName,
	}
}

type gcsWatcherTargetKindsOption struct {
	ImportTargetKinds []interface{}
}

func (o *gcsWatcherTargetKindsOption) implements(s *gcsWatcherService) {
	s.ImportTargetKinds = o.ImportTargetKinds
}

// GCSWatcherWithTargetKinds provides target kind that insert into BigQuery.
// interface{} processed by Kind method of *goon.Goon.
func GCSWatcherWithTargetKinds(targets ...interface{}) GCSWatcherOption {
	return &gcsWatcherTargetKindsOption{
		ImportTargetKinds: targets,
	}
}

type gcsWatcherTargetKindNamesOption struct {
	ImportTargetKindNames []string
}

func (o *gcsWatcherTargetKindNamesOption) implements(s *gcsWatcherService) {
	s.ImportTargetKindNames = o.ImportTargetKindNames
}

// GCSWatcherWithTargetKindNames provides target kind that insert into BigQuery.
func GCSWatcherWithTargetKindNames(names ...string) GCSWatcherOption {
	return &gcsWatcherTargetKindNamesOption{
		ImportTargetKindNames: names,
	}
}

type gcsWatcherDatasetIDOption struct {
	DatasetID string
}

func (o *gcsWatcherDatasetIDOption) implements(s *gcsWatcherService) {
	s.DatasetID = o.DatasetID
}

// GCSWatcherWithDatasetID provides Dataset ID of BigQuery.
func GCSWatcherWithDatasetID(id string) GCSWatcherOption {
	return &gcsWatcherDatasetIDOption{
		DatasetID: id,
	}
}

type gcsWatcherWithContext struct {
	Func func(c context.Context) (GCSWatcherOption, error)
}

func (o *gcsWatcherWithContext) implements(s *gcsWatcherService) {
	s.WithContextFuncs = append(s.WithContextFuncs, o.Func)
}

// GCSWatcherWithAfterContext can process GCSWatcherOption with context.
func GCSWatcherWithAfterContext(f func(c context.Context) (GCSWatcherOption, error)) GCSWatcherOption {
	return &gcsWatcherWithContext{
		Func: f,
	}
}

type gcsWatcherService struct {
	QueueName             string
	BackupBucketName      string
	ImportTargetKinds     []interface{} // convert to ImportTargetKindNames using goon.
	ImportTargetKindNames []string
	DatasetID             string

	WithContextFuncs     []func(c context.Context) (GCSWatcherOption, error)
	ProcessedWithContext bool

	OCNReceiveURL       string
	GCSObjectToBQJobURL string
}

// GCSWatcherService serves GCS Object Change Notification receiving APIs.
type GCSWatcherService interface {
	SetupWithUcon()
	HandleOCN(c context.Context, r *http.Request, obj *GCSObject) error
	HandleBackupToBQJob(c context.Context, req *GCSObjectToBQJobReq) error
}

// NewGCSWatcherService returns ready to use GCSWatcherService.
func NewGCSWatcherService(opts ...GCSWatcherOption) (GCSWatcherService, error) {
	s := &gcsWatcherService{
		QueueName:           "",
		BackupBucketName:    "",
		OCNReceiveURL:       "/api/gcs/object-change-notification",
		GCSObjectToBQJobURL: "/tq/gcs/object-to-bq",
	}

	for _, opt := range opts {
		opt.implements(s)
	}

	if len(s.ImportTargetKinds) == 0 && len(s.ImportTargetKindNames) == 0 {
		return nil, ErrInvalidState
	}
	if s.DatasetID == "" {
		return nil, ErrInvalidState
	}

	return s, nil
}

func (s *gcsWatcherService) SetupWithUcon() {
	ucon.HandleFunc("GET,POST", s.OCNReceiveURL, s.HandleOCN) // from GCS, This API must not requires admin role.
	ucon.HandleFunc("GET,POST", s.GCSObjectToBQJobURL, s.HandleBackupToBQJob)
}

// GCSObject is received json data from GCS OCN.
type GCSObject struct {
	ID             string    `json:"id"`
	SelfLink       string    `json:"selfLink"`
	Name           string    `json:"name"`
	Bucket         string    `json:"bucket"`
	Generation     string    `json:"generation"`
	MetaGeneration string    `json:"metageneration"`
	ContentType    string    `json:"contentType"`
	Updated        time.Time `json:"updated"`
	Size           int64     `json:"size,string"`
	Md5Hash        string    `json:"md5Hash"`
	MediaLink      string    `json:"mediaLink"`
	Crc32c         string    `json:"crc32c"`
	Etag           string    `json:"etag"`
	TimeCreated    time.Time `json:"timeCreated"`
	TimeDeleted    time.Time `json:"timeDeleted"`
}

func (s *gcsWatcherService) processWithContext(c context.Context) error {
	if s.ProcessedWithContext {
		for _, f := range s.WithContextFuncs {
			opt, err := f(c)
			if err != nil {
				return err
			}
			opt.implements(s)
		}
		s.ProcessedWithContext = true
	}

	return nil
}

func (s *gcsWatcherService) HandleOCN(c context.Context, r *http.Request, obj *GCSObject) error {
	if err := s.processWithContext(c); err != nil {
		return err
	}

	// see https://cloud.google.com/storage/docs/object-change-notification

	channelID := r.Header.Get("X-Goog-Channel-Id")
	clientToken := r.Header.Get("X-Goog-Channel-Token")
	resourceID := r.Header.Get("X-Goog-Resource-Id")
	resourceState := r.Header.Get("X-Goog-Resource-State")
	resourceURI := r.Header.Get("X-Goog-Resource-Uri")

	log.Infof(c, "channelID: %s, clientToken: %s, resourceID: %s, resourceState: %s, resourceURI: %s", channelID, clientToken, resourceID, resourceState, resourceURI)

	for k, v := range r.Header {
		log.Infof(c, "Header %s: %s", k, v)
	}

	log.Infof(c, "payload: %#v", obj)

	if s.BackupBucketName != "" && obj.Bucket != s.BackupBucketName {
		log.Infof(c, "%s is unexpected bucket", obj.Bucket)
		return nil
	}

	if resourceState != "exists" {
		log.Infof(c, "%s is unexpected state", resourceState)
		return nil
	}

	kindName := s.extractKindName(obj.Name)
	if kindName == "" {
		log.Infof(c, "This is not backup file")
		return nil
	}

	g := goon.FromContext(c)

	if len(s.ImportTargetKindNames) == 0 && len(s.ImportTargetKinds) != 0 {
		for _, target := range s.ImportTargetKinds {
			s.ImportTargetKindNames = append(s.ImportTargetKindNames, g.Kind(target))
		}
	}

	found := false
	for _, targetName := range s.ImportTargetKindNames {
		if targetName == kindName {
			found = true
		}
	}

	if !found {
		log.Infof(c, "%s is not required kind", kindName)
		return nil
	}

	log.Infof(c, "%s should imports", obj.Name)

	jsonReq := &GCSObjectToBQJobReq{
		Bucket:      obj.Bucket,
		FilePath:    obj.Name,
		KindName:    kindName,
		TimeCreated: obj.TimeCreated,
	}
	b, err := json.MarshalIndent(jsonReq, "", "  ")
	if err != nil {
		return err
	}

	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	task := &taskqueue.Task{
		Path:    s.GCSObjectToBQJobURL,
		Payload: b,
		Header:  h,
		Method:  "POST",
	}

	_, err = taskqueue.Add(c, task, s.QueueName)
	if err != nil {
		return err
	}

	return nil
}

// GCSObjectToBQJobReq means request of OCN to BQ.
type GCSObjectToBQJobReq struct {
	Bucket      string    `json:"bucket"`
	FilePath    string    `json:"filePath"`
	KindName    string    `json:"kindName"`
	TimeCreated time.Time `json:"TimeCreated"`
}

func (s *gcsWatcherService) HandleBackupToBQJob(c context.Context, req *GCSObjectToBQJobReq) error {
	if err := s.processWithContext(c); err != nil {
		return err
	}

	log.Infof(c, "bucket: %s, filePath: %s, timeCreated: %s", req.Bucket, req.FilePath, req.TimeCreated)

	if req.Bucket == "" || req.FilePath == "" || req.KindName == "" {
		log.Warningf(c, "unexpected parameters")
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
					DatasetId: s.DatasetID,
					TableId:   req.KindName,
				},
				SourceFormat:     "DATASTORE_BACKUP",
				WriteDisposition: "WRITE_TRUNCATE",
			},
		},
	}
	_, err = bqs.Jobs.Insert(appengine.AppID(c), job).Do()
	if err != nil {
		log.Warningf(c, "unexpected error in HandleBackupToBQJob: %s", err.Error())
		return nil
	}

	return nil
}

// extractKindName from agtzfnN0Zy1jaGFvc3JACxIcX0FFX0RhdGFzdG9yZUFkbWluX09wZXJhdGlvbhjx52oMCxIWX0FFX0JhY2t1cF9JbmZvcm1hdGlvbhgBDA.Article.backup_info like ID value.
func (s *gcsWatcherService) extractKindName(id string) string {
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
