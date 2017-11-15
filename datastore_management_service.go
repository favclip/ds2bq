package ds2bq

import (
	"context"
	"net/http"
	"time"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"google.golang.org/appengine/taskqueue"
)

// ManagementOption provides option value of datastoreManagementService.
type ManagementOption interface {
	implements(s *datastoreManagementService)
}

type managementURLOption struct {
	APIDeleteBackupsURL   string
	DeleteOldBackupURL    string
	DeleteUnitOfBackupURL string
}

func (o *managementURLOption) implements(s *datastoreManagementService) {
	if v := o.APIDeleteBackupsURL; v != "" {
		s.APIDeleteBackupsURL = v
	}
	if v := o.DeleteOldBackupURL; v != "" {
		s.DeleteOldBackupURL = v
	}
	if v := o.DeleteUnitOfBackupURL; v != "" {
		s.DeleteUnitOfBackupURL = v
	}
}

// ManagementWithURLs provides API endpoint URL.
func ManagementWithURLs(apiDeleteBackupURL, deleteOldBackupURL, deleteUnitOfBackupURL string) ManagementOption {
	return &managementURLOption{
		APIDeleteBackupsURL:   apiDeleteBackupURL,
		DeleteOldBackupURL:    deleteOldBackupURL,
		DeleteUnitOfBackupURL: deleteUnitOfBackupURL,
	}
}

type managementQueueNameOption struct {
	QueueName string
}

func (o *managementQueueNameOption) implements(s *datastoreManagementService) {
	s.QueueName = o.QueueName
}

// ManagementWithQueueName provides queue name of TaskQueue.
func ManagementWithQueueName(queueName string) ManagementOption {
	return &managementQueueNameOption{
		QueueName: queueName,
	}
}

type managementExpireDurationOption struct {
	ExpireAfter time.Duration
}

func (o *managementExpireDurationOption) implements(s *datastoreManagementService) {
	s.ExpireAfter = o.ExpireAfter
}

// ManagementWithExpireDuration privides expire duration of backup informations.
// default expiration duration is 30 days.
func ManagementWithExpireDuration(expireAfter time.Duration) ManagementOption {
	return &managementExpireDurationOption{
		ExpireAfter: expireAfter,
	}
}

type datastoreManagementService struct {
	QueueName   string
	ExpireAfter time.Duration

	APIDeleteBackupsURL   string
	DeleteOldBackupURL    string
	DeleteUnitOfBackupURL string
}

// DatastoreManagementService serves Datastore management APIs.
type DatastoreManagementService interface {
	SetupWithUconSwagger(swPlugin *swagger.Plugin)
	HandlePostTQ(c context.Context, req *Noop) (*Noop, error)
	HandlePostDeleteList(c context.Context, r *http.Request, req *ReqListBase) (*Noop, error)
	HandleDeleteAEBackupInformation(c context.Context, r *http.Request, req *AEBackupInformationDeleteReq) (*Noop, error)
}

// NewDatastoreManagementService returns ready to use DatastoreManagementService.
func NewDatastoreManagementService(opts ...ManagementOption) DatastoreManagementService {
	s := &datastoreManagementService{
		QueueName:             "exec-rm-old-datastore-backups",
		ExpireAfter:           30 * 24 * time.Hour,
		APIDeleteBackupsURL:   "/api/datastore-management/delete-old-backups",
		DeleteOldBackupURL:    "/tq/datastore-management/delete-old-backups",
		DeleteUnitOfBackupURL: "/tq/datastore-management/delete-backup",
	}

	for _, opt := range opts {
		opt.implements(s)
	}

	return s
}

// SetupWithUconSwagger setup handlers to ucon mux.
func (s *datastoreManagementService) SetupWithUconSwagger(swPlugin *swagger.Plugin) {
	tag := swPlugin.AddTag(&swagger.Tag{Name: "DatastoreManagement", Description: ""})
	var info *swagger.HandlerInfo

	info = swagger.NewHandlerInfo(s.HandlePostTQ)
	ucon.Handle("DELETE", s.APIDeleteBackupsURL, info)
	info.Description, info.Tags = "Remove old Datastore backups", []string{tag.Name}

	ucon.HandleFunc("GET,DELETE", s.DeleteOldBackupURL, s.HandlePostDeleteList)

	ucon.HandleFunc("GET,DELETE", s.DeleteUnitOfBackupURL, s.HandleDeleteAEBackupInformation)
}

func (s *datastoreManagementService) HandlePostTQ(c context.Context, req *Noop) (*Noop, error) {
	t := &taskqueue.Task{
		Method: "DELETE",
		Path:   s.DeleteOldBackupURL,
	}
	_, err := taskqueue.Add(c, t, s.QueueName)
	if err != nil {
		return nil, err
	}
	return &Noop{}, nil
}

func (s *datastoreManagementService) HandlePostDeleteList(c context.Context, r *http.Request, req *ReqListBase) (*Noop, error) {
	err := addDeleteOldBackupTasks(c, r, req, s.QueueName, s.DeleteUnitOfBackupURL, s.ExpireAfter)
	if err != nil {
		return nil, err
	}

	return &Noop{}, nil
}

// AEBackupInformationDeleteReq provides request of delete Datastore backup.
type AEBackupInformationDeleteReq struct {
	Key string `json:"key"`
}

func (s *datastoreManagementService) HandleDeleteAEBackupInformation(c context.Context, r *http.Request, req *AEBackupInformationDeleteReq) (*Noop, error) {
	err := deleteBackup(c, r, req, s.QueueName)
	if err != nil {
		return nil, err
	}

	return &Noop{}, nil
}
