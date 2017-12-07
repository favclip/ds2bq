package ds2bq

import (
	"context"
	"time"

	"google.golang.org/appengine"

	"golang.org/x/oauth2/google"

	"github.com/pkg/errors"

	datastore "google.golang.org/api/datastore/v1beta1"
)

// EntityFilter is Entity condition to export
type EntityFilter struct {
	Kinds           []string `json:"kinds,omitempty"`
	NamespaceIds    []string `json:"namespaceIds,omitempty"`
	ForceSendFields []string `json:"-"`
	NullFields      []string `json:"-"`
}

// DatastoreExportService serves DatastoreExport API Function.
type DatastoreExportService interface {
	Export(c context.Context, outputGCSPrefix string, entityFilter *EntityFilter) (*datastore.GoogleLongrunningOperation, error)
}

// NewDatastoreExportService returns ready to use DatastoreExportService
func NewDatastoreExportService() DatastoreExportService {
	return &datastoreExportService{}
}

type datastoreExportService struct{}

func (s *datastoreExportService) Export(c context.Context, outputGCSPrefix string, entityFilter *EntityFilter) (*datastore.GoogleLongrunningOperation, error) {
	ctxWithDeadline, cancel := context.WithTimeout(c, 9*time.Minute)
	defer cancel()
	client, err := google.DefaultClient(ctxWithDeadline, datastore.DatastoreScope)
	if err != nil {
		return nil, errors.Wrap(err, "failed google.DefaultClient")
	}

	service, err := datastore.New(client)
	if err != nil {
		return nil, errors.Wrap(err, "failed datastore.New")
	}

	ef := &datastore.GoogleDatastoreAdminV1beta1EntityFilter{
		Kinds:           entityFilter.Kinds,
		NamespaceIds:    entityFilter.NamespaceIds,
		ForceSendFields: entityFilter.ForceSendFields,
		NullFields:      entityFilter.NullFields,
	}

	p := appengine.AppID(c)
	op, err := service.Projects.Export(p, &datastore.GoogleDatastoreAdminV1beta1ExportEntitiesRequest{
		EntityFilter:    ef,
		OutputUrlPrefix: outputGCSPrefix,
	}).Do()
	if err != nil {
		return nil, errors.Wrap(err, "datastore.Projects.Export")
	}

	return op, nil
}
