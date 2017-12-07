package ds2bq

import (
	"context"

	"golang.org/x/oauth2/google"
	dsapi "google.golang.org/api/datastore/v1beta1"
	"google.golang.org/appengine"
)

// https://cloud.google.com/datastore/docs/export-import-entities

// EntityFilter is Entity condition to export
type EntityFilter struct {
	Kinds           []string `json:"kinds,omitempty"`
	NamespaceIds    []string `json:"namespaceIds,omitempty"`
	ForceSendFields []string `json:"-"`
	NullFields      []string `json:"-"`
}

// DatastoreExportService serves DatastoreExport API Function.
type DatastoreExportService interface {
	Export(c context.Context, outputGCSPrefix string, entityFilter *EntityFilter) (*dsapi.GoogleLongrunningOperation, error)
}

// NewDatastoreExportService returns ready to use DatastoreExportService
func NewDatastoreExportService() DatastoreExportService {
	return &datastoreExportService{}
}

type datastoreExportService struct{}

func (s *datastoreExportService) Export(c context.Context, outputGCSPrefix string, entityFilter *EntityFilter) (*dsapi.GoogleLongrunningOperation, error) {
	client, err := google.DefaultClient(c, dsapi.DatastoreScope)
	if err != nil {
		return nil, err
	}

	service, err := dsapi.New(client)
	if err != nil {
		return nil, err
	}

	eCall := service.Projects.Export(appengine.AppID(c), &dsapi.GoogleDatastoreAdminV1beta1ExportEntitiesRequest{
		EntityFilter: &dsapi.GoogleDatastoreAdminV1beta1EntityFilter{
			Kinds:           entityFilter.Kinds,
			NamespaceIds:    entityFilter.NamespaceIds,
			ForceSendFields: entityFilter.ForceSendFields,
			NullFields:      entityFilter.NullFields,
		},
		OutputUrlPrefix: outputGCSPrefix,
	})
	return eCall.Do()
}
