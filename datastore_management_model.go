package ds2bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mjibson/goon"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

// AEDatastoreStore provides methods of Datastore backup information handling.
type AEDatastoreStore struct{}

// AEDatastoreAdminOperation mapped to _AE_DatastoreAdmin_Operation kind.
// +qbg
type AEDatastoreAdminOperation struct {
	Kind          string    `goon:"kind,_AE_DatastoreAdmin_Operation" json:"-"`
	ID            int64     `datastore:"-" goon:"id"`
	ActiveJobIDs  []string  `datastore:"active_job_ids"`
	ActiveJobs    int       `datastore:"active_jobs"`
	CompletedJobs int       `datastore:"completed_jobs"`
	Description   string    `datastore:"description"`
	LastUpdated   time.Time `datastore:"last_updated"`
	ServiceJobID  string    `datastore:"service_job_id"` // 型が不明だったので仮置き
	Status        string    `datastore:"status"`
	StatusInfo    string    `datastore:"status_info"`

	AEBackupInformationList []*AEBackupInformation `datastore:"-"`
	AEBackupKindList        []*AEBackupKind        `datastore:"-"`
}

// AEBackupInformation mapped to _AE_Backup_Information kind.
// +qbg
type AEBackupInformation struct {
	Kind string `goon:"kind,_AE_Backup_Information" json:"-"`
	// This kind does not have ParentKey rarely. maybe it comes from restore with backup of other app created.
	ParentKey     *datastore.Key `json:"-" datastore:"-" goon:"parent"` // AEDatastoreAdminOperation
	ID            int64          `datastore:"-" goon:"id"`
	ActiveJobs    []string       `datastore:"active_jobs"`
	CompleteTime  time.Time      `datastore:"complete_time"`
	CompletedJobs []string       `datastore:"completed_jobs"`
	Destination   string         `datastore:"destination"` // This field is string type maybe.
	Filesystem    string         `datastore:"filesystem"`
	GSHandle      string         `datastore:"gs_handle"`
	Kinds         []string       `datastore:"kinds"`
	Name          string         `datastore:"name"`
	OriginalApp   string         `datastore:"original_app"`
	StartTime     time.Time      `datastore:"start_time"`

	AEBackupInformationKindFilesList []*AEBackupInformationKindFiles `datastore:"-"`
}

// AEBackupInformationKindFiles mapped to _AE_Backup_Information_Kind_Files kind.
// +qbg
type AEBackupInformationKindFiles struct {
	Kind      string         `goon:"kind,_AE_Backup_Information_Kind_Files" json:"-"`
	ParentKey *datastore.Key `json:"-" datastore:"-" goon:"parent"` // AEBackupInformation
	ID        string         `datastore:"-" goon:"id"`
	Files     []string       `datastore:"files"`
}

// AEBackupKind mapped to _AE_DatastoreAdmin_Operation/_AE_Backup_Information/Kind kind.
// +qbg
type AEBackupKind struct {
	Kind      string         `goon:"kind,Kind" json:"-"`
	ParentKey *datastore.Key `json:"-" datastore:"-" goon:"parent"` // AEBackupInformation
	ID        string         `datastore:"-" goon:"id"`
	// Field無しのKeyだけの存在

	AEBackupInformationKindTypeInfoList []*AEBackupInformationKindTypeInfo `datastore:"-"`
}

// AEBackupInformationKindTypeInfo mapped to _AE_Backup_Information_Kind_Type_Info kind.
// +qbg
type AEBackupInformationKindTypeInfo struct {
	Kind               string                  `goon:"kind,_AE_Backup_Information_Kind_Type_Info" json:"-"`
	ParentKey          *datastore.Key          `json:"-" datastore:"-" goon:"parent"` // AEBackupKind
	ID                 string                  `datastore:"-" goon:"id"`
	EntityTypeInfo     string                  `datastore:"entity_type_info" json:"-"`
	EntityTypeInfoJSON *AEBackupEntityTypeInfo `datastore:"-" json:"entityTypeInfo"`
	IsPartial          bool                    `datastore:"is_partial"`
}

// AEBackupEntityTypeInfo mapped to field of AEBackupInformationKindTypeInfo type.
type AEBackupEntityTypeInfo struct {
	Kind       string                            `json:"kind"`
	Properties []*AEBackupEntityTypeInfoProperty `json:"properties"`
}

// AEBackupEntityTypeInfoProperty mapped to field of AEBackupInformationKindTypeInfo type.
type AEBackupEntityTypeInfoProperty struct {
	EmbeddedEntities []string `json:"embedded_entities"` // 型が不明だったので仮置き
	IsRepeated       bool     `json:"is_repeated"`
	Name             string   `json:"name"`
	PrimitiveTypes   []int    `json:"primitive_types"`
}

// GetAEDatastoreAdminOperation returns AEDatastoreAdminOperation that specified by id.
func (store *AEDatastoreStore) GetAEDatastoreAdminOperation(c context.Context, id int64) (*AEDatastoreAdminOperation, error) {
	if id == 0 {
		return nil, ErrInvalidID
	}

	g := goon.FromContext(c)

	entity := &AEDatastoreAdminOperation{ID: id}
	err := g.Get(entity)
	if err != nil {
		log.Infof(c, "on Get AEDatastoreAdminOperation: %s", err.Error())
		return nil, err
	}
	err = entity.FetchChildren(c)
	if err != nil {
		log.Infof(c, "on AEDatastoreAdminOperation.FetchChildren: %s", err.Error())
		return nil, err
	}

	return entity, nil
}

// ListAEDatastoreAdminOperation return list of AEDatastoreAdminOperation.
func (store *AEDatastoreStore) ListAEDatastoreAdminOperation(c context.Context, req *ReqListBase) ([]*AEDatastoreAdminOperation, *RespListBase, error) {
	if req.Limit == 0 {
		req.Limit = 10
	}

	qb := newAEDatastoreAdminOperationQueryBuilder()
	qb.ID.Asc()
	q := qb.Query()
	ldr := &AEDatastoreAdminOperationListLoader{
		List:     make([]*AEDatastoreAdminOperation, 0, req.Limit),
		Req:      *req,
		RespList: &RespListBase{},
	}
	err := ExecQuery(c, q, ldr)
	if err != nil {
		return nil, nil, err
	}

	return ldr.List, ldr.RespListBase(), nil
}

// GetAEBackupInformation returns AEBackupInformation that specified id.
func (store *AEDatastoreStore) GetAEBackupInformation(c context.Context, parentKey *datastore.Key, id int64) (*AEBackupInformation, error) {
	if id == 0 {
		return nil, ErrInvalidID
	}

	g := goon.FromContext(c)

	entity := &AEBackupInformation{ParentKey: parentKey, ID: id}
	err := g.Get(entity)
	if err != nil {
		log.Infof(c, "on Get AEBackupInformation: %s", err.Error())
		return nil, err
	}
	err = entity.FetchChildren(c)
	if err != nil {
		log.Infof(c, "on AEBackupInformation.FetchChildren: %s", err.Error())
		return nil, err
	}

	return entity, nil
}

// ListAEBackupInformation return list of AEBackupInformation.
func (store *AEDatastoreStore) ListAEBackupInformation(c context.Context, req *ReqListBase) ([]*AEBackupInformation, *RespListBase, error) {
	if req.Limit == 0 {
		req.Limit = 10
	}

	qb := newAEBackupInformationQueryBuilder()
	qb.ID.Asc()
	q := qb.Query()
	ldr := &AEBackupInformationListLoader{
		List:     make([]*AEBackupInformation, 0, req.Limit),
		Req:      *req,
		RespList: &RespListBase{},
	}
	err := ExecQuery(c, q, ldr)
	if err != nil {
		return nil, nil, err
	}

	return ldr.List, ldr.RespListBase(), nil
}

// DeleteAEBackupInformationAndRelatedData removes all child entities about AEBackupInformation or AEDatastoreAdminOperation kinds.
func (store *AEDatastoreStore) DeleteAEBackupInformationAndRelatedData(c context.Context, key *datastore.Key) error {
	g := goon.FromContext(c)

	if key.Kind() != "_AE_Backup_Information" {
		return fmt.Errorf("invalid kind: %s", key.Kind())
	}

	rootKey := key
	if key.Parent() != nil {
		rootKey = key.Parent()
	}

	log.Infof(c, "rootKey: %s", rootKey.String())

	q := datastore.NewQuery("").Ancestor(rootKey).KeysOnly()
	keys, err := g.GetAll(q, nil)
	if err != nil {
		return err
	}

	for _, key := range keys {
		log.Infof(c, "remove target key: %s", key.String())
	}

	err = g.DeleteMulti(keys)
	if err != nil {
		return err
	}

	return nil
}

// FetchChildren gathering children and fills fields.
func (entity *AEDatastoreAdminOperation) FetchChildren(c context.Context) error {
	g := goon.FromContext(c)

	// AEBackupInformationList と AEBackupKindList の処理
	{
		var backupInfoList []*AEBackupInformation
		qb := newAEBackupInformationQueryBuilder().Ancestor(g.Key(entity))
		_, err := g.GetAll(qb.Query(), &backupInfoList)
		if err != nil {
			log.Infof(c, "on AEBackupInformation#GetAll: %s", err.Error())
			return err
		}
		for _, backupInfo := range backupInfoList {
			if backupInfo.ParentKey == nil || backupInfo.ParentKey.Incomplete() {
				// TODO うまく処理できるようになおす
				log.Infof(c, "on backupInfo.ParentKey == nil, %#v, %#v", entity, backupInfo)
				continue
			}
			err := backupInfo.FetchChildren(c)
			if err != nil {
				log.Infof(c, "on AEBackupInformation#FetchChildren: %s", err.Error())
				return err
			}
		}
		entity.AEBackupInformationList = backupInfoList
	}
	{
		var backupKindList []*AEBackupKind
		// AEBackupKind はDatastore上にEntityが存在しない
		// AEBackupInformationに存在するものが存在するのでは？というのも仮定
		for _, backupInfo := range entity.AEBackupInformationList {
			for _, kind := range backupInfo.Kinds {
				backupKind := &AEBackupKind{
					ParentKey: g.Key(backupInfo),
					ID:        kind,
				}
				err := backupKind.FetchChildren(c)
				if err != nil {
					return err
				}
				backupKindList = append(backupKindList, backupKind)
			}
		}
		entity.AEBackupKindList = backupKindList
	}

	return nil
}

// FetchChildren gathering children and fills fields.
func (entity *AEBackupInformation) FetchChildren(c context.Context) error {
	g := goon.FromContext(c)

	// AEBackupInformationKindFilesList の処理
	{
		var backupInfoKindFilesList []*AEBackupInformationKindFiles
		qb := newAEBackupInformationKindFilesQueryBuilder().Ancestor(g.Key(entity))
		_, err := g.GetAll(qb.Query(), &backupInfoKindFilesList)
		if err != nil {
			log.Infof(c, "on AEBackupInformationKindFiles#GetAll: %s", err.Error())
			return err
		}
		for _, backupInfoKindFiles := range backupInfoKindFilesList {
			err := backupInfoKindFiles.FetchChildren(c)
			if err != nil {
				log.Infof(c, "on AEBackupInformationKindFiles#FetchChildren: %s", err.Error())
				return err
			}
		}
		entity.AEBackupInformationKindFilesList = backupInfoKindFilesList
	}

	return nil
}

// FetchChildren gathering children and fills fields.
func (entity *AEBackupKind) FetchChildren(c context.Context) error {
	g := goon.FromContext(c)

	// AEBackupInformationKindTypeInfoList の処理
	{
		var backupInfoKindTypesList []*AEBackupInformationKindTypeInfo
		qb := newAEBackupInformationKindTypeInfoQueryBuilder().Ancestor(g.Key(entity))
		_, err := g.GetAll(qb.Query(), &backupInfoKindTypesList)
		if err != nil {
			return err
		}
		for _, backupInfoKindTypes := range backupInfoKindTypesList {
			err := backupInfoKindTypes.FetchChildren(c)
			if err != nil {
				return err
			}
		}
		entity.AEBackupInformationKindTypeInfoList = backupInfoKindTypesList
	}

	return nil
}

// FetchChildren gathering children and fills fields.
func (entity *AEBackupInformationKindFiles) FetchChildren(c context.Context) error {
	// 現時点でとくに処理なし
	return nil
}

// FetchChildren gathering children and fills fields.
func (entity *AEBackupInformationKindTypeInfo) FetchChildren(c context.Context) error {
	entityTypeInfo := &AEBackupEntityTypeInfo{}
	err := json.Unmarshal([]byte(entity.EntityTypeInfo), entityTypeInfo)
	if err != nil {
		g := goon.FromContext(c)
		key := g.Key(entity)
		log.Infof(c, "on AEBackupInformationKindTypeInfo#FetchChildren json.Unmarshal, key: %s, %s", key.String(), entity.EntityTypeInfo)
		return err
	}
	entity.EntityTypeInfoJSON = entityTypeInfo

	return nil
}

// AEDatastoreAdminOperationListLoader implements QueryListLoader.
type AEDatastoreAdminOperationListLoader struct {
	List     []*AEDatastoreAdminOperation
	cursor   datastore.Cursor
	Req      ReqListBase
	RespList *RespListBase
}

// LoadInstance from Datastore.
func (ldr *AEDatastoreAdminOperationListLoader) LoadInstance(c context.Context, key *datastore.Key) (interface{}, error) {
	store := &AEDatastoreStore{}
	entity, err := store.GetAEDatastoreAdminOperation(c, key.IntID())
	if err != nil {
		return nil, err
	}
	return entity, nil
}

// Append instance to internal list.
func (ldr *AEDatastoreAdminOperationListLoader) Append(v interface{}) error {
	if entity, ok := v.(*AEDatastoreAdminOperation); ok {
		ldr.List = append(ldr.List, entity)
	} else {
		return fmt.Errorf("v is not *AEDatastoreAdminOperation, actual: %#v", v)
	}

	return nil
}

// PostProcess internal list.
func (ldr *AEDatastoreAdminOperationListLoader) PostProcess(c context.Context) error {
	for _, entity := range ldr.List {
		if err := entity.FetchChildren(c); err != nil {
			return err
		}
	}
	return nil
}

// ReqListBase returns internal stored ReqListBase.
func (ldr *AEDatastoreAdminOperationListLoader) ReqListBase() ReqListBase {
	return ldr.Req
}

// RespListBase returns internal stored *RespListBase.
func (ldr *AEDatastoreAdminOperationListLoader) RespListBase() *RespListBase {
	return ldr.RespList
}

// AEBackupInformationListLoader implements QueryListLoader.
type AEBackupInformationListLoader struct {
	List     []*AEBackupInformation
	cursor   datastore.Cursor
	Req      ReqListBase
	RespList *RespListBase
}

// LoadInstance from Datastore.
func (ldr *AEBackupInformationListLoader) LoadInstance(c context.Context, key *datastore.Key) (interface{}, error) {
	store := &AEDatastoreStore{}
	entity, err := store.GetAEBackupInformation(c, key.Parent(), key.IntID())
	if err != nil {
		return nil, err
	}
	return entity, nil
}

// Append instance to internal list.
func (ldr *AEBackupInformationListLoader) Append(v interface{}) error {
	if entity, ok := v.(*AEBackupInformation); ok {
		ldr.List = append(ldr.List, entity)
	} else {
		return fmt.Errorf("v is not *AEBackupInformation, actual: %#v", v)
	}

	return nil
}

// PostProcess internal list.
func (ldr *AEBackupInformationListLoader) PostProcess(c context.Context) error {
	for _, entity := range ldr.List {
		if err := entity.FetchChildren(c); err != nil {
			return err
		}
	}
	return nil
}

// ReqListBase returns internal stored ReqListBase.
func (ldr *AEBackupInformationListLoader) ReqListBase() ReqListBase {
	return ldr.Req
}

// RespListBase returns internal stored *RespListBase.
func (ldr *AEBackupInformationListLoader) RespListBase() *RespListBase {
	return ldr.RespList
}
