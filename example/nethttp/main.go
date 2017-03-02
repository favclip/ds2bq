package example

import (
	"net/http"
	"time"

	"github.com/drillbits/ds2bq"
)

const (
	apiDeleteBackup   = "/api/datastore-management/delete-old-backups"
	tqDeleteOldBackup = "/tq/datastore-management/delete-old-backups"
	tqDeleteBackup    = "/tq/datastore-management/delete-backup"
	apiReceiveOCN     = "/api/gcs/object-change-notification"
	tqImportBigQuery  = "/tq/gcs/object-to-bq"
)

func init() {
	// Datastore backup
	queueName := "exec-rm-old-datastore-backups"
	expireAfter := 24 * time.Hour * 30
	http.HandleFunc(apiDeleteBackup, ds2bq.DeleteOldBackupAPIHandlerFunc(queueName, tqDeleteOldBackup))
	http.HandleFunc(tqDeleteOldBackup, ds2bq.DeleteOldBackupTaskHandlerFunc(queueName, tqDeleteBackup, expireAfter))
	http.HandleFunc(tqDeleteBackup, ds2bq.DeleteBackupTaskHandlerFunc(queueName))

	// GCS to BigQuery
	queueName = "datastore-to-bq"
	bucketName := "ds2bqexample-nethttp"
	datasetID := "datastore_imports"
	targetKinds := []string{"Article", "User"}
	http.HandleFunc(apiReceiveOCN, ds2bq.ReceiveOCNHandleFunc(bucketName, queueName, tqImportBigQuery, targetKinds)) // from GCS, This API must not requires admin role.
	http.HandleFunc(tqImportBigQuery, ds2bq.ImportBigQueryHandleFunc(datasetID))
}
