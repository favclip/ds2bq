package ds2bq

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// DecodeReqListBase decodes a ReqListBase from r.
func DecodeReqListBase(r io.Reader) (*ReqListBase, error) {
	decoder := json.NewDecoder(r)
	var req *ReqListBase
	err := decoder.Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// DecodeAEBackupInformationDeleteReq decodes a AEBackupInformationDeleteReq from r.
func DecodeAEBackupInformationDeleteReq(r io.Reader) (*AEBackupInformationDeleteReq, error) {
	decoder := json.NewDecoder(r)
	var req *AEBackupInformationDeleteReq
	err := decoder.Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// DeleteOldBackupAPIHandlerFunc returns a http.HandlerFunc that delegate to taskqueue.
// The path is for DeleteOldBackupTask.
func DeleteOldBackupAPIHandlerFunc(queueName, path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		task := &taskqueue.Task{
			Method: "DELETE",
			Path:   path,
		}
		_, err := taskqueue.Add(c, task, queueName)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to add a task: %s", err)
			return
		}
	}
}

// DeleteOldBackupTaskHandlerFunc returns a http.HandlerFunc that adds tasks to delete old AEBackupInformation.
// The path is for DeleteBackupTask.
func DeleteOldBackupTaskHandlerFunc(queueName, path string, expireAfter time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		req, err := DecodeReqListBase(r.Body)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		err = addDeleteOldBackupTasks(c, r, req, queueName, path, expireAfter)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to delete old backup: %s", err)
			return
		}
	}
}

// DeleteBackupTaskHandlerFunc returns a http.HandlerFunc that removes all child entities about AEBackupInformation or AEDatastoreAdminOperation kinds.
func DeleteBackupTaskHandlerFunc(queueName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		req, err := DecodeAEBackupInformationDeleteReq(r.Body)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		err = deleteBackup(c, r, req, queueName)
		if err != nil {
			log.Warningf(c, "ds2bq: failed to delete appengine backup information: %s", err)
			return
		}
	}
}
