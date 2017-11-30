package ds2bq

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mjibson/goon"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

func addDeleteOldBackupTasks(c context.Context, r *http.Request, req *ReqListBase, queueName, deleteBackupURL string, expireAfter time.Duration) error {
	if expireAfter <= 0 {
		// to do nothing
		return nil
	}

	store := &AEDatastoreStore{}
	list, listRespBase, err := store.ListAEBackupInformation(c, req)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		return nil
	}

	g := goon.FromContext(c)
	expireThreshold := time.Now().Add(-1 * expireAfter)
	for _, backupInfo := range list {
		if backupInfo.CompleteTime.Before(expireThreshold) {
			key := g.Key(backupInfo)
			log.Infof(c, "ds2bq: %s should be removed, %#v", key.String(), backupInfo)

			u, err := url.Parse(deleteBackupURL)
			if err != nil {
				return err
			}
			vs := url.Values{}
			vs.Add("key", key.Encode())
			u.RawQuery = vs.Encode()
			t := &taskqueue.Task{
				Method: "DELETE",
				Path:   u.String(),
			}
			_, err = taskqueue.Add(c, t, queueName)
			if err != nil {
				return err
			}
		}
	}

	if listRespBase.Cursor != "" {
		u := r.URL
		vs := url.Values{}
		vs.Add("limit", strconv.Itoa(req.Limit))
		vs.Add("offset", strconv.Itoa(req.Offset))
		vs.Add("cursor", listRespBase.Cursor)
		u.RawQuery = vs.Encode()
		t := &taskqueue.Task{
			Method: "DELETE",
			Path:   u.String(),
		}
		_, err = taskqueue.Add(c, t, queueName)
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteBackup(c context.Context, r *http.Request, req *AEBackupInformationDeleteReq, queueName string) error {
	if !isInTaskqueue(r, queueName) {
		_, err := delegateToTaskqueue(r, queueName)
		if err != nil {
			return err
		}
		log.Infof(c, "ds2bq: this request was delegated to taskqueue")
		return err
	}

	key, err := datastore.DecodeKey(req.Key)
	if err != nil {
		return err
	}

	store := &AEDatastoreStore{}
	return store.DeleteAEBackupInformationAndRelatedData(c, key)
}
