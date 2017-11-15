package ds2bq

import (
	"encoding/json"
	"io"
	"net/http"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
)

// DecodeGCSObject decodes a GCSObject from r.
func DecodeGCSObject(r io.Reader) (*GCSObject, error) {
	decoder := json.NewDecoder(r)
	var obj *GCSObject
	err := decoder.Decode(&obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// DecodeGCSObjectToBQJobReq decodes a GCSObjectToBQJobReq from r.
func DecodeGCSObjectToBQJobReq(r io.Reader) (*GCSObjectToBQJobReq, error) {
	decoder := json.NewDecoder(r)
	var req *GCSObjectToBQJobReq
	err := decoder.Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// ReceiveOCNHandleFunc returns a http.HandlerFunc that receives OCN.
// The path is for
func ReceiveOCNHandleFunc(bucketName, queueName, path string, kindNames []string) http.HandlerFunc {
	// TODO: processWithContext
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		obj, err := DecodeGCSObject(r.Body)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		if !obj.IsImportTarget(c, r, bucketName, kindNames) {
			return
		}

		err = receiveOCN(c, obj, queueName, path)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to receive OCN: %s", err)
			return
		}
	}
}

// ImportBigQueryHandleFunc returns a http.HandlerFunc that imports GCSObject to BigQuery.
func ImportBigQueryHandleFunc(datasetID string) http.HandlerFunc {
	// TODO: processWithContext
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		req, err := DecodeGCSObjectToBQJobReq(r.Body)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		err = insertImportJob(c, req, datasetID)
		if err != nil {
			log.Errorf(c, "ds2bq: failed to import BigQuery: %s", err)
			return
		}
	}
}
