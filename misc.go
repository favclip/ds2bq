//go:generate qbg -output model_query.go -private -inlineinterfaces .

package ds2bq

import (
	"context"
	"errors"

	"github.com/mjibson/goon"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

// ErrInvalidID is message of Invalid ID error.
var ErrInvalidID = errors.New("invalid id")

// ErrInvalidState is message of Invalid State error.
var ErrInvalidState = errors.New("invalid state")

// Noop is Noop.
type Noop struct {
}

// ReqListBase means request of query.
type ReqListBase struct {
	Limit  int    `json:"limit" endpoints:"d=10" swagger:",in=query,d=10"`
	Offset int    `json:"offset" swagger:",in=query"`
	Cursor string `json:"cursor" swagger:",in=query"`
}

// RespListBase means response of query.
type RespListBase struct {
	Cursor string `json:"cursor,omitempty" swagger:",in=query"`
}

// QueryListLoader hosted entity list construction.
type QueryListLoader interface {
	LoadInstance(c context.Context, key *datastore.Key) (interface{}, error)
	Append(v interface{}) error
	PostProcess(c context.Context) error
	ReqListBase() ReqListBase
	RespListBase() *RespListBase
}

// ExecQuery with QueryListLoader.
func ExecQuery(c context.Context, q *datastore.Query, ldr QueryListLoader) error {
	g := goon.FromContext(c)

	req := ldr.ReqListBase()

	if req.Limit == 0 {
		req.Limit = 10
	}
	if req.Limit != -1 {
		q = q.Limit(req.Limit + 1) // get 1 more, fill blank to cursor when next one is not exists.
	}

	if req.Offset > 0 {
		q = q.Offset(req.Offset)
	}

	if req.Cursor != "" {
		cursor, err := datastore.DecodeCursor(req.Cursor)
		if err != nil {
			return err
		}
		q = q.Start(cursor)
	}

	q = q.KeysOnly()

	log.Debugf(c, "%#v", q)

	t := g.Run(q)

	count := 0
	hasNext := false
	var cursor datastore.Cursor
	for {
		key, err := t.Next(nil)
		if err == datastore.Done {
			break
		}
		if err != nil {
			return err
		}
		count++
		if req.Limit != -1 && req.Limit < count {
			// +1
			hasNext = true
			break
		}
		inst, err := ldr.LoadInstance(c, key)
		if err != nil {
			return err
		}
		err = ldr.Append(inst)
		if err != nil {
			return err
		}
		if req.Limit == count {
			// store cursor at reach to limit.
			cursor, err = t.Cursor()
			if err != nil {
				return err
			}
		}
	}

	err := ldr.PostProcess(c)
	if err != nil {
		return err
	}

	resp := ldr.RespListBase()

	if hasNext {
		resp.Cursor = cursor.String()
	}

	return nil
}
