package ds2bq

import (
	"net/http"

	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
)

func isInTaskqueue(r *http.Request, queueName string) bool {
	return r.Header.Get("X-AppEngine-QueueName") == queueName
}

func delegateToTaskqueue(r *http.Request, queueName string) (*taskqueue.Task, error) {
	var t *taskqueue.Task
	switch r.Method {
	case "POST", "PUT":
		err := r.ParseForm()
		if err != nil {
			return nil, err
		}
		t = taskqueue.NewPOSTTask(r.URL.Path, r.Form)
		// PUT
		t.Method = r.Method
	default:
		t = &taskqueue.Task{
			Method: r.Method,
			Path:   r.URL.String(),
		}
	}

	c := appengine.NewContext(r)
	t, err := taskqueue.Add(c, t, queueName)
	if err != nil {
		return nil, err
	}
	return t, nil
}
