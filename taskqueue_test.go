package ds2bq

import (
	"bytes"
	"testing"

	"github.com/favclip/testerator"
)

func TestIsInTaskqueue(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err)
	}
	defer testerator.SpinDown()

	for i, tc := range []struct {
		noHeader        bool
		headerQueuename string
		queueName       string
		exp             bool
	}{
		{true, "", "", true},
		{true, "", "test-queueName", false},
		{false, "test-queueName", "test-queueName", true},
	} {
		i, tc := i, tc
		// TODO: Go 1.6 does not provide subtests
		// t.Run("", func(t *testing.T) {
		r, err := inst.NewRequest("GET", "", nil)
		if err != nil {
			t.Fatalf("%02d: %s", i, err)
		}
		if !tc.noHeader {
			r.Header.Set("X-AppEngine-QueueName", tc.headerQueuename)
		}

		got := isInTaskqueue(r, tc.queueName)
		if got != tc.exp {
			t.Errorf("%02d: isInTaskqueue(%#v, %s)\n => %t, want %t", i, r, tc.queueName, got, tc.exp)
		}
		// })
	}
}

func TestDelegateToTaskqueue(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err)
	}
	defer testerator.SpinDown()

	for i, tc := range []struct {
		method     string
		path       string
		body       []byte
		expMethod  string
		expPath    string
		expPayload []byte
	}{
		{"GET", "/test/get?foo=bar&baz", nil, "GET", "/test/get?foo=bar&baz", nil},
		{"GET", "/test/get?foo=bar&baz", []byte("qux=quux"), "GET", "/test/get?foo=bar&baz", nil},
		{"POST", "/test/post?qux", []byte("foo=bar&baz"), "POST", "/test/post", []byte("baz=&foo=bar&qux=")},
		{"PUT", "/test/put?qux", []byte("foo=bar&baz"), "PUT", "/test/put", []byte("baz=&foo=bar&qux=")},
		{"DELETE", "/test/delete?foo=bar&baz", nil, "DELETE", "/test/delete?foo=bar&baz", nil},
		{"DELETE", "/test/delete?foo=bar&baz", []byte("qux=quux"), "DELETE", "/test/delete?foo=bar&baz", nil},
	} {
		i, tc := i, tc
		// TODO: Go 1.6 does not provide subtests
		// t.Run("", func(t *testing.T) {
		r, err := inst.NewRequest(tc.method, tc.path, bytes.NewReader(tc.body))
		if err != nil {
			t.Fatalf("%02d: %s", i, err)
		}
		if tc.method == "POST" || tc.method == "PUT" {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}

		task, err := delegateToTaskqueue(r, "default") // workaround for UNKNOWN_QUEUE in unittest
		if err != nil {
			t.Fatalf("%02d: %s", i, err)
		}
		if task.Method != tc.expMethod {
			t.Errorf("%02d: delegateToTaskqueue(%#v, \"default\")\n => task.Method %s, want %s", i, r, task.Method, tc.expMethod)
		}
		if task.Path != tc.expPath {
			t.Errorf("%02d: delegateToTaskqueue(%#v, \"default\")\n => task.Path %s, want %s", i, r, task.Path, tc.expPath)
		}
		if !bytes.Equal(task.Payload, tc.expPayload) {
			t.Errorf("%02d: delegateToTaskqueue(%#v, \"default\")\n => task.Payload %s, want %s", i, r, task.Payload, tc.expPayload)
		}
		// })
	}
}
