package ds2bq

import (
	"testing"
)

func TestGCSObject_ExtractKindName(t *testing.T) {
	{
		o := GCSObject{
			Name: "agtzfnN0Zy1jaGFvc3JACxIcX0FFX0RhdGFzdG9yZUFkbWluX09wZXJhdGlvbhjx52oMCxIWX0FFX0JhY2t1cF9JbmZvcm1hdGlvbhgBDA.Article.backup_info",
		}
		kind := o.ExtractKindName()
		if e, g := "Article", kind; e != g {
			t.Fatalf("expected kind %s; got %s", e, g)
		}
	}
	{
		o := GCSObject{
			Name: "2017-11-14T06:47:01_23208/all_namespaces/kind_Item/all_namespaces_kind_Item.export_metadata",
		}
		kind := o.ExtractKindName()
		if e, g := "Item", kind; e != g {
			t.Fatalf("expected kind %s; got %s", e, g)
		}
	}
}

func TestGCSObject_extractKindNameForDatastoreAdmin(t *testing.T) {
	o := GCSObject{}
	kind := o.extractKindNameForDatastoreAdmin("agtzfnN0Zy1jaGFvc3JACxIcX0FFX0RhdGFzdG9yZUFkbWluX09wZXJhdGlvbhjx52oMCxIWX0FFX0JhY2t1cF9JbmZvcm1hdGlvbhgBDA.Article.backup_info")
	if e, g := "Article", kind; e != g {
		t.Fatalf("expected kind %s; got %s", e, g)
	}
}

func TestGCSObject_extractKindNameForDatastoreExport(t *testing.T) {
	o := GCSObject{}

	tests := []struct {
		input string
		want  string
	}{
		{input: "2017-11-14T06:47:01_23208/all_namespaces/kind_Item/all_namespaces_kind_Item.export_metadata", want: "Item"},
		{input: "2017-11-14T06:47:01_23208/2017-11-30T08:22:02_14720.overall_export_metadata", want: ""},
		{input: "2017-11-14T06:47:01_23208/all_namespaces/kind_Item/output-95", want: ""},
	}

	for _, test := range tests {
		if e, g := test.want, o.extractKindNameForDatastoreExport(test.input); e != g {
			t.Fatalf("expected kind %s; got %s", e, g)
		}
	}
}
