package example

import (
	"net/http"
	"time"

	"github.com/favclip/ds2bq"
	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
)

func UseAppengineContext(b *ucon.Bubble) error {
	b.Context = appengine.NewContext(b.R)
	return b.Next()
}

func init() {
	ucon.Middleware(UseAppengineContext)
	ucon.Orthodox()

	swPlugin := swagger.NewPlugin(&swagger.Options{
		Object: &swagger.Object{
			Info: &swagger.Info{
				Title:   "ds2bqexample",
				Version: "1",
			},
		},
	})
	ucon.Plugin(swPlugin)

	{
		s := ds2bq.NewDatastoreManagementService(
			ds2bq.ManagementWithURLs(
				"/api/datastore-management/delete-old-backups",
				"/tq/datastore-management/delete-old-backups",
				"/tq/datastore-management/delete-backup",
			),
			ds2bq.ManagementWithQueueName("exec-rm-old-datastore-backups"),
			ds2bq.ManagementWithExpireDuration(30*24*time.Hour),
		)
		s.SetupWithUconSwagger(swPlugin)
	}
	{
		s, err := ds2bq.NewGCSWatcherService(
			ds2bq.GCSWatcherWithURLs(
				"/api/gcs/object-change-notification",
				"/tq/gcs/object-to-bq",
			),
			ds2bq.GCSWatcherWithBackupBucketName("ds2bqexample-ucon"),
			// or
			ds2bq.GCSWatcherWithAfterContext(func(c context.Context) (ds2bq.GCSWatcherOption, error) {
				bucketName := appengine.AppID(c) + "-datastore-backups"
				return ds2bq.GCSWatcherWithBackupBucketName(bucketName), nil
			}),
			ds2bq.GCSWatcherWithDatasetID("datastore_imports"),
			ds2bq.GCSWatcherWithQueueName("datastore-to-bq"),
			ds2bq.GCSWatcherWithTargetKindNames("Article", "User"),
			// or
			ds2bq.GCSWatcherWithTargetKinds(&Article{}, &User{}),
		)
		if err != nil {
			panic(err)
		}
		s.SetupWithUcon()
	}

	ucon.DefaultMux.Prepare()
	http.Handle("/", ucon.DefaultMux)
}

type Article struct{}
type User struct{}
