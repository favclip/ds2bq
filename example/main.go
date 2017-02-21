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
				Title:   "dstimesexample",
				Version: "1",
			},
		},
	})
	ucon.Plugin(swPlugin)

	{
		s := dstimes.NewDatastoreManagementService(
			dstimes.ManagementWithURLs(
				"/api/datastore-management/delete-old-backups",
				"/tq/datastore-management/delete-old-backups",
				"/tq/datastore-management/delete-backup",
			),
			dstimes.ManagementWithQueueName("exec-rm-old-datastore-backups"),
			dstimes.ManagementWithExpireDuration(30*24*time.Hour),
		)
		s.SetupWithUconSwagger(swPlugin)
	}
	{
		s, err := dstimes.NewGCSWatcherService(
			dstimes.GCSWatcherWithURLs(
				"/api/gcs/object-change-notification",
				"/tq/gcs/object-to-bq",
			),
			dstimes.GCSWatcherWithBackupBucketName("dstimesexample"),
			// or
			dstimes.GCSWatcherWithAfterContext(func(c context.Context) (dstimes.GCSWatcherOption, error) {
				bucketName := appengine.AppID(c) + "-datastore-backups"
				return dstimes.GCSWatcherWithBackupBucketName(bucketName), nil
			}),
			dstimes.GCSWatcherWithDatasetID("datastore_imports"),
			dstimes.GCSWatcherWithQueueName("datastore-to-bq"),
			dstimes.GCSWatcherWithTargetKindNames("Article", "User"),
			// or
			dstimes.GCSWatcherWithTargetKinds(&Article{}, &User{}),
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
