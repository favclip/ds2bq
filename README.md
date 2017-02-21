# ds2bq

Import Datastore backup into BigQuery & Clean up old Datastore backup information.

## How it works?

1. Setup [Google Cloud Storage - Object Change Notification](https://cloud.google.com/storage/docs/object-change-notification).
2. Setup [Datastore Scheduled Backups](https://cloud.google.com/appengine/articles/scheduled_backups).
3. Receive webhook and import data to BigQuery when create backup by [cron](https://cloud.google.com/appengine/docs/go/config/cron).
    * appengine(backup cron) -> GCS object (send notification by webhook) -> appengine(import into bq)
4. Clean up backups on GCS files (by lifecycle) and meta data (on Datastore) by cron.

## Setup

### Coding

see [example](https://github.com/favclip/ds2bq/blob/master/example/main.go).

### Prepare

* Create Dataset at BigQuery
    * https://bigquery.cloud.google.com/dataset/
    * I'm using name `datastore_imports` usually.
* Create service account
    * https://console.cloud.google.com/iam-admin/serviceaccounts/project
    * I'm using name `gcs-objectchangenotification` usually.
    * Add `Storage Admin` role.
    * Download new key by JSON format.
* Add Domain verification
    * If you want. This operation is required when GCS OCN send to custom domain.
    * https://console.cloud.google.com/apis/credentials/domainverification

### Environment variables & account

We will use above environment variables.
You can change it with your favorite settings.

```
$ SERVICE_ACCOUNT_NAME=gcs-objectchangenotification
$ API_ID=foobar
$ BACKUP_BUCKET=foobar-datastore-backups
$ API_ENDPOINT=https://foobar.appspot.com/api/gcs/object-change-notification
$ echo ${SERVICE_ACCOUNT_NAME} ${APP_ID} ${BACKUP_BUCKET} ${API_ENDPOINT}
```

We will exec some commands in local machine.
set up gcloud command account that uses service account.

```
$ gcloud auth activate-service-account ${SERVICE_ACCOUNT_NAME}@${APP_ID}.iam.gserviceaccount.com --key-file <downloaded secret key file path>
$ gcloud auth list
```

## GCS OCN setup

https://cloud.google.com/storage/docs/object-change-notification

You MUST save the execution log.

```
$ gsutil acl ch -u ${APP_ID}@appspot.gserviceaccount.com:O gs://${BACKUP_BUCKET}
$ gsutil notification watchbucket ${API_ENDPOINT} gs://${BACKUP_BUCKET}
Watching bucket gs://foobar-datastore-backups/ with application URL https://foobar.appspot.com/api/gcs/object-change-notification ...
Successfully created watch notification channel.
Watch channel identifier: XXXXX
Canonicalized resource identifier: YYYYYY
Client state token: None
```

If you want to stop receiving, You can stop the channel.

```
$ gsutil notification stopchannel XXXXX YYYYYY
```

This parameters can't obtaine again using any command. (isn't it?)

## GCS lifecycle setup

https://cloud.google.com/storage/docs/managing-lifecycles

Set up expire duration same as `DatastoreManagementService#ExpireDuration` (go code).

```
$ cat additional-settings.json
{
  "lifecycle": {
    "rule": [
      {
        "action": {
          "type": "Delete"
        },
        "condition": {
          "age": 30
        }
      }
    ]
  }
}
$ gsutil lifecycle get gs://${BACKUP_BUCKET} > bucket-lifecycle.json
# merge JSON manually
$ gsutil lifecycle set bucket-lifecycle.json gs://${BACKUP_BUCKET}
```
