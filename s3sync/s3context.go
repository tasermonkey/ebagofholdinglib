package s3sync

import (
	"github.com/crowdmob/goamz/s3"
	"github.com/tasermonkey/goenv"
	"time"
)

var (
	myenv         = goenv.DefaultGoenv()
	DefaultAuth   = myenv.GetAwsAuth()
	DefaultRegion = myenv.GetAwsRegion()
	ListBatchSize = myenv.GetListBatchSize()
	DefaultBucket = myenv.GetAwsBucketName()
	DefaultDBPath = myenv.GetSyncDbFilename()
)

type S3Context struct {
	s3.S3
	SyncBucketName string
	SyncBucket     *s3.Bucket
	BasePath       string
}

type LocalInfo struct {
	LocalPath string
	DbPath    string
}

type S3Sync struct {
	S3Context
	LocalInfo
}

func MakeS3Sync() S3Sync {
	var result S3Sync
	result.setDefaults()
	return result
}

func (this *S3Sync) setDefaults() {
	if isAwsAuthEmpty(this.Auth) {
		this.Auth = DefaultAuth
	}
	if isAwsRegionEmpty(this.Region) {
		this.Region = DefaultRegion
	}
	if this.SyncBucket != nil {
		this.SyncBucketName = this.SyncBucket.Name
	} else {
		if this.SyncBucketName == "" {
			this.SyncBucketName = DefaultBucket
		}
		if this.SyncBucketName != "" {
			this.SyncBucket = this.Bucket(this.SyncBucketName)
		}
	}
}

func (this *S3Sync) makeDownloadEvent(key s3.Key) DownloadEvent {
	return DownloadEvent{key, this.getFullLocalPath(key.Key), time.Now()}
}
