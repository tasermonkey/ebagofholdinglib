package s3sync

import (
	"errors"
	"github.com/crowdmob/goamz/s3"
	"log"
)

func (this S3Sync) ReadListing() <-chan DownloadEvent {
	startEvents := make(chan DownloadEvent)
	go func() {
		defer close(startEvents)
		var remote_listing *s3.ListResp = new(s3.ListResp)
		remote_listing.IsTruncated = true
		var i int
		for remote_listing.IsTruncated {
			var err error
			remote_listing, err = this.remoteListNext(remote_listing)
			if err != nil {
				log.Fatalln("Error while listing ", err)
				return
			}
			for _, item := range remote_listing.Contents {
				evt := this.makeDownloadEvent(item)
				startEvents <- evt
			}
			i++
		}
	}()
	return startEvents
}

func (this S3Sync) remoteList() (result *s3.ListResp, err error) {
	this.setDefaults()
	if this.SyncBucket == nil {
		err = errors.New("SyncBucket must be defined. This requires Aws creditials, region, and SyncBucketName to be specified.")
		return
	}
	result, err = this.SyncBucket.List(this.BasePath, "", "", ListBatchSize)
	return
}

func (this S3Sync) remoteListNext(current *s3.ListResp) (result *s3.ListResp, err error) {
	if current == nil || current.NextMarker == "" {
		return this.remoteList()
	}
	this.setDefaults()
	if this.SyncBucket == nil {
		err = errors.New("SyncBucket must be defined. This requires Aws creditials, region, and SyncBucketName to be specified.")
		return
	}
	result, err = this.SyncBucket.List(this.BasePath, "", current.NextMarker, ListBatchSize)
	return
}
