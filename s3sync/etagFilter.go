package s3sync

import (
	"github.com/crowdmob/goamz/s3"
)

func (this S3Sync) needsDownloaded(evt DownloadEvent) bool {
	s3Key := evt.RemoteItem
	localFullPath := this.getFullLocalPath(s3Key.Key)
	etag, err := this.getLocalETagForFilename(localFullPath)
	remoteEtag := getEtagFromKey(s3Key)
	return err != nil || remoteEtag != etag
}

func (this S3Sync) FilterAlreadyDownloaded(incoming <-chan DownloadEvent) <-chan DownloadEvent {
	output := make(chan DownloadEvent)
	go func() {
		defer close(output)
		for evt := range incoming {
			if this.needsDownloaded(evt) {
				output <- evt
			}
		}
	}()
	return output
}

func (this S3Sync) WriteETagData(incoming <-chan DownloadEvent) <-chan DownloadEvent {
	output := make(chan DownloadEvent)
	go func() {
		defer close(output)
		for evt := range incoming {
			writeETagToDisk(evt)
			output <- evt
		}
	}()
	return output
}

func (this S3Sync) getLocalETagForFilename(localFullPath string) (etag string, err error) {
	localFullPath = getEtagLocalFilename(localFullPath)
	etag, err = getFirstLineOfFileAsString(localFullPath)
	return
}

func getEtagFromKey(etag s3.Key) string {
	return etag.ETag[1 : len(etag.ETag)-1]
}

func writeETagToDisk(evt DownloadEvent) (err error) {
	s3Key := evt.RemoteItem
	etag := getEtagFromKey(s3Key)
	localFullPath := evt.LocalFileName
	localFullPath = getEtagLocalFilename(localFullPath)
	err = writeStringToFile(localFullPath, etag)
	return
}
