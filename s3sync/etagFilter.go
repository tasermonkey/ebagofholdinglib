package s3sync

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/crowdmob/goamz/s3"
	"io"
	"log"
	"os"
)

func (this S3Sync) needsDownloaded(evt DownloadEvent) bool {
	s3Key := evt.RemoteItem
	localFullPath := this.getFullLocalPath(s3Key.Key)
	etag, err := this.getLocalETagForFilename(localFullPath)
	remoteEtag := getEtagFromKey(s3Key)
	// var condition string
	// if etag == remoteEtag {
	// 	condition = " == "
	// } else {
	// 	condition = " != "
	// }
	// log.Println(etag, condition, remoteEtag)
	return err != nil || remoteEtag != etag
}

func (this S3Sync) needsUpload(evt UploadEvent) bool {
	localFullPath := evt.LocalFileName
	etag, err := this.generateETagForFile(localFullPath)
	if err != nil {
		log.Println(err)
		return true
	}
	err = writeEtagToDisk(localFullPath, etag)
	if err != nil {
		log.Println(err)
		return true
	}
	return this.etagNotMatchesRemote(localFullPath, etag)
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

func (this S3Sync) FilterSameAsRemote(incoming <-chan UploadEvent) <-chan UploadEvent {
	output := make(chan UploadEvent)
	go func() {
		defer close(output)
		for evt := range incoming {
			if this.needsUpload(evt) {
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
			writeDownloadEventETagToDisk(evt)
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

func writeDownloadEventETagToDisk(evt DownloadEvent) (err error) {
	s3Key := evt.RemoteItem
	etag := getEtagFromKey(s3Key)
	return writeEtagToDisk(evt.LocalFileName, etag)
}

func writeEtagToDisk(localFullPath string, etag string) (err error) {
	localFullPath = getEtagLocalFilename(localFullPath)
	err = writeStringToFile(localFullPath, etag)
	return
}

func (this S3Sync) etagNotMatchesRemote(remotePath string, etag string) bool {
	var headers map[string][]string
	AddEtagHeader(headers, etag)
	response, err := this.SyncBucket.Head(remotePath, headers)
	if err != nil {
		log.Println(err)
		return false
	}
	s3Obj := S3Object{s3Source: &this.S3,
		bucket:       this.SyncBucket,
		objectName:   remotePath,
		httpResponse: response}
	return s3Obj.HasChanged()
}

func (this S3Sync) generateETagForFile(fullpath string) (etag string, err error) {
	f, err := os.Open(fullpath)
	if err != nil {
		return
	}
	defer checkClose(f, &err)
	st, err := f.Stat()
	if err != nil {
		return
	}
	if st.IsDir() {
		err = errors.New(fmt.Sprintf("%s is a directory", fullpath))
		return
	}
	md5sum := md5.New()
	io.Copy(md5sum, f)
	etag = fmt.Sprintf("%x", md5sum.Sum(nil))
	return
}
