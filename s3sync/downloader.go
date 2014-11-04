package s3sync

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

var (
	DOWNLOAD_WORKERS = myenv.GetDownloadWorkers()
)

func (this S3Sync) DownloadItems(incoming <-chan DownloadEvent) <-chan DownloadEvent {
	output := make(chan DownloadEvent, DOWNLOAD_WORKERS)
	var wg sync.WaitGroup
	wg.Add(DOWNLOAD_WORKERS)
	for i := 0; i < DOWNLOAD_WORKERS; i++ {
		go func() {
			defer wg.Done()
			for evt := range incoming {
				err := this.writeS3ObjectToDisk(evt)
				if err != nil {
					log.Fatalln(err)
				} else {
					output <- evt
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(output)
	}()
	return output
}

func (this S3Sync) writeS3ObjectToDisk(evt DownloadEvent) (err error) {
	key := evt.RemoteItem
	localFullPath := evt.LocalFileName
	localDir := filepath.Dir(localFullPath)
	// Do local operations first to save on a request to s3 if some local error happened
	os.MkdirAll(localDir, 0700)
	out, err := os.Create(localFullPath)
	if err != nil {
		return
	}
	defer checkClose(out, &err)
	// now lets open up the remote file, and copy it to our local file.
	s3Object, err := this.remoteGetFile(key.Key)
	if err != nil {
		return
	}
	defer checkClose(s3Object, &err)
	_, err = io.Copy(out, s3Object.GetContents())
	return
}

func (this S3Sync) remoteGetFile(remoteFilePath string) (s3Obj S3Object, err error) {
	this.setDefaults()
	if this.SyncBucket == nil {
		err = errors.New("SyncBucket must be defined. This requires Aws creditials, region, and SyncBucketName to be specified.")
		return
	}
	response, err := this.SyncBucket.GetResponse(remoteFilePath)
	s3Obj = S3Object{s3Source: &this.S3,
		bucket:       this.SyncBucket,
		objectName:   remoteFilePath,
		httpResponse: response}
	return
}
