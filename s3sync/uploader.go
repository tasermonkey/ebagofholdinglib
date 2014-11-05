package s3sync

import (
	//"net/http"
	"github.com/crowdmob/goamz/s3"
	"log"
	"os"
	"sync"
)

var (
	UPLOAD_WORKERS = myenv.GetUploadWorkers()
)

func (this S3Sync) UploadItems(incoming <-chan UploadEvent) <-chan UploadEvent {
	output := make(chan UploadEvent, UPLOAD_WORKERS)
	var wg sync.WaitGroup
	wg.Add(UPLOAD_WORKERS)
	for i := 0; i < UPLOAD_WORKERS; i++ {
		go func() {
			defer wg.Done()
			for evt := range incoming {
				// path string, r io.Reader, length int64, contType string, perm ACL, options Option
				file, err := os.Open(evt.LocalFileName)
				if err != nil {
					log.Fatalln(err)
					continue
				}
				defer checkClose(file, &err)
				fi, err := file.Stat()
				if err != nil {
					log.Fatalln(err)
					continue
				}
				contentType, err := DetermineContentType(file)
				if err != nil {
					log.Fatalln(err)
					continue
				}
				err = this.SyncBucket.PutReader(evt.RemotePath, file, fi.Size(), string(contentType), s3.Private, s3.Options{})
				if err != nil {
					log.Fatalln(err)
					continue
				}
				output <- evt
			}
		}()
	}
	go func() {
		wg.Wait()
		close(output)
	}()
	return output
}
