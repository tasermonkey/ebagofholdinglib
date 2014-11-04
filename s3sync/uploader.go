package s3sync

import (
	"os"
	"sync"
)

func (this S3Sync) UploadItems(incoming <-chan UploadEvent) <-chan UploadEvent {
	output := make(chan UploadEvent, 5)
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for evt := range incoming {
				// path string, r io.Reader, length int64, contType string, perm ACL, options Option
				file, err := os.Open(evt.localFileName)
				fi, err := file.Stat()
				this.PutReader(evt.remotePath, rc, file)
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
