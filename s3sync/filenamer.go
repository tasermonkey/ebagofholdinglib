package s3sync

func (this S3Sync) name_remote_file(incoming <-chan UploadEvent) <-chan UploadEvent {
	output := make(chan UploadEvent)
	go func() {
		defer close(output)
		for evt := range incoming {
			output <- evt
		}
	}()
	return output
}
