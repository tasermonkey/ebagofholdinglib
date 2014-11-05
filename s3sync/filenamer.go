package s3sync

func (this S3Sync) RemoteFileNamer(incoming <-chan UploadEvent) <-chan UploadEvent {
	output := make(chan UploadEvent)
	go func() {
		defer close(output)
		for evt := range incoming {
			evt.RemotePath = this.getRemoteFilenameFromLocalPath(evt.LocalFileName)
			output <- evt
		}
	}()
	return output
}
