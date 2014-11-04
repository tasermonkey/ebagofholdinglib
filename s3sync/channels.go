package s3sync

import (
	"github.com/crowdmob/goamz/s3"
	"time"
)

type DownloadEvent struct {
	RemoteItem    s3.Key
	LocalFileName string
	StartTime     time.Time
}

type UploadEvent struct {
	remotePath    string
	localFileName string
	StartTime     time.Time
}

func ChannelTee(input <-chan DownloadEvent, outputs ...chan<- DownloadEvent) {
	for input := range input {
		for _, out := range outputs {
			out <- input
		}
	}
	for _, out := range outputs {
		close(out)
	}
}
