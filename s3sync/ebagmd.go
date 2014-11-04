package s3sync

import (
	"github.com/twinj/uuid"
	"path/filepath"
)

func getEtagLocalFilename(localFullPath string) string {
	localDir := filepath.Dir(localFullPath)
	return filepath.Join(localDir, ".ebagmd", filepath.Base(localFullPath)+".etag")
}

func (this S3Sync) getRemoteFilenameFromLocalPath(localFullPath string) (remotePath string) {
	localDir := filepath.Dir(localFullPath)
	// in order for Rel to work properly we need the absolute path of the 'local bag'
	absLocalPath := filepath.Abs(this.LocalPath)
	remotePath = filepath.Rel(absLocalPath, localFullPath)
	return
}
