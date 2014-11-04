package s3sync

import (
	"path/filepath"
)

func getEtagLocalFilename(localFullPath string) string {
	localDir := filepath.Dir(localFullPath)
	return filepath.Join(localDir, ".ebagmd", filepath.Base(localFullPath)+".etag")
}

func (this S3Sync) getRemoteFilenameFromLocalPath(localFullPath string) (remotePath string) {
	// in order for Rel to work properly we need the absolute path of the 'local bag'
	absLocalPath, _ := filepath.Abs(this.LocalPath)
	remotePath, _ = filepath.Rel(absLocalPath, localFullPath)
	return
}
