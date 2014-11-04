package s3sync

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

func (this *S3Sync) getFullLocalPath(path string) string {
	// this may be wrong for windows based systems as the aws key(filepath here) may have the wrong '/'
	return filepath.Join(this.LocalPath, path)
}

func checkClose(c io.Closer, errp *error) {
	err := c.Close()
	if err != nil && *errp == nil {
		*errp = err
	}
}

func getFirstLineOfFileAsString(localFullPath string) (output string, err error) {
	f, err := os.Open(localFullPath)
	if err != nil {
		return
	}
	defer checkClose(f, &err)
	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		output = scanner.Text()
	} else {
		if err = scanner.Err(); err != nil {
			return
		}
		err = errors.New(fmt.Sprintf("Unable to get first line from %s", localFullPath))
	}
	return
}

func writeStringToFile(localFullPath string, data string) (err error) {
	localFullPath = getEtagLocalFilename(localFullPath)
	// Do local operations first to save on a request to s3 if some local error happened
	os.MkdirAll(filepath.Dir(localFullPath), 0700)
	out, err := os.Create(localFullPath)
	if err != nil {
		return
	}
	defer checkClose(out, &err)
	out.WriteString(data)
	return
}

func DetermineContentType(file *os.File) (contentType ContentType, err error) {
	data := make([]byte, 512)
	_, err = file.Read(data)
	defer file.Seek(0, os.SEEK_SET)
	if err != nil {
		return
	}
	contentType = ContentType(http.DetectContentType(data))
	return
}
