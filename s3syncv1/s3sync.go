package s3syncv1

import (
	"bufio"
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/tasermonkey/ebagofholdinglib"
	"github.com/tasermonkey/goenv"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	myenv         = goenv.DefaultGoenv()
	DefaultAuth   = myenv.GetAwsAuth()
	DefaultRegion = myenv.GetAwsRegion()
	ListBatchSize = myenv.GetListBatchSize()
	DefaultBucket = myenv.GetAwsBucketName()
	DefaultDBPath = myenv.GetSyncDbFilename()
)

type S3Context struct {
	s3.S3
	SyncBucketName string
	SyncBucket     *s3.Bucket
	BasePath       string
}

type DownloadEvent struct {
	RemoteItem    s3.Key
	LocalFileName string
	startTime     time.Time
	finishTime    time.Time
}

type DownloadEventsChannels struct {
	startEvents  chan<- DownloadEvent
	finishEvents chan<- DownloadEvent
}

type LocalInfo struct {
	LocalPath string
	DbPath    string
}

type S3Sync struct {
	S3Context
	LocalInfo
}

func isAwsAuthEmpty(auth aws.Auth) bool {
	return len(auth.AccessKey) == 0 || len(auth.SecretKey) == 0
}

func isAwsRegionEmpty(region aws.Region) bool {
	return len(region.Name) == 0 || len(region.S3Endpoint) == 0 || len(region.EC2Endpoint) == 0
}

func (this *S3Sync) setDefaults() {
	if isAwsAuthEmpty(this.Auth) {
		this.Auth = DefaultAuth
	}
	if isAwsRegionEmpty(this.Region) {
		this.Region = DefaultRegion
	}
	if this.SyncBucket != nil {
		this.SyncBucketName = this.SyncBucket.Name
	} else {
		if this.SyncBucketName == "" {
			this.SyncBucketName = DefaultBucket
		}
		if this.SyncBucketName != "" {
			this.SyncBucket = this.Bucket(this.SyncBucketName)
		}
	}
}

func (this S3Sync) RemoteList() (result *s3.ListResp, err error) {
	this.setDefaults()
	if this.SyncBucket == nil {
		err = errors.New("SyncBucket must be defined. This requires Aws creditials, region, and SyncBucketName to be specified.")
		return
	}
	result, err = this.SyncBucket.List(this.BasePath, "", "", ListBatchSize)
	return
}

func (this S3Sync) RemoteListNext(current *s3.ListResp) (result *s3.ListResp, err error) {
	if current == nil || current.NextMarker == "" {
		return this.RemoteList()
	}
	this.setDefaults()
	if this.SyncBucket == nil {
		err = errors.New("SyncBucket must be defined. This requires Aws creditials, region, and SyncBucketName to be specified.")
		return
	}
	result, err = this.SyncBucket.List(this.BasePath, "", current.NextMarker, ListBatchSize)
	return
}

func (this S3Sync) ListFilesToDownload() (result *s3.ListResp, err error) {
	return this.ListFilesToDownloadNext(nil)
}

func (this S3Sync) ListFilesToDownloadNext(current *s3.ListResp) (result *s3.ListResp, err error) {
	var contents []s3.Key
	result = current
	for len(contents) < ListBatchSize {
		result, err = this.RemoteListNext(result)
		if err != nil {
			return nil, err
		}
		for _, file := range result.Contents {
			if this.needsDownloaded(file) {
				contents = append(contents, file)
			}
		}
	}
	result.Contents = contents
	return
}

func (this S3Sync) RemoteGetFile(filePath string) (rc io.ReadCloser, err error) {
	this.setDefaults()
	log.Println("AWS_AUTH", this.Auth)
	if this.SyncBucket == nil {
		err = errors.New("SyncBucket must be defined. This requires Aws creditials, region, and SyncBucketName to be specified.")
		return
	}
	rc, err = this.SyncBucket.GetReader(filePath)
	return
}

func (this S3Sync) WriteAllS3ObjectsToDisk(keys []s3.Key, events DownloadEventsChannels) (err error) {
	defer close(events.startEvents)
	defer close(events.finishEvents)
	for _, key := range keys {
		event := DownloadEvent{key, this.getFullLocalPath(key.Key), time.Now(), nil}
		events.startEvents <- event
		err = this.WriteS3ObjectToDisk(key)
		event.finishTime = time.Now()
		events.finishEvents <- event
		if err != nil {
			return
		}
	}
	return
}

func (this S3Sync) WriteS3ObjectToDisk(key s3.Key) (err error) {
	localFullPath := this.getFullLocalPath(key.Key)
	localDir := filepath.Dir(localFullPath)
	// Do local operations first to save on a request to s3 if some local error happened
	os.MkdirAll(localDir, 0700)
	out, err := os.Create(localFullPath)
	if err != nil {
		return
	}
	defer ebagofholdinglib.CheckClose(out, &err)
	// now lets open up the remote file, and copy it to our local file.
	reader, err := this.RemoteGetFile(key.Key)
	if err != nil {
		return
	}
	defer ebagofholdinglib.CheckClose(reader, &err)
	_, err = io.Copy(out, reader)
	if err == nil {
		err = this.writeETagToDisk(key)
	}
	return
}

func (this S3Sync) needsDownloaded(s3Key s3.Key) bool {
	localFullPath := this.getFullLocalPath(s3Key.Key)
	etag, err := this.getLocalETagForFilename(localFullPath)
	remoteEtag := getEtagFromKey(s3Key)
	log.Println(s3Key.Key, remoteEtag, "!=", etag)
	return err != nil || remoteEtag != etag
}

func (this S3Sync) GenerateETagForFile(filepath string) (etag string, err error) {
	fullpath := this.getFullLocalPath(filepath)
	f, err := os.Open(fullpath)
	if err != nil {
		return
	}
	defer ebagofholdinglib.CheckClose(f, &err)
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

func (this S3Sync) getFullLocalPath(path string) string {
	// this may be wrong for windows based systems as the aws key(filepath here) may have the wrong '/'
	return filepath.Join(this.LocalPath, path)
}

func (this S3Sync) getLocalETagForFilename(localFullPath string) (etag string, err error) {
	localFullPath = getEtagLocalFilename(localFullPath)
	f, err := os.Open(localFullPath)
	if err != nil {
		return
	}
	defer ebagofholdinglib.CheckClose(f, &err)
	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		etag = scanner.Text()
	} else {
		if err = scanner.Err(); err != nil {
			return
		}
		err = errors.New(fmt.Sprintf("Unable to get etag from %s", localFullPath))
		return
	}
	return
}

func getEtagFromKey(etag s3.Key) string {
	return etag.ETag[1 : len(etag.ETag)-1]
}

func getEtagLocalFilename(localFullPath string) string {
	localDir := filepath.Dir(localFullPath)
	return filepath.Join(localDir, ".etag", filepath.Base(localFullPath)+".etag")
}

func (this S3Sync) writeETagToDisk(s3Key s3.Key) (err error) {
	localFullPath := this.getFullLocalPath(s3Key.Key)
	localFullPath = getEtagLocalFilename(localFullPath)
	// Do local operations first to save on a request to s3 if some local error happened
	os.MkdirAll(filepath.Dir(localFullPath), 0700)
	out, err := os.Create(localFullPath)
	if err != nil {
		return
	}
	defer ebagofholdinglib.CheckClose(out, &err)
	etag := getEtagFromKey(s3Key)
	out.WriteString(etag)
	return
}

func MakeEventChannels() DownloadEventsChannels {
	return DownloadEventsChannels{make(chan DownloadEvent, 5), make(chan DownloadEvent, 5)}
}
