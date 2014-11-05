package s3sync

import (
	"github.com/crowdmob/goamz/s3"
	"io"
	"net/http"
	"strconv"
)

type S3Object struct {
	s3Source     *s3.S3
	bucket       *s3.Bucket
	objectName   string
	httpResponse *http.Response
}

func (this S3Object) GetContents() (contents io.ReadCloser) {
	return this.httpResponse.Body
}

func (this S3Object) IsSuccessFull() bool {
	return this.httpResponse.StatusCode == 200
}

func (this S3Object) HasNotChanged() bool {
	return this.httpResponse.StatusCode == 412 || this.httpResponse.StatusCode == 304
}

func (this S3Object) HasChanged() bool {
	return !this.HasChanged()
}

func (this S3Object) HasDeleteMarker() bool {
	x := this.getFirstHeaderValue("x-amz-delete-mark")
	bval, _ := strconv.ParseBool(x)
	return bval
}

type ExpiryHeader string
type ContentType string

// TODO: parse expiry header into an object
func (this S3Object) GetExpiration() ExpiryHeader {
	return ExpiryHeader(this.getFirstHeaderValue("x-amz-expiration"))
}

// Valid Values: AES256
func (this S3Object) GetServerSideEncryption() string {
	return this.getFirstHeaderValue("x-amz-server-side​-encryption")
}

func (this S3Object) GetServerSideEncryptionCustomerAlgorithm() string {
	return this.getFirstHeaderValue("x-amz-server-side​-encryption​-customer-algorithm")
}

func (this S3Object) GetServerSideEncruptionCustomerKeyMd5() string {
	return this.getFirstHeaderValue("x-amz-server-side​-encryption​-customer-key-MD5")
}

func (this S3Object) GetVersionId() string {
	return this.getFirstHeaderValue("x-amz-version-id")
}

func (this S3Object) GetRedirectionLocation() string {
	return this.getFirstHeaderValue("x-amz-website​-redirect-location")
}

func (this S3Object) GetETagFromResponse() string {
	return this.getFirstHeaderValue("ETag")
}

func (this S3Object) GetContentType() ContentType {
	return ContentType(this.getFirstHeaderValue("Content-Type"))
}

func (this S3Object) GetContentLength() int64 {
	return this.httpResponse.ContentLength
}

func (this S3Object) GetMetaData(name string) string {
	return this.getFirstHeaderValue("x-amz-meta-" + name)
}

func (this S3Object) GetMetaDataInt(name string) int {
	result, _ := strconv.Atoi(this.GetMetaData(name))
	return result
}

func (this S3Object) GetMetaDataBool(name string) bool {
	bval, _ := strconv.ParseBool(this.GetMetaData(name))
	return bval
}

func (this S3Object) Close() error {
	return this.GetContents().Close()
}

func (this S3Object) getFirstHeaderValue(name string) string {
	x := this.httpResponse.Header[name]
	if len(x) >= 1 {
		return x[0]
	}
	return ""
}

func AddEtagHeader(h map[string][]string, etag string) {
	h["ETag"] = []string{etag}
}
