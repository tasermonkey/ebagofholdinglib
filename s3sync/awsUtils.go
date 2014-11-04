package s3sync

import (
	"github.com/crowdmob/goamz/aws"
)

func isAwsAuthEmpty(auth aws.Auth) bool {
	return len(auth.AccessKey) == 0 || len(auth.SecretKey) == 0
}

func isAwsRegionEmpty(region aws.Region) bool {
	return len(region.Name) == 0 || len(region.S3Endpoint) == 0 || len(region.EC2Endpoint) == 0
}
