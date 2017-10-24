package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
)

var (
	region = "us-west-2"
)

func NewSession() *session.Session {
	return session.Must(session.NewSession(aws.NewConfig().WithRegion(region)))
}
