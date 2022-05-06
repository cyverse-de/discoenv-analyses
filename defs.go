package main

import (
	"errors"

	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/p/go/header"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const serviceName = "discoenv-analyses"

var log = logging.Log.WithFields(logrus.Fields{"service": serviceName})

var ErrAnalysisNotFound = errors.New("analysis not found")

type DERequestTypes interface {
	GetHeader() *header.Header

	proto.Message
}

type DEResponseTypes interface {
	GetHeader() *header.Header
	GetError() *svcerror.ServiceError

	proto.Message
}

type DEServiceError struct {
	ServiceError *svcerror.ServiceError
}

// NewDEServiceError returns a newly created instance of DEServiceError. The
// httpStatusCode parameters after 'message' are variadic, but only the first
// value is used. No error is raised if you pass in multiple status codes, the
// extra ones are just ignored. This prevents us from having to include error
// handling logic inside our error handling logic, which just gets annoying.
func NewDEServiceError(errorCode svcerror.ErrorCode, message string, httpStatusCode ...int32) *DEServiceError {
	se := svcerror.ServiceError{
		ErrorCode: errorCode,
		Message:   message,
	}

	if len(httpStatusCode) > 0 {
		se.StatusCode = httpStatusCode[0]
	}

	return &DEServiceError{
		ServiceError: &se,
	}
}

func (d DEServiceError) Error() string {
	return d.ServiceError.Message
}

type ErrorOptions struct {
	ErrorCode  svcerror.ErrorCode
	StatusCode int32
}
