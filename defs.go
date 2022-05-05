package main

import (
	"errors"

	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/p/go/analysis"
	"github.com/cyverse-de/p/go/header"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const serviceName = "discoenv-analyses"

var log = logging.Log.WithFields(logrus.Fields{"service": serviceName})

type statusCount struct {
	Count  int64  `json:"count"`
	Status string `json:"status"`
}

type analysisResponse struct {
	Analyses    []*analysis.AnalysisRecord `json:"analyses"`
	Timestamp   string                     `json:"timestamp,omitempty"`
	Total       int64                      `json:"total,omitempty"`
	StatusCount []statusCount              `json:"status-count,omitempty"`
}

var ErrAnalysisNotFound = errors.New("analysis not found")

type DETypes interface {
	GetHeader() *header.Header

	proto.Message
}

type DEServiceError struct {
	ServiceError *svcerror.Error
}

func NewServiceError(serr *svcerror.Error) *DEServiceError {
	return &DEServiceError{
		ServiceError: serr,
	}
}

func (d DEServiceError) Error() string {
	return d.ServiceError.Message
}

type ErrorOptions struct {
	ErrorCode svcerror.Code
}
