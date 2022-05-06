package main

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// NATSRequest handles instrumenting the outgoing request with telemetry info,
// blocking until the request is responded to, and handling responses containing
// errors returned by the other service. It is a generic function that can
// accept values that implement the DETypes interfacefor both the outgoing
// request and the expected response.
func NATSRequest[ReqType DERequestTypes, Expected DEResponseTypes](
	ctx context.Context,
	conn *nats.EncodedConn,
	subject string,
	request ReqType,
) (Expected, error) {
	var (
		err   error
		dePtr Expected
	)

	carrier := gotelnats.PBTextMapCarrier{
		Header: request.GetHeader(),
	}

	_, span := gotelnats.InjectSpan(ctx, &carrier, subject, gotelnats.Send)
	defer span.End()

	// Uses the EncodedCode to unmarshal the data into dePtr.
	err = conn.Request(subject, request, dePtr, 30*time.Second)
	if err != nil {
		return dePtr, err
	}

	// Since the potential error details are embedded in the unmarshalled
	// data, we have to look to make sure it's not set.
	dePtrErr := dePtr.GetError()
	if dePtrErr.ErrorCode != svcerror.ErrorCode_UNSET {
		if dePtrErr.StatusCode != 0 { // httpStatusCode is optional.
			err = NewDEServiceError(dePtrErr.ErrorCode, dePtrErr.Message, dePtrErr.StatusCode)
		} else {
			err = NewDEServiceError(dePtrErr.ErrorCode, dePtrErr.Message)
		}
		return dePtr, err
	}

	return dePtr, nil
}

// NATSPublishRespone instruments outgoing responses with telemetry information.
// It is a generic function that will accept types that implement the DETypes
// interface.
func NATSPublishResponse[ResponseT DEResponseTypes](
	ctx context.Context,
	conn *nats.EncodedConn,
	reply string,
	response ResponseT,
) error {
	reflectValue := reflect.ValueOf(response)
	if reflectValue.Kind() != reflect.Pointer || reflectValue.IsNil() {
		return fmt.Errorf("cannot unmarshal into type '%s'; it must be a pointer and non-nil", reflect.TypeOf(response))
	}

	carrier := gotelnats.PBTextMapCarrier{
		Header: response.GetHeader(),
	}

	_, span := gotelnats.InjectSpan(ctx, &carrier, reply, gotelnats.Send)
	defer span.End()

	return conn.Publish(reply, response)
}

func InitServiceError(ctx context.Context, err error, opts *ErrorOptions) *svcerror.ServiceError {
	var svcErr *svcerror.ServiceError

	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	switch err.(type) {
	case DEServiceError:
		svcErr = err.(DEServiceError).ServiceError
	case *DEServiceError:
		svcErr = err.(*DEServiceError).ServiceError
	default:
		svcErr = &svcerror.ServiceError{
			ErrorCode: svcerror.ErrorCode_UNSPECIFIED,
			Message:   err.Error(),
		}
	}

	if opts != nil {
		if opts.ErrorCode != svcerror.ErrorCode_UNSET {
			svcErr.ErrorCode = opts.ErrorCode
		}
		if opts.StatusCode != 0 {
			svcErr.StatusCode = opts.StatusCode
		}
	}

	return svcErr
}
