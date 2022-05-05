package main

import (
	"context"
	"errors"
	"time"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func NATSRequest[ReqType DETypes, Expected DETypes](ctx context.Context, conn *nats.EncodedConn, subject string, request ReqType) (Expected, error) {
	var (
		err  error
		resp Expected
		ok   bool
	)

	carrier := gotelnats.PBTextMapCarrier{
		Header: request.GetHeader(),
	}

	_, span := gotelnats.InjectSpan(ctx, &carrier, subject, gotelnats.Send)
	defer span.End()

	natsMsg := nats.Msg{}

	if err = conn.Request(subject, &request, &natsMsg, 30*time.Second); err != nil {
		return resp, err
	}

	var t interface{}
	if err = protobufjson.Unmarshal(natsMsg.Data, &t); err != nil {
		return resp, err
	}

	switch t.(type) {
	case svcerror.Error:
		respErr := t.(DEServiceError)
		return resp, &respErr
	default:
		resp, ok = t.(Expected)
		if !ok {
			return resp, errors.New("could not cast body to expected type")
		}
	}

	return resp, nil
}

func NATSPublishResponse[ResponseT DETypes](ctx context.Context, conn *nats.EncodedConn, reply string, response ResponseT) error {
	carrier := gotelnats.PBTextMapCarrier{
		Header: response.GetHeader(),
	}

	_, span := gotelnats.InjectSpan(ctx, &carrier, reply, gotelnats.Send)
	defer span.End()

	return conn.Publish(reply, response)
}

func HandleError(ctx context.Context, err error, reply string, conn *nats.EncodedConn, opts *ErrorOptions) {
	var serviceErr *DEServiceError

	// Set error information into the span.
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	// Set up the service error.
	switch err.(type) {
	case *DEServiceError:
		serviceErr = err.(*DEServiceError)
	default:
		serviceErr = NewServiceError(&svcerror.Error{
			ErrorCode: svcerror.Code_INTERNAL,
			Message:   err.Error(),
		})
	}

	// Let the options ErrorCode override the one passed with the error.
	if opts != nil {
		serviceErr.ServiceError.ErrorCode = opts.ErrorCode
	}

	// Make sure the span is set up in the outgoing message.
	carrier := gotelnats.PBTextMapCarrier{
		Header: serviceErr.ServiceError.Header,
	}

	_, span = gotelnats.InjectSpan(ctx, &carrier, reply, gotelnats.Send)
	defer span.End()

	if err = conn.Publish(reply, &serviceErr.ServiceError); err != nil {
		log.Error(err)
	}
}
