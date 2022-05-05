package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/otelutils"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/cyverse-de/p/go/analysis"
	"github.com/cyverse-de/p/go/header"
	"github.com/cyverse-de/p/go/svcerror"
	pbuser "github.com/cyverse-de/p/go/user"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const serviceName = "discoenv-analyses"

var log = logging.Log.WithFields(logrus.Fields{"service": serviceName})

func main() {
	var (
		err    error
		config *viper.Viper

		natsURL       = flag.String("nats", "tls://nats:4222", "NATS connection URL")
		configPath    = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the config file")
		tlsCert       = flag.String("tlscert", "/etc/nats/tls.crt", "Path to the TLS cert used for the NATS connection")
		tlsKey        = flag.String("tlskey", "/etc/nats/tls.key", "Path to the TLS key used for the NATS connection")
		caCert        = flag.String("tlsca", "/etc/nats/ca.crt", "Path to the TLS CA cert used for the NATS connection")
		credsPath     = flag.String("creds", "/etc/nats/service.creds", "Path to the NATS user creds file used for authn with NATS")
		maxReconnects = flag.Int("max-reconnects", 10, "The number of reconnection attempts the NATS client will make if the server does not respond")
		reconnectWait = flag.Int("reconnect-wait", 1, "The number of seconds to wait between reconnection attempts")
		natsSubject   = flag.String("subject", "cyverse.discoenv.analyses.>", "The NATS subject to subscribe to for incoming requests")
		natsQueue     = flag.String("queue", "discoenv_analyses_service", "The NATS queue name for this instance. Joins to a queue group by default")
		usersSubject  = flag.String("users-subject", "cyverse.discoenv.users.requests", "The NATS subject to send user-related requests out on")
		logLevel      = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	// Open Telemetry setup
	tracerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

	// Use this client instead of the default client from the http package.
	httpClient := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	// Register the encoder that will decode the incoming protocol buffers messages.
	nats.RegisterEncoder("protojson", &protobufjson.Codec{})

	config, err = configurate.Init(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("read configuration from %s", *configPath)

	appsBase := config.GetString("apps.base")
	if appsBase == "" {
		log.Fatal("apps.base must be set in the configuration file")
	}

	appsBaseURL, err := url.Parse(appsBase)
	if err != nil {
		log.Fatal("apps.base must be a parseable URL")
	}

	log.Infof("APPS base URL is %s", appsBase)
	log.Infof("NATS URL is %s", *natsURL)
	log.Infof("NATS TLS cert file is %s", *tlsCert)
	log.Infof("NATS TLS key file is %s", *tlsKey)
	log.Infof("NATS CA cert file is %s", *caCert)
	log.Infof("NATS creds file is %s", *credsPath)

	// Connect to NATS
	nc, err := nats.Connect(
		*natsURL,
		nats.UserCredentials(*credsPath),
		nats.RootCAs(*caCert),
		nats.ClientCert(*tlsCert, *tlsKey),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(*maxReconnects),
		nats.ReconnectWait(time.Duration(*reconnectWait)*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Ensure that the messages are automatically decoded from protobuf JSON
	conn, err := nats.NewEncodedConn(nc, "protojson")
	if err != nil {
		log.Fatal(err)
	}

	if _, err = conn.QueueSubscribe(*natsSubject, *natsQueue, getHandler(conn, &httpClient, appsBaseURL, *usersSubject)); err != nil {
		log.Fatal(err)
	}

	select {}
}

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

func getAnalysis(httpClient *http.Client, appsBaseURL *url.URL, filter []map[string]string) ([]*analysis.AnalysisRecord, error) {
	reqURL := *appsBaseURL
	reqURL.Path = path.Join(reqURL.Path, "/analysis")

	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}

	reqURL.Query().Add("filter", url.QueryEscape(string(filterJSON)))

	response, err := httpClient.Get(reqURL.String())
	if err != nil {
		response.Body.Close()
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	resp := analysisResponse{}
	if err = json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	return resp.Analyses, nil
}

func getAnalysisIDByExternalID(httpClient *http.Client, appsBaseURL *url.URL, requestingUser, externalID string) (string, error) {
	reqURL := *appsBaseURL
	reqURL.Path = path.Join(reqURL.Path, "/admin/analyses/by-external-id", externalID)

	reqURL.Query().Add("user", requestingUser)

	response, err := httpClient.Get(reqURL.String())
	if err != nil {
		response.Body.Close()
		return "", err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	resp := analysisResponse{}
	if err = json.Unmarshal(body, &resp); err != nil {
		return "", err
	}

	if len(resp.Analyses) == 0 {
		return "", ErrAnalysisNotFound
	}

	return resp.Analyses[0].Id, nil
}

func lookupUsername(ctx context.Context, conn *nats.EncodedConn, subject string, userID string) (string, error) {
	var (
		err      error
		request  *pbuser.UserLookupRequest
		expected *pbuser.User
	)
	request = &pbuser.UserLookupRequest{
		LookupIds: &pbuser.UserLookupRequest_UserId{
			UserId: userID,
		},
	}
	expected, err = NATSRequest[*pbuser.UserLookupRequest, *pbuser.User](ctx, conn, subject, request)
	if err != nil {
		return "", err
	}
	return expected.Username, nil
}

func getHandler(conn *nats.EncodedConn, httpClient *http.Client, appsBaseURL *url.URL, usersSubject string) nats.Handler {
	return func(subject, reply string, request *analysis.AnalysisRecordLookupRequest) {
		var (
			err        error
			filter     []map[string]string
			analysisID string
		)
		// Set up telemetry tracking
		carrier := gotelnats.PBTextMapCarrier{
			Header: request.Header,
		}

		ctx, span := gotelnats.StartSpan(&carrier, subject, gotelnats.Process)
		defer span.End()

		log.Infof("%+v\n", request)

		requestingUser := request.RequestingUser

		switch request.LookupIds.(type) {
		case *analysis.AnalysisRecordLookupRequest_AnalysisId:
			analysisID = request.GetAnalysisId()

			filter = []map[string]string{
				{
					"id":   analysisID,
					"user": requestingUser,
				},
			}

		case *analysis.AnalysisRecordLookupRequest_ExternalId:
			// Hits the /admin/analyses/by-external-id/{external-id} endpoint,
			// grabs the analysis ID, and then calls the analysis id endpoint.
			// This is done so that any permissions logic in the analysis id
			// endpoint is still hit, preventing unauthorized access to an
			// analysis through this endpoint.
			analysisID, err = getAnalysisIDByExternalID(httpClient, appsBaseURL, requestingUser, request.GetExternalId())
			if err != nil {
				if errors.Is(err, ErrAnalysisNotFound) {
					HandleError(ctx, err, reply, conn, &ErrorOptions{
						ErrorCode: svcerror.Code_NOT_FOUND,
					})
				} else {
					HandleError(ctx, err, reply, conn, &ErrorOptions{
						ErrorCode: svcerror.Code_BAD_REQUEST,
					})
				}
				return
			}

			filter = []map[string]string{
				{
					"id":   analysisID,
					"user": requestingUser,
				},
			}

		case *analysis.AnalysisRecordLookupRequest_UserId:
			// Hits the discoenv-users service to get the username and then
			// filters with that.
			username, err := lookupUsername(ctx, conn, usersSubject, request.GetUserId())
			if err != nil {
				HandleError(ctx, err, reply, conn, nil)
				return
			}

			filter = []map[string]string{
				{
					"username": username,
					"user":     requestingUser,
				},
			}

		case *analysis.AnalysisRecordLookupRequest_Username:
			// Hits the /analyses endpoint and filters by username. filter needs to be [{"field":"username", "value":"<username>"}]
			username := request.GetUsername()

			filter = []map[string]string{
				{
					"username": username,
					"user":     requestingUser,
				},
			}
		}

		log.Debug(filter)

		records, err := getAnalysis(httpClient, appsBaseURL, filter)
		if err != nil {
			HandleError(ctx, err, reply, conn, nil)
			return
		}

		analysisList := analysis.AnalysisRecordList{
			Analyses: records,
		}

		if err = NATSPublishResponse(ctx, conn, reply, &analysisList); err != nil {
			HandleError(ctx, err, reply, conn, nil)
			return
		}
	}
}

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

type ErrorOptions struct {
	ErrorCode svcerror.Code
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
		serviceErr = &DEServiceError{
			ServiceError: &svcerror.Error{
				ErrorCode: svcerror.Code_INTERNAL,
				Message:   err.Error(),
			},
		}
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
