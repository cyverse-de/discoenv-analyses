package main

import (
	"context"
	"flag"
	"net/http"
	"net/url"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/otelutils"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

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
