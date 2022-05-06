package main

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/p/go/analysis"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/nats-io/nats.go"
)

func getHandler(conn *nats.EncodedConn, httpClient *http.Client, appsBaseURL *url.URL, usersSubject string) nats.Handler {
	return func(subject, reply string, request *analysis.AnalysisRecordLookupRequest) {
		var (
			err        error
			filter     []map[string]string
			analysisID string
		)

		analysisList := analysis.AnalysisRecordList{
			Analyses: []*analysis.AnalysisRecord{},
		}

		// Set up telemetry tracking
		carrier := gotelnats.PBTextMapCarrier{
			Header: request.Header,
		}

		ctx, span := gotelnats.StartSpan(&carrier, subject, gotelnats.Process)
		defer span.End()

		log.Debugf("%+v\n", request)

		requestingUser := request.RequestingUser

		if requestingUser == "" {
			analysisList.Error = gotelnats.InitServiceError(
				ctx,
				errors.New("requesting_user must be set in request"),
				&gotelnats.ErrorOptions{
					ErrorCode: svcerror.ErrorCode_PARAMETER_MISSING,
				},
			)
			if err = gotelnats.PublishResponse(ctx, conn, reply, &analysisList); err != nil {
				log.Error(err)
			}
			return
		}

		switch request.LookupIds.(type) {
		case *analysis.AnalysisRecordLookupRequest_AnalysisId:
			analysisID = request.GetAnalysisId()

			filter = []map[string]string{
				{
					"field": "id",
					"value": analysisID,
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
					analysisList.Error = gotelnats.InitServiceError(ctx, err, &gotelnats.ErrorOptions{
						ErrorCode: svcerror.ErrorCode_NOT_FOUND,
					})
				} else {
					analysisList.Error = gotelnats.InitServiceError(ctx, err, &gotelnats.ErrorOptions{
						ErrorCode: svcerror.ErrorCode_BAD_REQUEST,
					})
				}
				if err = gotelnats.PublishResponse(ctx, conn, reply, &analysisList); err != nil {
					log.Error(err)
				}
				return
			}

			filter = []map[string]string{
				{
					"field": "id",
					"value": analysisID,
				},
			}

		case *analysis.AnalysisRecordLookupRequest_UserId:
			// Hits the discoenv-users service to get the username and then
			// filters with that.
			username, err := lookupUsername(ctx, conn, usersSubject, request.GetUserId())
			if err != nil {
				analysisList.Error = gotelnats.InitServiceError(ctx, err, nil)
				if err = gotelnats.PublishResponse(ctx, conn, reply, &analysisList); err != nil {
					log.Error(err)
				}
				return
			}

			filter = []map[string]string{
				{
					"field": "username",
					"value": username,
				},
			}

		case *analysis.AnalysisRecordLookupRequest_Username:
			// Hits the /analyses endpoint and filters by username. filter needs to be [{"field":"username", "value":"<username>"}]
			username := request.GetUsername()

			filter = []map[string]string{
				{
					"field": "username",
					"value": username,
				},
			}
		}

		records, err := getAnalysis(httpClient, appsBaseURL, requestingUser, filter)
		if err != nil {
			analysisList.Error = gotelnats.InitServiceError(ctx, err, nil)
			if err = gotelnats.PublishResponse(ctx, conn, reply, &analysisList); err != nil {
				log.Error(err)
			}
			return
		}

		analysisList.Analyses = records

		if err = gotelnats.PublishResponse(ctx, conn, reply, &analysisList); err != nil {
			log.Error(err)
			return
		}
	}
}
