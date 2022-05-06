package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/cyverse-de/p/go/analysis"
	"github.com/cyverse-de/p/go/svcerror"
	pbuser "github.com/cyverse-de/p/go/user"
	"github.com/nats-io/nats.go"
)

func getAnalysis(httpClient *http.Client, appsBaseURL *url.URL, requestingUser string, filter []map[string]string) ([]*analysis.AnalysisRecord, error) {
	reqURL := *appsBaseURL
	reqURL.Path = path.Join(reqURL.Path, "/analyses")

	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}

	q := reqURL.Query()
	q.Add("user", requestingUser)
	q.Add("filter", string(filterJSON))
	reqURL.RawQuery = q.Encode()

	response, err := httpClient.Get(reqURL.String())
	if err != nil {
		response.Body.Close()
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 400 {
		return nil, fmt.Errorf("status code from '%s' was %d", reqURL.String(), response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	resp := analysis.AnalysisRecordResponse{}
	if err = json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	return resp.Analyses, nil
}

func getAnalysisIDByExternalID(httpClient *http.Client, appsBaseURL *url.URL, requestingUser, externalID string) (string, error) {
	reqURL := *appsBaseURL
	reqURL.Path = path.Join(reqURL.Path, "/admin/analyses/by-external-id", externalID)

	q := reqURL.Query()
	q.Add("user", requestingUser)
	reqURL.RawQuery = q.Encode()

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

	if response.StatusCode < 200 || response.StatusCode >= 400 {
		svcErr := NewDEServiceError(svcerror.ErrorCode_INTERNAL, string(body))
		return "", svcErr
	}

	resp := analysis.AnalysisRecordList{}
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
