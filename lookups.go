package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/cyverse-de/p/go/analysis"
	pbuser "github.com/cyverse-de/p/go/user"
	"github.com/nats-io/nats.go"
)

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
