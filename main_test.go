package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	queryv1alpha1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockParcaQuerier is a mock implementation of the ParcaQuerier interface.
type mockParcaQuerier struct {
	QueryParcaFunc func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (*queryv1alpha1.QueryResponse, error)
}

func (m *mockParcaQuerier) QueryParca(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (*queryv1alpha1.QueryResponse, error) {
	if m.QueryParcaFunc != nil {
		return m.QueryParcaFunc(ctx, queryStr, start, end, reportTypeStr)
	}
	return nil, errors.New("QueryParcaFunc not implemented in mock")
}

func TestParcaQueryHandler(t *testing.T) {
	// Sample valid times for requests
	validStartTime := time.Now().Add(-1 * time.Hour)
	validEndTime := time.Now()
	validStartTimeStr := validStartTime.Format(time.RFC3339)
	validEndTimeStr := validEndTime.Format(time.RFC3339)

	// Sample QueryResponse for success cases
	sampleQueryResponse := &queryv1alpha1.QueryResponse{
		Report: &queryv1alpha1.QueryResponse_FlamegraphArrow{
			FlamegraphArrow: &queryv1alpha1.FlamegraphArrow{
				Total: 100,
				Unit:  "samples",
			},
		},
		Total: 100,
		Filtered: 0,
		PeriodType: &queryv1alpha1.PeriodType{Type: "cpu", Unit: "nanoseconds"},
		Timestamp: timestamppb.New(validEndTime),
		Duration: durationpb.New(validEndTime.Sub(validStartTime)),
	}


	tests := []struct {
		name               string
		queryParams        map[string]string
		mockSetup          func(*mockParcaQuerier)
		expectedStatusCode int
		expectedErrorMsg   string // Substring to check in error response
		checkBody          func(t *testing.T, body []byte, expectedStatusCode int) // Custom body check for success or complex cases
	}{
		{
			name: "success case",
			queryParams: map[string]string{
				"query":      `{__name__="process_cpu"}`,
				"start":      validStartTimeStr,
				"end":        validEndTimeStr,
				"reportType": "flamegraph_arrow",
			},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (*queryv1alpha1.QueryResponse, error) {
					return sampleQueryResponse, nil
				}
			},
			expectedStatusCode: http.StatusOK,
			checkBody: func(t *testing.T, body []byte, expectedStatusCode int) {
				var resp queryv1alpha1.QueryResponse
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatalf("Failed to unmarshal response body: %v. Body: %s", err, string(body))
				}
				// Basic check, can be more thorough. For proto, direct comparison is hard.
				if resp.GetFlamegraphArrow().GetTotal() != sampleQueryResponse.GetFlamegraphArrow().GetTotal() {
					t.Errorf("Expected flamegraph total %d, got %d", sampleQueryResponse.GetFlamegraphArrow().GetTotal(), resp.GetFlamegraphArrow().GetTotal())
				}
			},
		},
		{
			name:               "missing query parameter",
			queryParams:        map[string]string{"start": validStartTimeStr, "end": validEndTimeStr, "reportType": "pprof"},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "Missing required query parameters",
		},
		{
			name:               "missing start parameter",
			queryParams:        map[string]string{"query": "test", "end": validEndTimeStr, "reportType": "pprof"},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "Missing required query parameters",
		},
		{
			name:               "missing end parameter",
			queryParams:        map[string]string{"query": "test", "start": validStartTimeStr, "reportType": "pprof"},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "Missing required query parameters",
		},
		{
			name:               "missing reportType parameter",
			queryParams:        map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "Missing required query parameters",
		},
		{
			name:               "invalid start time format",
			queryParams:        map[string]string{"query": "test", "start": "invalid-time", "end": validEndTimeStr, "reportType": "pprof"},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "Invalid start time format",
		},
		{
			name:               "invalid end time format",
			queryParams:        map[string]string{"query": "test", "start": validStartTimeStr, "end": "invalid-time", "reportType": "pprof"},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "Invalid end time format",
		},
		{
			name:               "start time after end time",
			queryParams:        map[string]string{"query": "test", "start": validEndTimeStr, "end": validStartTimeStr, "reportType": "pprof"},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "Start time must be before end time",
		},
		{
			name: "QueryParca returns client error (invalid reportType)",
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "invalid_type"},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (*queryv1alpha1.QueryResponse, error) {
					return nil, errors.New("invalid reportType: invalid_type") // Error from Querier.QueryParca
				}
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorMsg:   "invalid reportType: invalid_type",
		},
		{
			name: "QueryParca returns server error",
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "pprof"},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (*queryv1alpha1.QueryResponse, error) {
					return nil, errors.New("internal Parca server error")
				}
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorMsg:   "internal Parca server error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockQuerier := &mockParcaQuerier{}
			if tc.mockSetup != nil {
				tc.mockSetup(mockQuerier)
			}

			api := &apiHandler{querier: mockQuerier}

			reqURL := "/parcaquery"
			if len(tc.queryParams) > 0 {
				reqURL += "?"
				params := []string{}
				for k, v := range tc.queryParams {
					params = append(params, fmt.Sprintf("%s=%s", k, v))
				}
				reqURL += strings.Join(params, "&")
			}

			req, err := http.NewRequest("GET", reqURL, nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(api.parcaQueryHandler)
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tc.expectedStatusCode {
				t.Errorf("handler returned wrong status code: got %v want %v. Body: %s",
					status, tc.expectedStatusCode, rr.Body.String())
			}

			if tc.expectedErrorMsg != "" {
				var jsonError map[string]string
				if err := json.Unmarshal(rr.Body.Bytes(), &jsonError); err != nil {
					t.Fatalf("Failed to unmarshal error response body: %v. Body: %s", err, rr.Body.String())
				}
				if errMsg, ok := jsonError["error"]; !ok || !strings.Contains(errMsg, tc.expectedErrorMsg) {
					t.Errorf("handler returned unexpected error message: got '%s' want substring '%s'",
						jsonError["error"], tc.expectedErrorMsg)
				}
			}

			if tc.checkBody != nil {
				tc.checkBody(t, rr.Body.Bytes(), tc.expectedStatusCode)
			}
		})
	}
}
