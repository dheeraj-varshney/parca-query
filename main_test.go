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
	"reflect" // Added for DeepEqual

	queryv1alpha1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockParcaQuerier is a mock implementation of the ParcaQuerier interface.
type mockParcaQuerier struct {
	// QueryParcaFunc's signature now returns (any, error) to match the ParcaQuerier interface.
	QueryParcaFunc func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error)
}

// QueryParca method matches the ParcaQuerier interface.
func (m *mockParcaQuerier) QueryParca(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
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
				"reportType": "flamegraph_arrow", // This is a standard protobuf response
			},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					// Return as any
					return sampleQueryResponse, nil
				}
			},
			expectedStatusCode: http.StatusOK,
			checkBody: func(t *testing.T, body []byte, expectedStatusCode int) {
				// Expecting queryv1alpha1.QueryResponse for this report type
				var resp queryv1alpha1.QueryResponse
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatalf("Failed to unmarshal QueryResponse body: %v. Body: %s", err, string(body))
				}
				if resp.GetFlamegraphArrow().GetTotal() != sampleQueryResponse.GetFlamegraphArrow().GetTotal() {
					t.Errorf("Expected flamegraph total %d, got %d", sampleQueryResponse.GetFlamegraphArrow().GetTotal(), resp.GetFlamegraphArrow().GetTotal())
				}
			},
		},
		// Test cases for json_flamegraph
		{
			name: "success json_flamegraph",
			queryParams: map[string]string{
				"query":      `{__name__="process_cpu"}`,
				"start":      validStartTimeStr,
				"end":        validEndTimeStr,
				"reportType": "json_flamegraph",
			},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					return &FlamegraphNode{Name: "root", Value: 100, Children: []*FlamegraphNode{{Name: "child1", Value: 60}}}, nil
				}
			},
			expectedStatusCode: http.StatusOK,
			checkBody: func(t *testing.T, body []byte, expectedStatusCode int) {
				var respNode FlamegraphNode
				if err := json.Unmarshal(body, &respNode); err != nil {
					t.Fatalf("Failed to unmarshal FlamegraphNode response body: %v. Body: %s", err, string(body))
				}
				if respNode.Name != "root" || respNode.Value != 100 {
					t.Errorf("Unexpected FlamegraphNode data: got name '%s', value %d", respNode.Name, respNode.Value)
				}
				if len(respNode.Children) != 1 || respNode.Children[0].Name != "child1" {
					t.Error("Unexpected FlamegraphNode children")
				}
			},
		},
		{
			name: "json_flamegraph - QueryParca returns conversion error",
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "json_flamegraph"},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					return nil, errors.New("mock flamegraph conversion error")
				}
			},
			expectedStatusCode: http.StatusInternalServerError, // This error comes from QueryParca, could be 500
			expectedErrorMsg:   "mock flamegraph conversion error",
		},
		{
			name: "json_flamegraph - QueryParca returns wrong type (handler type assertion error)",
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "json_flamegraph"},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					return &queryv1alpha1.QueryResponse{}, nil // Returning incorrect type for json_flamegraph
				}
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorMsg:   "Internal error processing flamegraph data",
		},
		// Test cases for json_stacks
		{
			name: "success json_stacks",
			queryParams: map[string]string{
				"query":      `{__name__="process_cpu"}`,
				"start":      validStartTimeStr,
				"end":        validEndTimeStr,
				"reportType": "json_stacks",
			},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					sampleReport := &JSONStacksReport{
						ReportType:        "json_stacks",
						Unit:              "samples",
						UniqueStackTraces: 1,
						TotalValue:        10,
						Samples: []*JSONStacksSample{
							{
								Value:  10,
								Labels: map[string][]string{"job": {"parca-load"}, "instance": {"localhost:7171"}},
								Stack: []*JSONStacksFrame{
									{FunctionName: "main.main", FileName: "main.go", LineNumber: 42, BinaryName: "parca-load"},
									{FunctionName: "runtime.main", FileName: "proc.go", LineNumber: 250},
								},
							},
						},
					}
					return sampleReport, nil
				}
			},
			expectedStatusCode: http.StatusOK,
			checkBody: func(t *testing.T, body []byte, expectedStatusCode int) {
				var respReport JSONStacksReport
				if err := json.Unmarshal(body, &respReport); err != nil {
					t.Fatalf("Failed to unmarshal JSONStacksReport response body: %v. Body: %s", err, string(body))
				}
				if respReport.ReportType != "json_stacks" || respReport.Unit != "samples" || respReport.TotalValue != 10 {
					t.Errorf("Unexpected JSONStacksReport metadata: got type '%s', unit '%s', total %d",
						respReport.ReportType, respReport.Unit, respReport.TotalValue)
				}
				if len(respReport.Samples) != 1 || respReport.Samples[0].Value != 10 {
					t.Errorf("Unexpected JSONStacksReport sample data: expected 1 sample with value 10, got %d samples", len(respReport.Samples))
					if len(respReport.Samples) > 0 {
						t.Logf("Sample 0 value: %d", respReport.Samples[0].Value)
					}
					return // Avoid panic on nil Samples[0]
				}

				expectedLabels := map[string][]string{"job": {"parca-load"}, "instance": {"localhost:7171"}}
				if !reflect.DeepEqual(respReport.Samples[0].Labels, expectedLabels) {
					t.Errorf("Unexpected JSONStacksReport sample labels: got %+v, want %+v",
						respReport.Samples[0].Labels, expectedLabels)
				}

				if len(respReport.Samples[0].Stack) != 2 || respReport.Samples[0].Stack[0].FunctionName != "main.main" {
					t.Error("Unexpected JSONStacksReport stack data")
				}
				if respReport.Samples[0].Stack[0].BinaryName != "parca-load" {
					t.Errorf("Expected BinaryName 'parca-load', got '%s'", respReport.Samples[0].Stack[0].BinaryName)
				}
			},
		},
		{
			name: "json_stacks - QueryParca returns conversion error",
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "json_stacks"},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					return nil, errors.New("mock pprof conversion error")
				}
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorMsg:   "mock pprof conversion error",
		},
		{
			name: "json_stacks - QueryParca returns wrong type (handler type assertion error)",
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "json_stacks"},
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					return &queryv1alpha1.QueryResponse{}, nil // Returning incorrect type for json_stacks
				}
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErrorMsg:   "Internal error processing pprof data",
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
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "invalid_type"}, // This uses the generic error path
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					// Error from Querier.QueryParca for invalid report type string before conversion attempt
					return nil, errors.New("invalid reportType: invalid_type")
				}
			},
			expectedStatusCode: http.StatusBadRequest, // As per existing handler logic for this error string
			expectedErrorMsg:   "invalid reportType: invalid_type",
		},
		{
			name: "QueryParca returns server error",
			queryParams: map[string]string{"query": "test", "start": validStartTimeStr, "end": validEndTimeStr, "reportType": "pprof"}, // Standard protobuf error path
			mockSetup: func(mq *mockParcaQuerier) {
				mq.QueryParcaFunc = func(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
					return nil, errors.New("internal Parca server error")
				}
			},
			expectedStatusCode: http.StatusInternalServerError, // Generic internal server error
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
