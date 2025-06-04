package main

import (
	"bytes"
	"errors"
	"fmt"
	"strings" // Added for pprof tests
	"testing"

	queryv1alpha1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/google/pprof/profile" // Added for pprof tests
)

// Helper function to create a sample Arrow record for flamegraphs
// This is a simplified version. A real one would have more columns and potentially more complex data.
func createSampleFlamegraphArrowBytes(t *testing.T, nodes []map[string]any) []byte {
	t.Helper()

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "location_id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "parent_id", Type: arrow.PrimitiveTypes.Uint64}, // 0 for root's parent
			{Name: "function_name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Int64}, // Parca might use Uint64, ensure consistency
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	locationIDBuilder := builder.Field(0).(*array.Uint64Builder)
	parentIDBuilder := builder.Field(1).(*array.Uint64Builder)
	nameBuilder := builder.Field(2).(*array.StringBuilder)
	valueBuilder := builder.Field(3).(*array.Int64Builder)

	for _, node := range nodes {
		locationIDBuilder.Append(node["location_id"].(uint64))
		parentIDBuilder.Append(node["parent_id"].(uint64))
		nameBuilder.Append(node["function_name"].(string))
		valueBuilder.Append(node["value"].(int64))
	}

	record := builder.NewRecord()
	defer record.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		t.Fatalf("Failed to write arrow record: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close arrow writer: %v", err)
	}

	return buf.Bytes()
}

func TestConvertFlamegraphArrowToJSON(t *testing.T) {
	tests := []struct {
		name          string
		setupResponse func(t *testing.T) *queryv1alpha1.QueryResponse
		validate      func(t *testing.T, result any, err error)
	}{
		{
			name: "valid simple flamegraph",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				arrowData := createSampleFlamegraphArrowBytes(t, []map[string]any{
					{"location_id": uint64(1), "parent_id": uint64(0), "function_name": "root", "value": int64(100)},
					{"location_id": uint64(2), "parent_id": uint64(1), "function_name": "child1", "value": int64(60)},
					{"location_id": uint64(3), "parent_id": uint64(1), "function_name": "child2", "value": int64(40)},
					{"location_id": uint64(4), "parent_id": uint64(2), "function_name": "grandchild1", "value": int64(60)},
				})
				return &queryv1alpha1.QueryResponse{
					Report: &queryv1alpha1.QueryResponse_FlamegraphArrow{
						FlamegraphArrow: &queryv1alpha1.FlamegraphArrow{Record: arrowData, Unit: "samples"},
					},
				}
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				rootNode, ok := result.(*FlamegraphNode)
				if !ok {
					t.Fatalf("Expected result type *FlamegraphNode, got %T", result)
				}
				if rootNode.Name != "root" || rootNode.Value != 100 {
					t.Errorf("Unexpected root node data: Name=%s, Value=%d", rootNode.Name, rootNode.Value)
				}
				if len(rootNode.Children) != 2 {
					t.Fatalf("Expected 2 children for root, got %d", len(rootNode.Children))
				}
				// Child1
				child1Found := false
				grandChildFound := false
				for _, child := range rootNode.Children {
					if child.Name == "child1" && child.Value == 60 {
						child1Found = true
						if len(child.Children) != 1 {
							t.Fatalf("Expected 1 grandchild for child1, got %d", len(child.Children))
						}
						if child.Children[0].Name == "grandchild1" && child.Children[0].Value == 60 {
							grandChildFound = true
						}
					}
				}
				if !child1Found {
					t.Error("Child1 not found or has incorrect value")
				}
				if !grandChildFound {
					t.Error("Grandchild1 not found or has incorrect value")
				}
			},
		},
		{
			name: "empty arrow record",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				return &queryv1alpha1.QueryResponse{
					Report: &queryv1alpha1.QueryResponse_FlamegraphArrow{
						FlamegraphArrow: &queryv1alpha1.FlamegraphArrow{Record: []byte{}, Unit: "samples"},
					},
				}
			},
			validate: func(t *testing.T, result any, err error) {
				if err == nil {
					t.Fatal("Expected error for empty arrow record, got nil")
				}
				expectedErrorMsg := "malformed or empty flamegraph arrow response"
				if err.Error() != expectedErrorMsg {
					t.Errorf("Expected error message '%s', got '%s'", expectedErrorMsg, err.Error())
				}
			},
		},
		{
			name: "nil flamegraph arrow in response",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				return &queryv1alpha1.QueryResponse{
					Report: &queryv1alpha1.QueryResponse_FlamegraphArrow{ FlamegraphArrow: nil },
				}
			},
			validate: func(t *testing.T, result any, err error) {
				if err == nil {
					t.Fatal("Expected error for nil flamegraph arrow, got nil")
				}
				expectedErrorMsg := "malformed or empty flamegraph arrow response"
				 if err.Error() != expectedErrorMsg {
					t.Errorf("Expected error message '%s', got '%s'", expectedErrorMsg, err.Error())
				}
			},
		},
		{
			name: "missing essential column (value)",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				pool := memory.NewGoAllocator()
				// Schema without 'value' column
				schema := arrow.NewSchema(
					[]arrow.Field{
						{Name: "location_id", Type: arrow.PrimitiveTypes.Uint64},
						{Name: "parent_id", Type: arrow.PrimitiveTypes.Uint64},
						{Name: "function_name", Type: arrow.BinaryTypes.String},
					}, nil,
				)
				builder := array.NewRecordBuilder(pool, schema)
				defer builder.Release()
				// Add one row of data
				builder.Field(0).(*array.Uint64Builder).Append(1)
				builder.Field(1).(*array.Uint64Builder).Append(0)
				builder.Field(2).(*array.StringBuilder).Append("root")

				record := builder.NewRecord(); defer record.Release()
				var buf bytes.Buffer
				writer := ipc.NewWriter(&buf, ipc.WithSchema(schema)); defer writer.Close()
				_ = writer.Write(record); _ = writer.Close()

				return &queryv1alpha1.QueryResponse{
					Report: &queryv1alpha1.QueryResponse_FlamegraphArrow{
						FlamegraphArrow: &queryv1alpha1.FlamegraphArrow{Record: buf.Bytes()},
					},
				}
			},
			validate: func(t *testing.T, result any, err error) {
				if err == nil {
					t.Fatalf("Expected error due to missing 'value' column, got nil")
				}
				expectedErrorSubString := "missing essential columns in arrow record"
				if !strings.Contains(err.Error(), expectedErrorSubString) { // errors.Is might not work for wrapped errors without more setup
					t.Errorf("Error message '%s' does not contain expected substring '%s'", err.Error(), expectedErrorSubString)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := tc.setupResponse(t)
			result, err := convertFlamegraphArrowToJSON(resp)
			tc.validate(t, result, err)
		})
	}
}

// --- Tests for PPROF to JSON Stacks Conversion ---

// Helper function to create sample pprof data bytes
func createSamplePprofBytes(t *testing.T, sampleTypes []*profile.ValueType, samples []*profile.Sample) []byte {
	t.Helper()
	prof := &profile.Profile{
		SampleType: sampleTypes,
		Sample:     samples,
		Location:   []*profile.Location{}, // Will be populated from samples
		Function:   []*profile.Function{}, // Will be populated from samples
		Mapping:    []*profile.Mapping{},  // Will be populated from samples
	}

	// Populate Location, Function, Mapping stores from samples to create a valid profile
	locationMap := make(map[*profile.Location]bool)
	functionMap := make(map[*profile.Function]bool)
	mappingMap := make(map[*profile.Mapping]bool)

	for _, s := range samples {
		for _, loc := range s.Location {
			if !locationMap[loc] {
				prof.Location = append(prof.Location, loc)
				locationMap[loc] = true
			}
			for _, line := range loc.Line {
				if !functionMap[line.Function] {
					prof.Function = append(prof.Function, line.Function)
					functionMap[line.Function] = true
				}
			}
			if loc.Mapping != nil && !mappingMap[loc.Mapping] {
				prof.Mapping = append(prof.Mapping, loc.Mapping)
				mappingMap[loc.Mapping] = true
			}
		}
	}


	var buf bytes.Buffer
	if err := prof.Write(&buf); err != nil {
		t.Fatalf("Failed to write pprof profile: %v", err)
	}
	return buf.Bytes()
}

func TestConvertPprofToJSONStacks(t *testing.T) {
	// Reusable sample function, location, mapping
	fn1 := &profile.Function{ID: 1, Name: "main.work", SystemName: "main.work", Filename: "main.go"}
	fn2 := &profile.Function{ID: 2, Name: "runtime.schedule", SystemName: "runtime.schedule", Filename: "runtime.go"}

	m1 := &profile.Mapping{ID: 1, File: "/bin/parca-load", HasFunctions: true}
	m2 := &profile.Mapping{ID: 2, File: "/lib/ld.so", HasFunctions: true}


	loc1 := &profile.Location{ID: 1, Mapping: m1, Address: 0x1000, Line: []profile.Line{{Function: fn1, Line: 50}}}
	loc2 := &profile.Location{ID: 2, Mapping: m2, Address: 0x2000, Line: []profile.Line{{Function: fn2, Line: 100}}}
	loc3WithoutMapping := &profile.Location{ID: 3, Address: 0x3000, Line: []profile.Line{{Function: fn1, Line: 55}}} // No mapping

	defaultSampleTypes := []*profile.ValueType{
		{Type: "samples", Unit: "count"},
		{Type: "cpu", Unit: "nanoseconds"},
	}

	tests := []struct {
		name          string
		setupResponse func(t *testing.T) *queryv1alpha1.QueryResponse
		validate      func(t *testing.T, result any, err error)
	}{
		{
			name: "valid pprof with binary name",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				samples := []*profile.Sample{
					{Location: []*profile.Location{loc1, loc2}, Value: []int64{1, 100}}, // Stack: loc2 (caller) -> loc1 (callee)
				}
				pprofBytes := createSamplePprofBytes(t, defaultSampleTypes, samples)
				return &queryv1alpha1.QueryResponse{Report: &queryv1alpha1.QueryResponse_Pprof{Pprof: pprofBytes}}
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				report, ok := result.(*JSONStacksReport)
				if !ok {
					t.Fatalf("Expected result type *JSONStacksReport, got %T", result)
				}
				if report.Unit != "nanoseconds" {
					t.Errorf("Expected unit 'nanoseconds', got '%s'", report.Unit)
				}
				if report.TotalValue != 100 {
					t.Errorf("Expected TotalValue 100, got %d", report.TotalValue)
				}
				if report.UniqueStackTraces != 1 {
					t.Errorf("Expected UniqueStackTraces 1, got %d", report.UniqueStackTraces)
				}
				if len(report.Samples) != 1 {
					t.Fatalf("Expected 1 sample, got %d", len(report.Samples))
				}
				sample := report.Samples[0]
				if len(sample.Stack) != 2 {
					t.Fatalf("Expected stack depth 2, got %d", len(sample.Stack))
				}
				// Stack is reversed by convertPprofToJSONStacks: caller first
				if sample.Stack[0].FunctionName != fn2.Name || sample.Stack[0].BinaryName != m2.File { // loc2 data
					t.Errorf("Unexpected frame 0: %+v. Expected Func: %s, Bin: %s", sample.Stack[0], fn2.Name, m2.File)
				}
				if sample.Stack[1].FunctionName != fn1.Name || sample.Stack[1].BinaryName != m1.File { // loc1 data
					t.Errorf("Unexpected frame 1: %+v. Expected Func: %s, Bin: %s", sample.Stack[1], fn1.Name, m1.File)
				}
			},
		},
		{
			name: "pprof with location missing mapping",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				samples := []*profile.Sample{ // Stack: loc1 -> loc3WithoutMapping
					{Location: []*profile.Location{loc3WithoutMapping, loc1}, Value: []int64{2, 200}},
				}
				pprofBytes := createSamplePprofBytes(t, defaultSampleTypes, samples)
				return &queryv1alpha1.QueryResponse{Report: &queryv1alpha1.QueryResponse_Pprof{Pprof: pprofBytes}}
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				report, _ := result.(*JSONStacksReport)
				if len(report.Samples[0].Stack) != 2 {
					t.Fatalf("Expected stack depth 2, got %d", len(report.Samples[0].Stack))
				}
				// Stack is reversed: loc1 (caller) then loc3WithoutMapping (callee)
				if report.Samples[0].Stack[0].FunctionName != fn1.Name || report.Samples[0].Stack[0].BinaryName != m1.File {
					t.Errorf("Expected frame 0 (loc1) to have binary name '%s', got: %+v", m1.File, report.Samples[0].Stack[0])
				}
				if report.Samples[0].Stack[1].FunctionName != fn1.Name || report.Samples[0].Stack[1].BinaryName != "" {
					t.Errorf("Expected frame 1 (loc3WithoutMapping) to have no binary name, got: %+v", report.Samples[0].Stack[1])
				}
			},
		},
		{
			name: "empty pprof data (nil bytes)",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				return &queryv1alpha1.QueryResponse{Report: &queryv1alpha1.QueryResponse_Pprof{Pprof: nil}}
			},
			validate: func(t *testing.T, result any, err error) {
				if err == nil || !strings.Contains(err.Error(), "malformed or empty pprof response") {
					t.Errorf("Expected 'malformed or empty pprof response' error, got %v", err)
				}
			},
		},
		{
			name: "pprof with no samples",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				pprofBytes := createSamplePprofBytes(t, defaultSampleTypes, []*profile.Sample{})
				return &queryv1alpha1.QueryResponse{Report: &queryv1alpha1.QueryResponse_Pprof{Pprof: pprofBytes}}
			},
			validate: func(t *testing.T, result any, err error) {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				report, _ := result.(*JSONStacksReport)
				if report.Unit != "nanoseconds" || report.TotalValue != 0 || report.UniqueStackTraces != 0 || len(report.Samples) != 0 {
					t.Errorf("Expected empty report, got %+v", report)
				}
			},
		},
		{
			name: "pprof with no sample types",
			setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
				samples := []*profile.Sample{{Location: []*profile.Location{loc1}, Value: []int64{100}}}
				pprofBytes := createSamplePprofBytes(t, []*profile.ValueType{}, samples) // Empty sample types
				return &queryv1alpha1.QueryResponse{Report: &queryv1alpha1.QueryResponse_Pprof{Pprof: pprofBytes}}
			},
			validate: func(t *testing.T, result any, err error) {
				if err == nil || !strings.Contains(err.Error(), "pprof profile has no sample types defined") {
					t.Errorf("Expected 'no sample types defined' error, got %v", err)
				}
			},
		},
		{
            name: "pprof with single sample type",
            setupResponse: func(t *testing.T) *queryv1alpha1.QueryResponse {
                sampleTypes := []*profile.ValueType{{Type: "goroutine", Unit: "count"}}
                samples := []*profile.Sample{
                    {Location: []*profile.Location{loc1}, Value: []int64{5}},
                }
                pprofBytes := createSamplePprofBytes(t, sampleTypes, samples)
                return &queryv1alpha1.QueryResponse{Report: &queryv1alpha1.QueryResponse_Pprof{Pprof: pprofBytes}}
            },
            validate: func(t *testing.T, result any, err error) {
                if err != nil {
                    t.Fatalf("Expected no error, got %v", err)
                }
                report, _ := result.(*JSONStacksReport)
                if report.Unit != "count" {
                    t.Errorf("Expected unit 'count', got '%s'", report.Unit)
                }
                if report.TotalValue != 5 {
                    t.Errorf("Expected TotalValue 5, got %d", report.TotalValue)
                }
            },
        },
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := tc.setupResponse(t)
			result, err := convertPprofToJSONStacks(resp)
			tc.validate(t, result, err)
		})
	}
}
