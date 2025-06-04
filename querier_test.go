package main

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	queryv1alpha1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
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
				child1 := rootNode.Children[0]
				if child1.Name != "child1" || child1.Value != 60 { // Order might vary, could make this more robust
					// Check other child if order is swapped
					child1 = rootNode.Children[1]
					if child1.Name != "child1" || child1.Value != 60 {
						t.Errorf("Unexpected child1 node data: Name=%s, Value=%d", child1.Name, child1.Value)
					}
				}
				if child1.Name == "child1" && len(child1.Children) != 1 {
                     t.Fatalf("Expected 1 grandchild for child1, got %d", len(child1.Children))
                }
				if child1.Name == "child1" && (child1.Children[0].Name != "grandchild1" || child1.Children[0].Value != 60) {
					t.Errorf("Unexpected grandchild1 node data: Name=%s, Value=%d", child1.Children[0].Name, child1.Children[0].Value)
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
				if !errors.Is(err, fmt.Errorf("%s",expectedErrorSubString)) && !strings.Contains(err.Error(), expectedErrorSubString) {
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
