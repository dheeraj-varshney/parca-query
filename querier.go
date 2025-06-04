package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/protobuf/types/known/durationpb"

	"buf.build/gen/go/parca-dev/parca/connectrpc/go/parca/query/v1alpha1/queryv1alpha1connect"
	queryv1alpha1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const numHorizontalPixelsOn8KDisplay = 7680

// Trim anything that can't be displayed by an 8K display (7680 horizontal
// pixels). This is a var because the proto request requires an address.
var nodeTrimThreshold = float32(1) / numHorizontalPixelsOn8KDisplay

type querierMetrics struct {
	labelsHistogram       *prometheus.HistogramVec
	valuesHistogram       *prometheus.HistogramVec
	profileTypesHistogram *prometheus.HistogramVec
	rangeHistogram        *prometheus.HistogramVec
	singleHistogram       *prometheus.HistogramVec
	mergeHistogram        *prometheus.HistogramVec
}

type timestampRange struct {
	first time.Time
	last  time.Time
}

type Querier struct {
	cancel context.CancelFunc
	done   chan struct{}

	metrics querierMetrics

	client queryv1alpha1connect.QueryServiceClient

	rng *rand.Rand

	labels       []string
	profileTypes []string
	series       map[string]timestampRange

	// queryTimeRanges are used by range and merge queries.
	queryTimeRanges []time.Duration
	// reportTypes are used by single and merge queries.
	reportTypes []queryv1alpha1.QueryRequest_ReportType
}

// ParcaQuerier defines the interface for querying Parca.
// This is implemented by Querier and can be mocked for testing.
type ParcaQuerier interface {
	QueryParca(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error)
}

func NewQuerier(reg *prometheus.Registry, client queryv1alpha1connect.QueryServiceClient, queryTimeRangesConf []time.Duration) *Querier {
	return &Querier{ // Must satisfy ParcaQuerier
		done: make(chan struct{}),
		metrics: querierMetrics{
			labelsHistogram: promauto.With(reg).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "parca_client_labels_seconds",
					Help:    "The seconds it takes to make Labels requests against a Parca",
					Buckets: []float64{0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1},
				},
				[]string{"grpc_code"},
			),
			valuesHistogram: promauto.With(reg).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "parca_client_values_seconds",
					Help:    "The seconds it takes to make Values requests against a Parca",
					Buckets: []float64{0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.25, 1.5, 1.75, 2, 3, 4, 5},
				},
				[]string{"grpc_code"},
			),
			profileTypesHistogram: promauto.With(reg).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "parca_client_profiletypes_seconds",
					Help:    "The seconds it takes to make ProfileTypes requests against a Parca",
					Buckets: []float64{0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 2, 3, 4, 5},
				},
				[]string{"grpc_code"},
			),
			rangeHistogram: promauto.With(reg).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "parca_client_queryrange_seconds",
					Help:    "The seconds it takes to make QueryRange requests against a Parca",
					Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.25, 1.5, 1.75, 2, 2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 9, 10},
				},
				[]string{"grpc_code", "range"},
			),
			singleHistogram: promauto.With(reg).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:        "parca_client_query_seconds",
					Help:        "The seconds it takes to make Query requests against a Parca",
					ConstLabels: map[string]string{"mode": "single"},
					Buckets:     prometheus.ExponentialBucketsRange(0.1, 120, 30),
				},
				[]string{"grpc_code", "report_type", "range"},
			),
			mergeHistogram: promauto.With(reg).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:        "parca_client_query_seconds",
					Help:        "The seconds it takes to make Query requests against a Parca",
					ConstLabels: map[string]string{"mode": "merge"},
					Buckets: []float64{
						0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
						12, 13, 14, 15, 16, 17, 18, 19, 20,
					},
				},
				[]string{"grpc_code", "report_type", "range"},
			),
		},
		client:          client,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		series:          make(map[string]timestampRange),
		queryTimeRanges: queryTimeRangesConf,
		reportTypes: []queryv1alpha1.QueryRequest_ReportType{
			queryv1alpha1.QueryRequest_REPORT_TYPE_PPROF,
			queryv1alpha1.QueryRequest_REPORT_TYPE_FLAMEGRAPH_ARROW,
			queryv1alpha1.QueryRequest_REPORT_TYPE_TABLE_ARROW,
		},
	}
}

// QueryParca fetches data from Parca based on the provided query parameters.
// It can return either a *queryv1alpha1.QueryResponse or a custom JSON structure (for json_flamegraph or json_stacks).
func (q *Querier) QueryParca(ctx context.Context, queryStr string, start time.Time, end time.Time, reportTypeStr string) (any, error) {
	var actualReportTypeForParca queryv1alpha1.QueryRequest_ReportType
	isJSONFlamegraph := false
	isJSONStacks := false

	switch reportTypeStr {
	case "pprof":
		actualReportTypeForParca = queryv1alpha1.QueryRequest_REPORT_TYPE_PPROF
	case "flamegraph_arrow":
		actualReportTypeForParca = queryv1alpha1.QueryRequest_REPORT_TYPE_FLAMEGRAPH_ARROW
	case "table_arrow":
		actualReportTypeForParca = queryv1alpha1.QueryRequest_REPORT_TYPE_TABLE_ARROW
	case "json_flamegraph":
		actualReportTypeForParca = queryv1alpha1.QueryRequest_REPORT_TYPE_FLAMEGRAPH_ARROW
		isJSONFlamegraph = true
	case "json_stacks":
		actualReportTypeForParca = queryv1alpha1.QueryRequest_REPORT_TYPE_PPROF
		isJSONStacks = true
	default:
		if reportTypeStr == "" {
			return nil, errors.New("reportType parameter is missing")
		}
		return nil, fmt.Errorf("invalid reportType: %s", reportTypeStr)
	}

	queryRequest := &queryv1alpha1.QueryRequest{
		Mode: queryv1alpha1.QueryRequest_MODE_MERGE,
		Options: &queryv1alpha1.QueryRequest_Merge{
			Merge: &queryv1alpha1.MergeProfile{
				Query: queryStr,
				Start: timestamppb.New(start),
				End:   timestamppb.New(end),
			},
		},
		ReportType:        actualReportTypeForParca,
		NodeTrimThreshold: &nodeTrimThreshold, // Using the global var from querier.go
	}

	resp, err := q.client.Query(ctx, connect.NewRequest(queryRequest))
	if err != nil {
		log.Printf("QueryParca(query=%s, reportType=%s, start=%v, end=%v): parca client query failed: %v\n", queryStr, reportTypeStr, start, end, err)
		return nil, fmt.Errorf("parca client query failed: %w", err)
	}

	log.Printf("QueryParca(query=%s, reportType=%s, start=%v, end=%v): parca client query successful\n", queryStr, reportTypeStr, start, end)

	if isJSONFlamegraph {
		// Convert the FLAMEGRAPH_ARROW response to our custom JSON format.
		return convertFlamegraphArrowToJSON(resp.Msg)
	} else if isJSONStacks {
		// Convert the PPROF response to our custom JSON stacks format.
		return convertPprofToJSONStacks(resp.Msg)
	}

	return resp.Msg, nil
}

// convertPprofToJSONStacks is a placeholder for converting Parca's
// PPROF response to a custom JSON stacks structure.
func convertPprofToJSONStacks(resp *queryv1alpha1.QueryResponse) (any, error) {
	if resp == nil || len(resp.GetPprof()) == 0 {
		return nil, errors.New("malformed or empty pprof response")
	}

	pprofBytes := resp.GetPprof()
	reader := bytes.NewReader(pprofBytes)

	parsedProfile, err := profile.ParseData(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pprof data: %w", err)
	}

	if len(parsedProfile.SampleType) == 0 {
		return nil, errors.New("pprof profile has no sample types defined")
	}
	if len(parsedProfile.Sample) == 0 {
		// Not necessarily an error, could be an empty profile. Return empty report.
		return &JSONStacksReport{
			ReportType: "json_stacks",
			Unit:       parsedProfile.SampleType[len(parsedProfile.SampleType)-1].Type, // Use last sample type as unit
			Samples:    []*JSONStacksSample{},
		}, nil
	}

	// Determine the unit and the index for the value we care about.
	// Typically, the last SampleType and corresponding value are the primary ones.
	valueTypeIndex := len(parsedProfile.SampleType) - 1
	unit := parsedProfile.SampleType[valueTypeIndex].Type

	report := &JSONStacksReport{
		ReportType: "json_stacks",
		Unit:       unit,
		Samples:    make([]*JSONStacksSample, 0, len(parsedProfile.Sample)),
	}

	for _, sample := range parsedProfile.Sample {
		if len(sample.Value) <= valueTypeIndex {
			// This sample doesn't have the expected value type, skip or error?
			// For now, log and skip.
			log.Printf("Warning: pprof sample does not have enough values for selected sample type. Sample: %+v, Expected index: %d", sample, valueTypeIndex)
			continue
		}

		jsonSample := &JSONStacksSample{
			Value:  sample.Value[valueTypeIndex],
			Labels: sample.Label, // Populate the Labels field
			Stack:  make([]*JSONStacksFrame, 0, len(sample.Location)),
		}
		report.TotalValue += jsonSample.Value

		// Pprof locations are from callee to caller. We want to reverse it for typical stack display (caller to callee).
		for i := len(sample.Location) - 1; i >= 0; i-- {
			loc := sample.Location[i]
			// A location can have multiple lines (e.g., for inlined functions).
			// We iterate through all lines, but often only the first one is most relevant for simple stacks.
			for _, line := range loc.Line {
				if line.Function == nil { // Should not happen in valid pprof
					continue
				}
				jsonFrame := &JSONStacksFrame{
					FunctionName: line.Function.Name,
					FileName:     line.Function.Filename,
					LineNumber:   line.Line,
				}
				// Add BinaryName from Mapping
				if loc.Mapping != nil && loc.Mapping.File != "" {
					jsonFrame.BinaryName = loc.Mapping.File
				}
				jsonSample.Stack = append(jsonSample.Stack, jsonFrame)
			}
		}
		report.Samples = append(report.Samples, jsonSample)
	}

	report.UniqueStackTraces = len(report.Samples) // In pprof, each Sample is typically a unique stack trace.
	return report, nil
}


// JSONStacksFrame defines the structure for a single frame in a stack trace.
type JSONStacksFrame struct {
	FunctionName string `json:"function_name"`
	FileName     string `json:"file_name"`
	LineNumber   int64  `json:"line_number"`
	BinaryName   string `json:"binary_name,omitempty"`
}

// JSONStacksSample defines a single sample with its value and stack trace.
type JSONStacksSample struct {
	Value  int64               `json:"value"`
	Labels map[string][]string `json:"labels,omitempty"`
	Stack  []*JSONStacksFrame  `json:"stack"`
}

// JSONStacksReport defines the overall structure for the JSON stacks report.
type JSONStacksReport struct {
	ReportType        string              `json:"report_type"`
	Unit              string              `json:"unit"`
	UniqueStackTraces int                 `json:"unique_stack_traces"`
	TotalValue        int64               `json:"total_value"`
	Samples           []*JSONStacksSample `json:"samples"`
}


// FlamegraphNode defines the structure for a node in the JSON flamegraph.
type FlamegraphNode struct {
	Name       string            `json:"name"`
	SystemName string            `json:"system_name,omitempty"`
	Filename   string            `json:"filename,omitempty"`
	StartLine  int32             `json:"start_line,omitempty"`
	Value      uint64            `json:"value"`
	Children   []*FlamegraphNode `json:"children"`
}

// convertFlamegraphArrowToJSON converts Parca's FLAMEGRAPH_ARROW response
// (an Arrow Record) into a custom JSON tree structure (*FlamegraphNode).
func convertFlamegraphArrowToJSON(resp *queryv1alpha1.QueryResponse) (any, error) {
	if resp == nil || resp.GetFlamegraphArrow() == nil || len(resp.GetFlamegraphArrow().GetRecord()) == 0 {
		return nil, errors.New("malformed or empty flamegraph arrow response")
	}

	recordBytes := resp.GetFlamegraphArrow().GetRecord()
	bytesReader := bytes.NewReader(recordBytes)

	// Create an Arrow IPC reader.
	// Note: Assuming a version of Apache Arrow, e.g., v12.
	// Actual version might need adjustment based on go.mod or transitive dependencies.
	ipcReader, err := ipc.NewReader(bytesReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow ipc reader: %w", err)
	}
	defer ipcReader.Release()

	if !ipcReader.Next() {
		if ipcReader.Err() != nil {
			return nil, fmt.Errorf("ipc reader error on Next(): %w", ipcReader.Err())
		}
		return nil, errors.New("no records found in arrow ipc stream")
	}
	record := ipcReader.Record()
	record.Retain() // Retain the record as ipcReader will release it.
	defer record.Release()

	// Schema and column indices (these are based on common Parca flamegraph schemas)
	// We need to find these dynamically or assume fixed positions/names if stable.
	// For robustness, finding by name is better.
	schema := record.Schema()
	getColumnIndex := func(name string) int {
		indices := schema.FieldIndices(name)
		if len(indices) == 0 {
			return -1 // Column not found
		}
		return indices[0]
	}

	// Expected column names (adjust if Parca's schema differs)
	// These are educated guesses based on Parca's typical Arrow structure.
	// Parca often uses: location_id, parent_id, value, diff, function_name, function_filename, line_start
	// The 'label' column is often a string representation of the function.
	// For simplicity, we'll try to use function_name as the primary 'Name'.

	idxLocationID := getColumnIndex("location_id") // uint64
	idxParentID := getColumnIndex("parent_id")     // uint64 (0 for root's parent)
	idxValue := getColumnIndex("value")            // int64 or uint64 (cumulative value)
	// idxDiff := getColumnIndex("diff") // int64 or uint64 (self value, not directly used in this basic node)

	// Function details (might be nested or prefixed, e.g., "function.name")
	// Assuming flat column names for now based on common Parca usage.
	idxFunctionName := getColumnIndex("function_name")         // string
	idxFunctionSystemName := getColumnIndex("function_system_name") // string
	idxFunctionFilename := getColumnIndex("function_filename")     // string
	idxFunctionStartLine := getColumnIndex("function_start_line") // int64 or int32

	// Validate required columns are present
	if idxLocationID == -1 || idxParentID == -1 || idxValue == -1 || idxFunctionName == -1 {
		return nil, fmt.Errorf("missing essential columns in arrow record (location_id, parent_id, value, function_name). Schema: %s", schema.String())
	}

	nodes := make(map[uint64]*FlamegraphNode) // Map location_id to FlamegraphNode
	var rootNodes []*FlamegraphNode           // Could be multiple roots, but typically one for flamegraphs

	// First pass: Create all nodes
	for i := 0; i < int(record.NumRows()); i++ {
		locationID := record.Column(idxLocationID).(array.Uint64Array).Value(i)

		nodeValue := uint64(0)
		switch arr := record.Column(idxValue).(type) {
		case *array.Int64:
			val := arr.Value(i)
			if val < 0 { val = 0 } // Value should be non-negative
			nodeValue = uint64(val)
		case *array.Uint64:
			nodeValue = arr.Value(i)
		default:
			return nil, fmt.Errorf("unexpected type for 'value' column: %T", record.Column(idxValue))
		}


		node := &FlamegraphNode{
			Value:    nodeValue,
			Children: []*FlamegraphNode{},
		}

		// Function Name (Primary Name)
		if idxFunctionName != -1 && !record.Column(idxFunctionName).IsNull(i) {
			node.Name = record.Column(idxFunctionName).(*array.String).Value(i)
		} else {
			node.Name = "[unknown]" // Fallback name
		}

		if idxFunctionSystemName != -1 && !record.Column(idxFunctionSystemName).IsNull(i) {
			node.SystemName = record.Column(idxFunctionSystemName).(*array.String).Value(i)
		}
		if idxFunctionFilename != -1 && !record.Column(idxFunctionFilename).IsNull(i) {
			node.Filename = record.Column(idxFunctionFilename).(*array.String).Value(i)
		}
		if idxFunctionStartLine != -1 && !record.Column(idxFunctionStartLine).IsNull(i) {
			// Assuming int64 from Arrow, converting to int32 for struct
			switch arr := record.Column(idxFunctionStartLine).(type) {
			case *array.Int32:
				node.StartLine = arr.Value(i)
			case *array.Int64:
				node.StartLine = int32(arr.Value(i))
			default:
				// In case of unexpected type, do not set or log warning
			}
		}
		nodes[locationID] = node
	}

	// Second pass: Link children to parents
	var actualRoot *FlamegraphNode = nil
	potentialRoots := 0

	for i := 0; i < int(record.NumRows()); i++ {
		locationID := record.Column(idxLocationID).(array.Uint64Array).Value(i)
		parentID := record.Column(idxParentID).(array.Uint64Array).Value(i)

		currentNode, ok := nodes[locationID]
		if !ok {
			// Should not happen if first pass was correct
			return nil, fmt.Errorf("node with location_id %d not found in map", locationID)
		}

		if parentID == 0 || parentID == locationID { // Common root indicators in Parca
			// In Parca, the root node often has parent_id == location_id or parent_id == 0.
			// If there are multiple such nodes, this logic might need refinement,
			// but typically there's one clear root, or a "fake" root whose value is sum of children.
			// We assume the node with parentID == 0 (or self-reference if that's the convention) is the true root.
			// If multiple true roots are found, it's an issue.
			if actualRoot == nil && parentID == 0 { // Prefer parentID == 0 as root
			    actualRoot = currentNode
			    potentialRoots++
			} else if actualRoot == nil && parentID == locationID { // Fallback if no parentID == 0 found yet
				actualRoot = currentNode
				potentialRoots++
			} else if (parentID == 0 || parentID == locationID) && currentNode != actualRoot {
				// If we find another root, this is ambiguous.
				// For now, we'll stick with the first one found.
				// This could happen if there's a dummy root node and then actual roots.
				// Parca's flamegraph usually has one dominant root.
				log.Printf("Warning: Multiple potential root nodes detected (location_id: %d, parent_id: %d). Using first one found.", locationID, parentID)
			}
			// Add to rootNodes for now, will pick one later or ensure there's only one significant one.
			rootNodes = append(rootNodes, currentNode)


		} else {
			parentNode, ok := nodes[parentID]
			if ok {
				parentNode.Children = append(parentNode.Children, currentNode)
			} else {
				// This might indicate a broken tree or a node whose parent is not in this record (should not happen for flamegraphs)
				log.Printf("Warning: Parent node with id %d not found for child %d. Attaching to a default root or ignoring.", parentID, locationID)
				// For now, let's add it as a root if its parent is missing, though this is usually not expected.
                // Or, create a dummy root if actualRoot is still nil after this loop.
                rootNodes = append(rootNodes, currentNode)

			}
		}
	}

	// Determine the final root to return
    if actualRoot != nil {
        // Prune rootNodes to only contain the actualRoot if it was identified
        // This handles cases where other nodes might have also seemed like roots initially
        finalRoots := []*FlamegraphNode{}
        for _, r := range rootNodes {
            isChild := false
            for _, n := range nodes {
                for _, child := range n.Children {
                    if child == r {
                        isChild = true
                        break
                    }
                }
                if isChild { break }
            }
            if !isChild {
                finalRoots = append(finalRoots,r)
            }
        }
        if len(finalRoots) == 1 {
            actualRoot = finalRoots[0]
        } else if len(finalRoots) > 1 {
             // If multiple true roots, this is complex. Default to the one with largest value or first one.
             log.Printf("Warning: Multiple true root nodes identified (%d). Selecting the one with highest value or first.", len(finalRoots))
             // Simple heuristic: pick the one with the largest value, or the first one.
             // This part might need more sophisticated handling depending on Parca's exact output for complex cases.
             if actualRoot == nil && len(finalRoots) > 0 { actualRoot = finalRoots[0] } // Default to first if actualRoot wasn't set
             for _, r := range finalRoots {
                 if r.Value > actualRoot.Value {
                     actualRoot = r
                 }
             }
        } else if len(finalRoots) == 0 && len(nodes) > 0 { // All nodes are part of some tree, but no clear single root.
            return nil, errors.New("failed to identify a clear root node, but nodes exist")
        }
    }


	if actualRoot == nil && len(nodes) > 0 {
        // If no root was identified by parent_id == 0 or self-reference,
        // it could be that the root is the node with no parent pointing to it from *other* nodes,
        // or the dataset is malformed.
        // A simple fallback: if there's only one node, it's the root.
        // Or, if there are multiple "rootCandidates" (nodes not parented by others in the list),
        // we might need a dummy root or error out.
        if len(rootNodes) == 1 {
            actualRoot = rootNodes[0]
        } else if len(rootNodes) > 1 {
            // This indicates multiple disconnected trees or multiple nodes claiming to be roots.
            // Create a synthetic root? For now, error or pick one.
            // Let's pick the one with the highest value, as a heuristic.
            log.Printf("Multiple root candidates (%d) found. Picking one with max value.", len(rootNodes))
            for _, rNode := range rootNodes {
                if actualRoot == nil || rNode.Value > actualRoot.Value {
                    actualRoot = rNode
                }
            }
             if actualRoot == nil { // If all values were 0 or rootNodes was empty
                 return nil, errors.New("multiple root candidates, but unable to pick one")
             }
        } else {
		    return nil, errors.New("no root node identified in flamegraph")
        }
	} else if actualRoot == nil && len(nodes) == 0 {
		return nil, errors.New("no nodes found in flamegraph data")
	}

	return actualRoot, nil
}


func (q *Querier) Run(ctx context.Context, interval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel

	defer close(q.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	run := func() {
		// Since a lot of these queries are dependent on other queries, run
		// them one at a time.
		q.queryLabels(ctx, interval)
		q.queryValues(ctx, interval)
		q.queryProfileTypes(ctx, interval)
		q.queryRange(ctx)
		q.querySingle(ctx)
		q.queryMerge(ctx)
	}

	// Immediately run and then wait for the ticker.
	// If we don't run immediately, we'll have to wait for the first tick to run which can be a long time e.g. 30min.
	run()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (q *Querier) Stop() {
	q.cancel()
	<-q.done
}

func (q *Querier) queryLabels(ctx context.Context, interval time.Duration) {
	var resp *connect.Response[queryv1alpha1.LabelsResponse]
	var count int
	operation := func() (err error) {
		defer func() { count++ }()

		queryStart := time.Now()
		resp, err = q.client.Labels(ctx, connect.NewRequest(&queryv1alpha1.LabelsRequest{}))
		latency := time.Since(queryStart)
		if err != nil {
			q.metrics.labelsHistogram.WithLabelValues(connect.CodeOf(err).String()).Observe(latency.Seconds())
			log.Println("labels: failed to make request", count, err)
			return
		}
		q.metrics.labelsHistogram.WithLabelValues(grpcCodeOK).Observe(latency.Seconds())
		q.labels = append(q.labels[:0], resp.Msg.LabelNames...)
		log.Printf("labels: took %v and got %d results\n", latency, len(resp.Msg.LabelNames))

		return nil
	}

	exp := backoff.NewExponentialBackOff()
	exp.MaxElapsedTime = interval
	if err := backoff.Retry(operation, backoff.WithContext(exp, ctx)); err != nil {
		return
	}
}

func (q *Querier) queryValues(ctx context.Context, interval time.Duration) {
	if len(q.labels) == 0 {
		log.Println("values: no labels to query")
		return
	}
	label := q.labels[q.rng.Intn(len(q.labels))]

	var resp *connect.Response[queryv1alpha1.ValuesResponse]
	var count int
	operation := func() (err error) {
		defer func() { count++ }()
		queryStart := time.Now()
		resp, err = q.client.Values(ctx, connect.NewRequest(&queryv1alpha1.ValuesRequest{
			LabelName: label,
			Match:     nil,
			Start:     timestamppb.New(time.Now().Add(-1 * time.Hour)),
			End:       timestamppb.New(time.Now()),
		}))
		latency := time.Since(queryStart)
		if err != nil {
			q.metrics.valuesHistogram.WithLabelValues(connect.CodeOf(err).String()).Observe(latency.Seconds())
			log.Printf("values(label=%s): failed to make request %d: %v\n", label, count, err)
			return
		}
		q.metrics.valuesHistogram.WithLabelValues(grpcCodeOK).Observe(latency.Seconds())
		log.Printf("values(label=%s): took %v and got %d results\n", label, latency, len(resp.Msg.LabelValues))

		return nil
	}

	exp := backoff.NewExponentialBackOff()
	exp.MaxElapsedTime = interval
	if err := backoff.Retry(operation, backoff.WithContext(exp, ctx)); err != nil {
		return
	}
}

func (q *Querier) queryProfileTypes(ctx context.Context, interval time.Duration) {
	var resp *connect.Response[queryv1alpha1.ProfileTypesResponse]
	var count int
	operation := func() (err error) {
		defer func() { count++ }()

		queryStart := time.Now()
		resp, err = q.client.ProfileTypes(ctx, connect.NewRequest(&queryv1alpha1.ProfileTypesRequest{}))
		latency := time.Since(queryStart)
		if err != nil {
			q.metrics.profileTypesHistogram.WithLabelValues(connect.CodeOf(err).String()).Observe(latency.Seconds())
			log.Println("profile types: failed to make request", count, err)
			return err
		}
		q.metrics.profileTypesHistogram.WithLabelValues(grpcCodeOK).Observe(latency.Seconds())
		log.Printf("profile types: took %v and got %d types\n", latency, len(resp.Msg.Types))

		return nil
	}

	exp := backoff.NewExponentialBackOff()
	exp.MaxElapsedTime = interval
	if err := backoff.Retry(operation, backoff.WithContext(exp, ctx)); err != nil {
		return
	}

	if len(resp.Msg.Types) == 0 {
		return
	}

	q.profileTypes = q.profileTypes[:0]
	for _, pt := range resp.Msg.Types {
		key := fmt.Sprintf("%s:%s:%s:%s:%s", pt.Name, pt.SampleType, pt.SampleUnit, pt.PeriodType, pt.PeriodUnit)
		if pt.Delta {
			key += ":delta"
		}
		q.profileTypes = append(q.profileTypes, key)
	}
}

func (q *Querier) queryRange(ctx context.Context) {
	if len(q.profileTypes) == 0 {
		log.Println("range: no profile types to query")
		return
	}

	profileType := q.profileTypes[q.rng.Intn(len(q.profileTypes))]

	for _, tr := range q.queryTimeRanges {
		rangeEnd := time.Now()
		rangeStart := rangeEnd.Add(-1 * tr)

		queryStart := time.Now()
		resp, err := q.client.QueryRange(ctx, connect.NewRequest(&queryv1alpha1.QueryRangeRequest{
			Query: profileType,
			Start: timestamppb.New(rangeStart),
			End:   timestamppb.New(rangeEnd),
			Step:  durationpb.New(time.Duration(tr.Nanoseconds() / numHorizontalPixelsOn8KDisplay)),
		}))
		latency := time.Since(queryStart)
		if err != nil {
			q.metrics.rangeHistogram.WithLabelValues(
				connect.CodeOf(err).String(), tr.String(),
			).Observe(latency.Seconds())
			log.Printf("range(type=%s,over=%s): failed to make request: %v\n", profileType, tr, err)
			continue
		}

		q.metrics.rangeHistogram.WithLabelValues(grpcCodeOK, tr.String()).Observe(latency.Seconds())
		log.Printf(
			"range(type=%s,over=%s): took %s and got %d series\n",
			profileType, tr, latency, len(resp.Msg.Series),
		)

		for k := range q.series {
			delete(q.series, k)
		}
		for _, series := range resp.Msg.Series {
			if len(series.Samples) < 2 {
				continue
			}
			lSet := labels.Labels{}
			for _, l := range series.Labelset.Labels {
				lSet = append(lSet, labels.Label{Name: l.Name, Value: l.Value})
			}
			sort.Sort(lSet)
			q.series[profileType+lSet.String()] = timestampRange{
				first: series.Samples[0].Timestamp.AsTime(),
				last:  series.Samples[len(series.Samples)-1].Timestamp.AsTime(),
			}
		}
	}
}

func (q *Querier) querySingle(ctx context.Context) {
	if len(q.series) == 0 {
		log.Println("single: no series to query")
		return
	}
	seriesKeys := maps.Keys(q.series)
	series := seriesKeys[q.rng.Intn(len(seriesKeys))]
	sampleTs := q.series[series].first

	for _, reportType := range q.reportTypes {
		queryStart := time.Now()
		_, err := q.client.Query(ctx, connect.NewRequest(&queryv1alpha1.QueryRequest{
			Mode: queryv1alpha1.QueryRequest_MODE_SINGLE_UNSPECIFIED,
			Options: &queryv1alpha1.QueryRequest_Single{
				Single: &queryv1alpha1.SingleProfile{
					Query: series,
					Time:  timestamppb.New(sampleTs),
				},
			},
			ReportType: reportType,
		}))
		latency := time.Since(queryStart)
		if err != nil {
			q.metrics.singleHistogram.WithLabelValues(
				connect.CodeOf(err).String(),
				reportType.String(),
				"",
			).Observe(latency.Seconds())
			log.Printf("single(series=%s, reportType=%s): failed to make request: %v\n", series, reportType, err)
			continue
		}

		q.metrics.singleHistogram.WithLabelValues(
			grpcCodeOK,
			reportType.String(),
			"",
		).Observe(latency.Seconds())
		log.Printf("single(series=%s, reportType=%s): took %v\n", series, reportType, latency)
	}
}

func (q *Querier) queryMerge(ctx context.Context) {
	if len(q.profileTypes) == 0 {
		log.Println("merge: no profile types to query")
		return
	}

	profileType := q.profileTypes[q.rng.Intn(len(q.profileTypes))]
	for _, tr := range q.queryTimeRanges {
		rangeEnd := time.Now()
		rangeStart := rangeEnd.Add(-1 * tr)

		for _, reportType := range q.reportTypes {
			queryStart := time.Now()
			_, err := q.client.Query(ctx, connect.NewRequest(&queryv1alpha1.QueryRequest{
				Mode: queryv1alpha1.QueryRequest_MODE_MERGE,
				Options: &queryv1alpha1.QueryRequest_Merge{
					Merge: &queryv1alpha1.MergeProfile{
						Query: profileType,
						Start: timestamppb.New(rangeStart),
						End:   timestamppb.New(rangeEnd),
					},
				},
				ReportType:        reportType,
				NodeTrimThreshold: &nodeTrimThreshold,
			}))
			latency := time.Since(queryStart)
			if err != nil {
				q.metrics.mergeHistogram.WithLabelValues(
					connect.CodeOf(err).String(),
					reportType.String(),
					tr.String(),
				).Observe(latency.Seconds())

				log.Printf(
					"merge(type=%s,reportType=%s,over=%s): failed to make request: %v\n",
					profileType, reportType, tr, err,
				)
				continue
			}

			q.metrics.mergeHistogram.WithLabelValues(
				grpcCodeOK,
				reportType.String(),
				tr.String(),
			).Observe(latency.Seconds())

			log.Printf(
				"merge(type=%s,reportType=%s,over=%s): took %s\n",
				profileType, reportType, tr, latency,
			)
		}
	}
}
