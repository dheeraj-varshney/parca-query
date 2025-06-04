# parca-load

This is a tool that continuously queries Parca instances for their data.

It is based on the Parca gRPC APIs defined on https://buf.build/parca-dev/parca and uses the generated connect-go code via `go.buf.build/bufbuild/connect-go/parca-dev/parca`.

### Installation

```
go install github.com/parca-dev/parca-load@latest
```

or run the container images

```
docker run -d ghcr.io/parca-dev/parca-load
```

### How it works

It runs a goroutine per API type.

It starts a `Labels` goroutine that starts querying all labels on a Parca instance and then writes these into a shared map.
The map is then read by the `Values` goroutine that selects a random label and queries all values for it.

This process it repeated every 5 seconds (configurable).
The entries of the shared map eventually expire to not query too old data.

Similarly, it starts querying `ProfileTypes` every 10 seconds (configurable) to know what profile types are available.
The result is written to a shared map.
Every 15 seconds (configurable) there are `QueryRange` requests (querying 15min and 7 day ranges) for all series.

Once the profile series are discovered above there are `Query` requests querying single profiles every 10 seconds.
For these queries it picks a random timestamp of the available time range and queries a random report type (flame graph, top table, pprof download).

Every 15 seconds (configurable) there are `Query` requests that actually request merged profiles for either 15min, or 7 days, if enough data is available for each in a series.

Metrics are collected and available on http://localhost:7171/metrics


## API Usage (`/parcaquery`)

This tool also exposes an HTTP API endpoint at `/parcaquery` (default address `127.0.0.1:7171`) that allows you to directly query the configured Parca instance and receive data in JSON format.

**Query Parameters:**

*   `query`: The Parca Query Language (PQL) string used to select the profile data. This typically consists of:
    *   A **profile type name** (e.g., `parca_agent_cpu`, `process_cpu:cpu:nanoseconds:cpu:nanoseconds:delta`). This specifies the kind of profile data you want.
    *   Optionally, **label selectors** in curly braces `{}` to filter by labels (e.g., `{job="your_app"}`, `{kubernetes_pod_name="my_pod_abc123"}`).
    *   You can combine these: `my_profile_type{label1="value1",label2="value2"}`.

    **Common Examples:**
    *   `parca_agent_cpu`: Selects the CPU profile for the Parca agent itself.
    *   `parca_agent_cpu{job="parca-load"}`: Selects CPU profiles for the `parca-load` job, if labeled as such.
    *   `{namespace="my_namespace"}`: Selects all profile types within the `my_namespace` label. (Note: this might return multiple different profile types if not further specified).
    *   `process_resident_memory_bytes`: Selects a memory profile type.
*   `start`: The start time for the query in RFC3339 format (e.g., `2023-10-26T10:00:00Z`).
*   `end`: The end time for the query in RFC3339 format (e.g., `2023-10-26T10:15:00Z`).
*   `reportType`: The type of report to generate. Supported types include:
    *   `pprof`: Returns the raw pprof protobuf data, marshalled as JSON.
    *   `flamegraph_arrow`: Returns Parca's Arrow flamegraph data, marshalled as JSON.
    *   `json_flamegraph`: Returns a custom JSON representation of an aggregated flamegraph tree.
    *   `json_stacks`: Returns a custom JSON representation of individual stack traces.

**Sample `curl` Requests:**

Replace placeholders like `YOUR_QUERY_STRING`, `RFC3339_START_TIME`, and `RFC3339_END_TIME` with actual values.

**1. Get a PPROF Report**

```bash
curl -G "http://127.0.0.1:7171/parcaquery" \
  --data-urlencode "query=YOUR_QUERY_STRING" \
  --data-urlencode "start=RFC3339_START_TIME" \
  --data-urlencode "end=RFC3339_END_TIME" \
  --data-urlencode "reportType=pprof"
```

**2. Get a Flamegraph (Custom JSON format)**

```bash
curl -G "http://127.0.0.1:7171/parcaquery" \
  --data-urlencode "query=YOUR_QUERY_STRING" \
  --data-urlencode "start=RFC3339_START_TIME" \
  --data-urlencode "end=RFC3339_END_TIME" \
  --data-urlencode "reportType=json_flamegraph"
```

**3. Get Individual Stack Traces (Custom JSON format)**

```bash
curl -G "http://127.0.0.1:7171/parcaquery" \
  --data-urlencode "query=YOUR_QUERY_STRING" \
  --data-urlencode "start=RFC3339_START_TIME" \
  --data-urlencode "end=RFC3339_END_TIME" \
  --data-urlencode "reportType=json_stacks"
```

**Example with actual values for `json_flamegraph`:**

```bash
curl -G "http://127.0.0.1:7171/parcaquery" \
  --data-urlencode "query=parca_agent_cpu:cpu:nanoseconds:cpu:nanoseconds:delta" \
  --data-urlencode "start=2023-11-01T00:00:00Z" \
  --data-urlencode "end=2023-11-01T00:05:00Z" \
  --data-urlencode "reportType=json_flamegraph"
```

**Notes:**
*   **URL Encoding**: `curl -G` with `--data-urlencode` handles URL encoding. If constructing URLs manually, ensure parameters are properly encoded.
*   **Time Range**: Ensure the `start` and `end` times cover a period where Parca has data for your query.
*   **Bearer Token**: If `parca-load` is configured to use a bearer token for Parca (via `-token` flag or Vault), it will be automatically included in requests to Parca. You do not need to add it to the `curl` request to `parca-load` itself.
