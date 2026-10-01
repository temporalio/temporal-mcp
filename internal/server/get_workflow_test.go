package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	"github.com/temporalio/operator-mcp/internal/config"
)

func TestGetWorkflowDeclaresOnlyItsRPCs(t *testing.T) {
	want := []string{
		workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
		workflowservice.WorkflowService_GetWorkflowExecutionHistory_FullMethodName,
	}
	for _, tl := range tools(&config.Config{}, nil) {
		if tl.name == "get_workflow" {
			if !slices.Equal(tl.rpcs, want) {
				t.Errorf("rpcs = %v, want %v", tl.rpcs, want)
			}
			return
		}
	}
	t.Fatal("get_workflow is not registered")
}

func describeWithSearchAttributes(fields map[string]*commonpb.Payload) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: fields},
		},
	}
}

func typedPayload(t *testing.T, value any, typ string) *commonpb.Payload {
	t.Helper()
	p, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		t.Fatal(err)
	}
	if typ != "" {
		p.Metadata["type"] = []byte(typ)
	}
	return p
}

func TestDescribedRunDecodesSearchAttributesByType(t *testing.T) {
	tests := []struct {
		typ   string
		value any
		want  string
	}{
		{"Int", int64(9007199254740993), `9007199254740993`},
		{"Double", 1.5, `1.5`},
		{"Bool", true, `true`},
		{"Keyword", "k", `"k"`},
		{"Text", "some text", `"some text"`},
		{"Datetime", time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC), `"2026-03-04T05:06:07Z"`},
		{"KeywordList", []string{"a", "b"}, `["a","b"]`},
		{"", "untyped", `"untyped"`},
		{"Unknown", 7, `7`},
	}
	fields := map[string]*commonpb.Payload{}
	for _, tt := range tests {
		fields["attr"+tt.typ] = typedPayload(t, tt.value, tt.typ)
	}
	run, err := describedRun(describeWithSearchAttributes(fields))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		got, err := json.Marshal(run.SearchAttributes["attr"+tt.typ])
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.want {
			t.Errorf("type %q: search attribute = %s, want %s", tt.typ, got, tt.want)
		}
	}
}

func TestDescribedRunRejectsUndecodableSearchAttribute(t *testing.T) {
	p := typedPayload(t, "not a number", "Int")
	_, err := describedRun(describeWithSearchAttributes(map[string]*commonpb.Payload{"Count": p}))
	var got *toolError
	if !errors.As(err, &got) || got.Code != "temporal_unavailable" {
		t.Errorf("err = %v, want a temporal_unavailable tool error", err)
	}
}

func TestToFailurePassesThroughEncodedAttributesAndLastHeartbeatDetails(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	encoded, err := dc.ToPayload("secret")
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, err := dc.ToPayloads("beat")
	if err != nil {
		t.Fatal(err)
	}
	marker := func(data string) payload {
		return payload{Handling: "passthrough", Encoding: "json/plain", Data: base64.StdEncoding.EncodeToString([]byte(data))}
	}
	secret := marker(`"secret"`)
	beat := []payload{marker(`"beat"`)}

	tests := []struct {
		name          string
		in            *failurepb.Failure
		wantEncoded   *payload
		wantHeartbeat []payload
	}{
		{
			name: "encoded attributes",
			in: &failurepb.Failure{
				EncodedAttributes: encoded,
				FailureInfo:       &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "T"}},
			},
			wantEncoded:   &secret,
			wantHeartbeat: []payload{},
		},
		{
			name: "timeout",
			in: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{LastHeartbeatDetails: heartbeat}},
			},
			wantHeartbeat: beat,
		},
		{
			name: "reset workflow",
			in: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ResetWorkflowFailureInfo{ResetWorkflowFailureInfo: &failurepb.ResetWorkflowFailureInfo{LastHeartbeatDetails: heartbeat}},
			},
			wantHeartbeat: beat,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toFailure(tt.in)
			if !reflect.DeepEqual(got.EncodedAttributes, tt.wantEncoded) {
				t.Errorf("EncodedAttributes = %#v, want %#v", got.EncodedAttributes, tt.wantEncoded)
			}
			if !reflect.DeepEqual(got.LastHeartbeatDetails, tt.wantHeartbeat) {
				t.Errorf("LastHeartbeatDetails = %#v, want %#v", got.LastHeartbeatDetails, tt.wantHeartbeat)
			}
		})
	}

	b, err := json.Marshal(toFailure(&failurepb.Failure{}))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["encoded_attributes"]) != "null" || string(fields["last_heartbeat_details"]) != "[]" {
		t.Errorf("failure = %s, want encoded_attributes null and last_heartbeat_details []", b)
	}
}

func TestAddCloseEventFailuresHaveThePassthroughFields(t *testing.T) {
	for _, e := range []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{}}},
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{}}},
	} {
		var run workflowRun
		addCloseEvent(&run, e)
		if run.Failure == nil || run.Failure.EncodedAttributes != nil || !reflect.DeepEqual(run.Failure.LastHeartbeatDetails, []payload{}) {
			t.Errorf("%s: failure = %#v, want encoded_attributes nil and last_heartbeat_details empty", e.GetEventType(), run.Failure)
		}
	}
}
