// The get_workflow_history_summary tool, which reads a workflow run's whole history and returns
// a summary of it whose size does not grow with the history.

package server

import (
	"cmp"
	"context"
	"encoding/json"
	"maps"
	"slices"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/payload"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

type getWorkflowHistorySummaryInput struct {
	Target string `json:"target" jsonschema:"the name or an alias of a target"`
	workflowRef
}

type historySummary struct {
	WorkflowID      string                            `json:"workflow_id"`
	RunID           string                            `json:"run_id"`
	Activities      summaryList[activityCounts]       `json:"activities"`
	Markers         markers                           `json:"markers"`
	ChildWorkflows  childWorkflows                    `json:"child_workflows"`
	NexusOperations nexusOperations                   `json:"nexus_operations"`
	SignalsReceived summaryList[receivedSignalCounts] `json:"signals_received"`
	SignalsSent     summaryList[sentSignalCounts]     `json:"signals_sent"`
	Updates         updates                           `json:"updates"`
	Timers          timers                            `json:"timers"`
	WorkflowTasks   workflowTasks                     `json:"workflow_tasks"`
	Close           *closeEvent                       `json:"close"`
	Coverage        coverage                          `json:"coverage"`
}

type summaryList[T any] struct {
	Items    []T       `json:"items"`
	Omitted  int       `json:"omitted"`
	NextCall *nextCall `json:"next_call"`
}

type activityCounts struct {
	Type         string          `json:"type"`
	Scheduled    int             `json:"scheduled"`
	Completed    int             `json:"completed"`
	Failed       int             `json:"failed"`
	TimedOut     int             `json:"timed_out"`
	Canceled     int             `json:"canceled"`
	Open         int             `json:"open"`
	MaxAttempt   int32           `json:"max_attempt"`
	LastFailure  *failureMessage `json:"last_failure"`
	FirstEventID int64           `json:"first_event_id"`
	LastEventID  int64           `json:"last_event_id"`
}

type failureMessage struct {
	Message            string  `json:"message"`
	EventID            int64   `json:"event_id"`
	MessageUnavailable *string `json:"message_unavailable"`
	encoded            *commonpb.Payload
}

type markers struct {
	Names        summaryList[markerCounts]       `json:"names"`
	Unsuccessful summaryList[unsuccessfulMarker] `json:"unsuccessful"`
}

type markerCounts struct {
	Name        string `json:"name"`
	Count       int    `json:"count"`
	Failed      int    `json:"failed"`
	LastEventID int64  `json:"last_event_id"`
}

type unsuccessfulMarker struct {
	Name string `json:"name"`
	failureMessage
}

type childWorkflows struct {
	Types        summaryList[childWorkflowCounts] `json:"types"`
	Unsuccessful summaryList[unsuccessfulChild]   `json:"unsuccessful"`
}

type childWorkflowCounts struct {
	Type        string `json:"type"`
	Initiated   int    `json:"initiated"`
	StartFailed int    `json:"start_failed"`
	Started     int    `json:"started"`
	Completed   int    `json:"completed"`
	Failed      int    `json:"failed"`
	TimedOut    int    `json:"timed_out"`
	Canceled    int    `json:"canceled"`
	Terminated  int    `json:"terminated"`
	Open        int    `json:"open"`
}

type unsuccessfulChild struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Outcome    string `json:"outcome"`
	EventID    int64  `json:"event_id"`
}

type nexusOperations struct {
	Operations   summaryList[nexusOperationCounts]       `json:"operations"`
	Unsuccessful summaryList[unsuccessfulNexusOperation] `json:"unsuccessful"`
}

type nexusOperation struct {
	Endpoint  string `json:"endpoint"`
	Service   string `json:"service"`
	Operation string `json:"operation"`
}

type nexusOperationCounts struct {
	nexusOperation
	Scheduled int `json:"scheduled"`
	Started   int `json:"started"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	Canceled  int `json:"canceled"`
	TimedOut  int `json:"timed_out"`
	Open      int `json:"open"`
}

type unsuccessfulNexusOperation struct {
	nexusOperation
	Outcome string           `json:"outcome"`
	EventID int64            `json:"event_id"`
	Links   []map[string]any `json:"links"`
}

type receivedSignalCounts struct {
	Name        string `json:"name"`
	Count       int    `json:"count"`
	LastEventID int64  `json:"last_event_id"`
}

type sentSignalCounts struct {
	Name      string `json:"name"`
	Initiated int    `json:"initiated"`
	Signaled  int    `json:"signaled"`
	Failed    int    `json:"failed"`
}

type updates struct {
	Names        summaryList[updateCounts]       `json:"names"`
	Unsuccessful summaryList[unsuccessfulUpdate] `json:"unsuccessful"`
}

type updateCounts struct {
	Name      string `json:"name"`
	Accepted  int    `json:"accepted"`
	Completed int    `json:"completed"`
	Failed    int    `json:"failed"`
}

type unsuccessfulUpdate struct {
	Name     string `json:"name"`
	UpdateID string `json:"update_id"`
	failureMessage
}

type timers struct {
	Started  int                    `json:"started"`
	Fired    int                    `json:"fired"`
	Canceled int                    `json:"canceled"`
	Open     summaryList[openTimer] `json:"open"`
}

type openTimer struct {
	TimerID string    `json:"timer_id"`
	FiresAt time.Time `json:"fires_at"`
	EventID int64     `json:"event_id"`
}

type workflowTasks struct {
	Completed     int                  `json:"completed"`
	Failed        int                  `json:"failed"`
	FailedByCause map[string]int       `json:"failed_by_cause"`
	TimedOut      int                  `json:"timed_out"`
	LastFailure   *workflowTaskFailure `json:"last_failure"`
}

type workflowTaskFailure struct {
	Cause string `json:"cause"`
	failureMessage
}

type closeEvent struct {
	EventType string     `json:"event_type"`
	EventTime *time.Time `json:"event_time"`
	EventID   int64      `json:"event_id"`
}

type coverage struct {
	EventsRead     int        `json:"events_read"`
	FirstEventID   int64      `json:"first_event_id"`
	FirstEventTime *time.Time `json:"first_event_time"`
	LastEventID    int64      `json:"last_event_id"`
	LastEventTime  *time.Time `json:"last_event_time"`
	PreviousRunID  *string    `json:"previous_run_id"`
}

func getWorkflowHistorySummary(cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer) mcp.ToolHandlerFor[getWorkflowHistorySummaryInput, envelope[historySummary]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowHistorySummaryInput) (*mcp.CallToolResult, envelope[historySummary], error) {
		var none envelope[historySummary]
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, none, err
		}
		svc := clients.Client(t.Name).WorkflowService()
		runID, err := resolveRunID(ctx, t, svc, in.workflowRef)
		if err != nil {
			return nil, none, err
		}
		execution := &commonpb.WorkflowExecution{WorkflowId: in.WorkflowID, RunId: runID}
		batch := payloads.Batch(t.Name)
		s := &summarizer{out: historySummary{WorkflowID: in.WorkflowID, RunID: runID, WorkflowTasks: workflowTasks{FailedByCause: map[string]int{}}}, batch: batch, endings: map[int64]*ending{}, unnamedUpdates: map[string]ending{}}
		for token := []byte(nil); ; {
			events, next, err := historyPage(ctx, t, svc, execution, false, 0, token)
			if err != nil {
				return nil, none, err
			}
			for _, e := range events {
				if err := s.add(e); err != nil {
					return nil, none, err
				}
			}
			if token = next; len(token) == 0 {
				break
			}
		}
		cuts := &listCuts{run: map[string]any{"target": t.Name, "workflow_id": in.WorkflowID, "run_id": runID}}
		summary := s.summary(cuts)
		if err := decodeMessages(ctx, batch, summary); err != nil {
			return nil, none, err
		}
		return nil, envelope[historySummary]{Target: &t.Name, Result: summary, Complete: !cuts.cut}, nil
	}
}

// A summarizer folds a run's history, read newest first, into a historySummary.
type summarizer struct {
	out                  historySummary
	batch                *payload.Batch
	endings              map[int64]*ending
	activities           tally[string, activityCounts]
	markers              tally[string, markerCounts]
	children             tally[string, childWorkflowCounts]
	nexus                tally[nexusOperation, nexusOperationCounts]
	signalsReceived      tally[string, receivedSignalCounts]
	signalsSent          tally[string, sentSignalCounts]
	updates              tally[string, updateCounts]
	unsuccessfulMarkers  []unsuccessfulMarker
	unsuccessfulChildren []unsuccessfulChild
	unsuccessfulNexus    []unsuccessfulNexusOperation
	unsuccessfulUpdates  []unsuccessfulUpdate
	openTimers           []openTimer
	unnamedUpdates       map[string]ending // by update ID, the ending of each update accepted without its request, which an older UpdateAdmitted event holds
}

// An ending holds what later events record about an activity, Nexus operation, sent signal, update or timer; read
// newest first, they arrive before the event that started it, which names its type.
type ending struct {
	eventID int64  // the latest event's ID
	outcome string // "" while open
	attempt int32
	failure *failureMessage
	started bool
	links   []map[string]any
}

// end returns the ending of what the event with ID start started, recording eventID as its latest event if no event read
// before it recorded any.
func (s *summarizer) end(start, eventID int64) *ending {
	if e, ok := s.endings[start]; ok {
		return e
	}
	e := &ending{eventID: eventID}
	s.endings[start] = e
	return e
}

// pop removes and returns the ending of what the event with ID start started, which is empty if no event recorded any.
func (s *summarizer) pop(start int64) ending {
	e, ok := s.endings[start]
	if !ok {
		return ending{}
	}
	delete(s.endings, start)
	return *e
}

// fail records f, the failure that event eventID records, if it has one and no later event recorded one.
func (e *ending) fail(f *failurepb.Failure, eventID int64) {
	if e.failure == nil && f != nil {
		e.failure = new(newFailureMessage(f, eventID))
	}
}

func newFailureMessage(f *failurepb.Failure, eventID int64) failureMessage {
	return failureMessage{Message: f.GetMessage(), EventID: eventID, encoded: f.GetEncodedAttributes()}
}

func (s *summarizer) add(e *historypb.HistoryEvent) error {
	id := e.GetEventId()
	c := &s.out.Coverage
	if c.EventsRead == 0 {
		c.LastEventID, c.LastEventTime = id, timeOf(e.GetEventTime())
	}
	c.EventsRead++
	c.FirstEventID, c.FirstEventTime = id, timeOf(e.GetEventTime())
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		if prev := e.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId(); prev != "" {
			c.PreviousRunID = &prev
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		s.out.Close = &closeEvent{EventType: e.GetEventType().String(), EventTime: timeOf(e.GetEventTime()), EventID: id}

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
		s.out.WorkflowTasks.Completed++
	case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
		a, w := e.GetWorkflowTaskFailedEventAttributes(), &s.out.WorkflowTasks
		w.Failed++
		w.FailedByCause[a.GetCause().String()]++
		if w.LastFailure == nil {
			w.LastFailure = &workflowTaskFailure{Cause: a.GetCause().String(), failureMessage: newFailureMessage(a.GetFailure(), id)}
		}
	case enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
		s.out.WorkflowTasks.TimedOut++

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		name := e.GetActivityTaskScheduledEventAttributes().GetActivityType().GetName()
		g, end := s.activities.at(name, activityCounts{Type: name}), s.pop(id)
		g.Scheduled++
		g.FirstEventID, g.LastEventID = id, max(g.LastEventID, id, end.eventID)
		g.MaxAttempt = max(g.MaxAttempt, end.attempt)
		if end.failure != nil && (g.LastFailure == nil || end.failure.EventID > g.LastFailure.EventID) {
			g.LastFailure = end.failure
		}
		switch end.outcome {
		case "completed":
			g.Completed++
		case "failed":
			g.Failed++
		case "timed_out":
			g.TimedOut++
		case "canceled":
			g.Canceled++
		default:
			g.Open++
		}
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		a := e.GetActivityTaskStartedEventAttributes()
		end := s.end(a.GetScheduledEventId(), id)
		end.attempt = a.GetAttempt()
		end.fail(a.GetLastFailure(), id)
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		s.end(e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId(), id).outcome = "completed"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		a := e.GetActivityTaskFailedEventAttributes()
		end := s.end(a.GetScheduledEventId(), id)
		end.outcome = "failed"
		end.fail(a.GetFailure(), id)
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		a := e.GetActivityTaskTimedOutEventAttributes()
		end := s.end(a.GetScheduledEventId(), id)
		end.outcome = "timed_out"
		end.fail(a.GetFailure(), id)
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		s.end(e.GetActivityTaskCanceledEventAttributes().GetScheduledEventId(), id).outcome = "canceled"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
		s.end(e.GetActivityTaskCancelRequestedEventAttributes().GetScheduledEventId(), id)

	case enumspb.EVENT_TYPE_MARKER_RECORDED:
		a := e.GetMarkerRecordedEventAttributes()
		g := s.markers.at(a.GetMarkerName(), markerCounts{Name: a.GetMarkerName(), LastEventID: id})
		g.Count++
		if f := a.GetFailure(); f != nil {
			g.Failed++
			s.unsuccessfulMarkers = append(s.unsuccessfulMarkers, unsuccessfulMarker{Name: a.GetMarkerName(), failureMessage: newFailureMessage(f, id)})
		}

	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
		s.child(e.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowType()).Initiated++
	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
		a := e.GetStartChildWorkflowExecutionFailedEventAttributes()
		s.child(a.GetWorkflowType()).StartFailed++
		s.unsuccessfulChildren = append(s.unsuccessfulChildren, unsuccessfulChild{WorkflowID: a.GetWorkflowId(), Outcome: "start_failed", EventID: id})
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
		s.child(e.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowType()).Started++
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
		s.child(e.GetChildWorkflowExecutionCompletedEventAttributes().GetWorkflowType()).Completed++
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
		a := e.GetChildWorkflowExecutionFailedEventAttributes()
		s.child(a.GetWorkflowType()).Failed++
		s.childEnded(a.GetWorkflowExecution(), "failed", id)
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
		a := e.GetChildWorkflowExecutionTimedOutEventAttributes()
		s.child(a.GetWorkflowType()).TimedOut++
		s.childEnded(a.GetWorkflowExecution(), "timed_out", id)
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
		a := e.GetChildWorkflowExecutionCanceledEventAttributes()
		s.child(a.GetWorkflowType()).Canceled++
		s.childEnded(a.GetWorkflowExecution(), "canceled", id)
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		a := e.GetChildWorkflowExecutionTerminatedEventAttributes()
		s.child(a.GetWorkflowType()).Terminated++
		s.childEnded(a.GetWorkflowExecution(), "terminated", id)

	case enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
		a := e.GetNexusOperationScheduledEventAttributes()
		op := nexusOperation{Endpoint: a.GetEndpoint(), Service: a.GetService(), Operation: a.GetOperation()}
		g, end := s.nexus.at(op, nexusOperationCounts{nexusOperation: op}), s.pop(id)
		g.Scheduled++
		if end.started {
			g.Started++
		}
		switch end.outcome {
		case "completed":
			g.Completed++
		case "failed":
			g.Failed++
		case "canceled":
			g.Canceled++
		case "timed_out":
			g.TimedOut++
		default:
			g.Open++
		}
		if end.outcome != "" && end.outcome != "completed" {
			s.unsuccessfulNexus = append(s.unsuccessfulNexus, unsuccessfulNexusOperation{nexusOperation: op, Outcome: end.outcome, EventID: end.eventID, Links: append([]map[string]any{}, end.links...)})
		}
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED:
		end := s.end(e.GetNexusOperationStartedEventAttributes().GetScheduledEventId(), id)
		end.started = true
		var err error
		if end.links, err = eventLinks(e, s.batch); err != nil {
			return err
		}
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
		s.end(e.GetNexusOperationCompletedEventAttributes().GetScheduledEventId(), id).outcome = "completed"
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
		s.end(e.GetNexusOperationFailedEventAttributes().GetScheduledEventId(), id).outcome = "failed"
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
		s.end(e.GetNexusOperationCanceledEventAttributes().GetScheduledEventId(), id).outcome = "canceled"
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
		s.end(e.GetNexusOperationTimedOutEventAttributes().GetScheduledEventId(), id).outcome = "timed_out"

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		name := e.GetWorkflowExecutionSignaledEventAttributes().GetSignalName()
		s.signalsReceived.at(name, receivedSignalCounts{Name: name, LastEventID: id}).Count++

	case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
		name := e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetSignalName()
		g := s.signalsSent.at(name, sentSignalCounts{Name: name})
		g.Initiated++
		switch s.pop(id).outcome {
		case "signaled":
			g.Signaled++
		case "failed":
			g.Failed++
		}
	case enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
		s.end(e.GetExternalWorkflowExecutionSignaledEventAttributes().GetInitiatedEventId(), id).outcome = "signaled"
	case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		s.end(e.GetSignalExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId(), id).outcome = "failed"

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
		a := e.GetWorkflowExecutionUpdateAcceptedEventAttributes()
		if end := s.pop(id); a.GetAcceptedRequest() == nil {
			s.unnamedUpdates[a.GetProtocolInstanceId()] = end
		} else {
			s.countUpdate(a.GetAcceptedRequest().GetInput().GetName(), a.GetProtocolInstanceId(), end)
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
		r := e.GetWorkflowExecutionUpdateAdmittedEventAttributes().GetRequest()
		if end, ok := s.unnamedUpdates[r.GetMeta().GetUpdateId()]; ok {
			delete(s.unnamedUpdates, r.GetMeta().GetUpdateId())
			s.countUpdate(r.GetInput().GetName(), r.GetMeta().GetUpdateId(), end)
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
		a := e.GetWorkflowExecutionUpdateCompletedEventAttributes()
		outcome := "completed"
		if a.GetOutcome().GetFailure() != nil {
			outcome = "failed"
		}
		end := s.end(a.GetAcceptedEventId(), id)
		end.outcome = outcome
		end.fail(a.GetOutcome().GetFailure(), id)

	case enumspb.EVENT_TYPE_TIMER_STARTED:
		a := e.GetTimerStartedEventAttributes()
		s.out.Timers.Started++
		if s.pop(id).outcome == "" && s.out.Close == nil {
			s.openTimers = append(s.openTimers, openTimer{TimerID: a.GetTimerId(), FiresAt: e.GetEventTime().AsTime().Add(a.GetStartToFireTimeout().AsDuration()), EventID: id})
		}
	case enumspb.EVENT_TYPE_TIMER_FIRED:
		s.out.Timers.Fired++
		s.end(e.GetTimerFiredEventAttributes().GetStartedEventId(), id).outcome = "fired"
	case enumspb.EVENT_TYPE_TIMER_CANCELED:
		s.out.Timers.Canceled++
		s.end(e.GetTimerCanceledEventAttributes().GetStartedEventId(), id).outcome = "canceled"
	}
	return nil
}

func (s *summarizer) child(typ *commonpb.WorkflowType) *childWorkflowCounts {
	return s.children.at(typ.GetName(), childWorkflowCounts{Type: typ.GetName()})
}

func (s *summarizer) childEnded(child *commonpb.WorkflowExecution, outcome string, eventID int64) {
	s.unsuccessfulChildren = append(s.unsuccessfulChildren, unsuccessfulChild{WorkflowID: child.GetWorkflowId(), RunID: child.GetRunId(), Outcome: outcome, EventID: eventID})
}

func (s *summarizer) countUpdate(name, updateID string, end ending) {
	g := s.updates.at(name, updateCounts{Name: name})
	g.Accepted++
	switch end.outcome {
	case "completed":
		g.Completed++
	case "failed":
		g.Failed++
		s.unsuccessfulUpdates = append(s.unsuccessfulUpdates, unsuccessfulUpdate{Name: name, UpdateID: updateID, failureMessage: *end.failure})
	}
}

// summary returns the summary of the events added, with each list cut as cutList cuts it.
func (s *summarizer) summary(cuts *listCuts) historySummary {
	out := s.out
	for _, c := range s.children.order {
		c.Open = c.Initiated - c.StartFailed - c.Completed - c.Failed - c.TimedOut - c.Canceled - c.Terminated
	}
	out.Activities = s.activities.list(cuts, func(a *activityCounts) int { return a.Scheduled }, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
	out.Markers = markers{
		Names:        s.markers.list(cuts, func(m *markerCounts) int { return m.Count }, enumspb.EVENT_TYPE_MARKER_RECORDED),
		Unsuccessful: cutList(s.unsuccessfulMarkers, cuts, enumspb.EVENT_TYPE_MARKER_RECORDED),
	}
	out.ChildWorkflows = childWorkflows{
		Types: s.children.list(cuts, func(c *childWorkflowCounts) int { return c.Initiated }, enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED),
		Unsuccessful: cutList(s.unsuccessfulChildren, cuts, enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED),
	}
	// Each unsuccessful Nexus operation and update was added when the event that names it was read, so sort them newest
	// first by the event that ended them.
	slices.SortFunc(s.unsuccessfulNexus, func(a, b unsuccessfulNexusOperation) int { return cmp.Compare(b.EventID, a.EventID) })
	slices.SortFunc(s.unsuccessfulUpdates, func(a, b unsuccessfulUpdate) int { return cmp.Compare(b.EventID, a.EventID) })
	out.NexusOperations = nexusOperations{
		Operations: s.nexus.list(cuts, func(n *nexusOperationCounts) int { return n.Scheduled }, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED),
		Unsuccessful: cutList(s.unsuccessfulNexus, cuts, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT),
	}
	out.SignalsReceived = s.signalsReceived.list(cuts, func(r *receivedSignalCounts) int { return r.Count }, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	out.SignalsSent = s.signalsSent.list(cuts, func(r *sentSignalCounts) int { return r.Initiated }, enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
	out.Updates = updates{
		Names: s.updates.list(cuts, func(u *updateCounts) int { return u.Accepted }, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED),
		Unsuccessful: cutList(s.unsuccessfulUpdates, cuts, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED),
	}
	out.Timers.Open = cutList(s.openTimers, cuts, enumspb.EVENT_TYPE_TIMER_STARTED)
	return out
}

// A tally holds groups by key, in the order their keys were first read.
type tally[K comparable, G any] struct {
	groups map[K]*G
	order  []*G
}

// at returns the group for k, starting it as g if k has none.
func (t *tally[K, G]) at(k K, g G) *G {
	if p, ok := t.groups[k]; ok {
		return p
	}
	if t.groups == nil {
		t.groups = map[K]*G{}
	}
	t.groups[k] = &g
	t.order = append(t.order, &g)
	return &g
}

// list returns t's groups, those with the highest count first, cut as cutList cuts them.
func (t *tally[K, G]) list(cuts *listCuts, count func(*G) int, types ...enumspb.EventType) summaryList[G] {
	groups := []G{}
	for _, g := range slices.SortedStableFunc(slices.Values(t.order), func(a, b *G) int { return cmp.Compare(count(b), count(a)) }) {
		groups = append(groups, *g)
	}
	return cutList(groups, cuts, types...)
}

// listCuts holds run, the target, workflow_id and run_id of the get_workflow_history call that a cut list gives, and
// cut, whether cutList has cut any list.
type listCuts struct {
	run map[string]any
	cut bool
}

// cutList returns the first maxListItems of items and, if it leaves any out, a next_call to get_workflow_history with the
// arguments in cuts.run and event_types set to types, and then it sets cuts.cut.
func cutList[T any](items []T, cuts *listCuts, types ...enumspb.EventType) summaryList[T] {
	l := summaryList[T]{Items: append([]T{}, items[:min(len(items), maxListItems)]...)}
	if l.Omitted = len(items) - len(l.Items); l.Omitted > 0 {
		cuts.cut = true
		args := maps.Clone(cuts.run)
		names := []string{}
		for _, typ := range types {
			names = append(names, typ.String())
		}
		args["event_types"] = names
		l.NextCall = &nextCall{Name: "get_workflow_history", Arguments: args}
	}
	return l
}

// decodeMessages replaces the message of each failureMessage in summary whose attributes the SDK encoded with the
// message in those attributes when the target's payload_handling shows them as JSON, and otherwise sets
// message_unavailable to why it does not.
func decodeMessages(ctx context.Context, batch *payload.Batch, summary historySummary) error {
	fields := map[*failureMessage]*payload.Field{}
	add := func(f *failureMessage) {
		if f != nil && f.encoded != nil {
			fields[f] = batch.Add(f.encoded)
		}
	}
	for _, a := range summary.Activities.Items {
		add(a.LastFailure)
	}
	for i := range summary.Markers.Unsuccessful.Items {
		add(&summary.Markers.Unsuccessful.Items[i].failureMessage)
	}
	for i := range summary.Updates.Unsuccessful.Items {
		add(&summary.Updates.Unsuccessful.Items[i].failureMessage)
	}
	if f := summary.WorkflowTasks.LastFailure; f != nil {
		add(&f.failureMessage)
	}
	if err := batch.Render(ctx); err != nil {
		return err
	}
	for f, field := range fields {
		var attributes struct {
			Message string `json:"message"`
		}
		if v, ok := field.Value.(json.RawMessage); ok {
			if json.Unmarshal(v, &attributes) == nil {
				f.Message = attributes.Message
			}
		} else {
			reason := "decode_failed"
			if field.Reason != "" {
				reason = field.Reason
			} else if field.Handling == "passthrough" {
				reason = "payload_handling"
			}
			f.MessageUnavailable = &reason
		}
	}
	return nil
}
