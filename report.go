package drainorder

import (
	"os"
	"time"
)

// Reason identifies what caused a shutdown to begin.
type Reason string

const (
	// ReasonSignal means an OS signal delivered through [Shutdown.Listen]
	// triggered the shutdown.
	ReasonSignal Reason = "signal"
	// ReasonManual means [Shutdown.Trigger] was called directly.
	ReasonManual Reason = "manual"
)

// PhaseStatus is the outcome of a single phase: the request drain, or one
// registered resource.
type PhaseStatus string

const (
	// StatusClosed means the phase finished within the deadline without error.
	StatusClosed PhaseStatus = "closed"
	// StatusFailed means the phase finished within the deadline but returned
	// an error.
	StatusFailed PhaseStatus = "failed"
	// StatusTimedOut means the shared deadline expired before the phase
	// finished. The phase's own goroutine may still be running in the
	// background; nothing further waits for it.
	StatusTimedOut PhaseStatus = "timed-out"
)

// PhaseReport describes how one phase (the request drain, named "http", or a
// registered resource) ended.
type PhaseReport struct {
	// Name is "http" for the request drain, or the resource's registered name.
	Name string
	// Status is how the phase ended.
	Status PhaseStatus
	// Duration is how long the phase actually ran for, from the moment its
	// dependencies were satisfied to the moment it finished or timed out.
	Duration time.Duration
	// Err is non-nil when Status is StatusFailed or StatusTimedOut. It is
	// always wrapped with the phase name via %w, so errors.Is/As still work
	// against the underlying cause (for example context.DeadlineExceeded).
	Err error
}

// Report is the result of a completed shutdown: [Shutdown.Trigger] returns
// one, and it is attached to the EventShutdownComplete event delivered to
// OnEvent so a signal-driven shutdown can still get it logged before the
// process exits.
type Report struct {
	// OK is true only if the request drain and every resource phase closed
	// cleanly. A single failed or timed-out phase makes this false even
	// though the process still exits (see the Exit* constants).
	OK bool
	// Reason says what triggered this shutdown.
	Reason Reason
	// Signal is the OS signal that triggered this shutdown, or nil for a
	// manual Trigger call.
	Signal os.Signal
	// PropagationDelay is the delay that was actually used for this run.
	PropagationDelay time.Duration
	// Request is the outcome of the request-drain phase (name "http").
	Request PhaseReport
	// Resources holds one entry per registered resource, in registration
	// order, regardless of the order they actually ran in.
	Resources []PhaseReport
	// TotalDuration is the wall-clock time from the moment the shutdown was
	// triggered to the moment every phase had finished or timed out. It
	// includes the propagation delay.
	TotalDuration time.Duration
}

// EventType names the kind of a lifecycle [Event].
type EventType string

const (
	// EventSignal fires when a signal arrives via Listen, before anything else.
	EventSignal EventType = "signal"
	// EventNotReady fires the moment the readiness probe should start failing,
	// before the propagation delay begins.
	EventNotReady EventType = "not-ready"
	// EventRequestDrainEnd fires when the request-drain phase finishes.
	EventRequestDrainEnd EventType = "request-drain-end"
	// EventResourceEnd fires when a single resource phase finishes.
	EventResourceEnd EventType = "resource-end"
	// EventShutdownComplete fires once, with the full Report, right before a
	// signal-driven shutdown calls Exit.
	EventShutdownComplete EventType = "shutdown-complete"
	// EventForcedExit fires when a second signal short-circuits an
	// in-progress shutdown, or when the liveness watchdog trips.
	EventForcedExit EventType = "forced-exit"
)

// Event is delivered to the OnEvent callback for every lifecycle transition.
// Only the fields relevant to Type are populated.
type Event struct {
	Type   EventType
	Signal os.Signal
	Phase  *PhaseReport
	Report *Report
}
