// Package drainorder provides graceful shutdown for a [net/http.Server],
// focused on two things almost every graceful-shutdown recipe gets wrong.
//
// # The readiness-propagation gap
//
// In Kubernetes, SIGTERM and endpoint removal race: the kubelet signals your
// container at the same moment the Service's endpoint controller starts
// removing the pod from Endpoints, and that removal takes time — up to a few
// seconds — to reach every kube-proxy and every load balancer in front of it.
// Some of them keep sending you traffic, entirely correctly, because they
// don't know yet that you're going away.
//
// [net/http.Server.Shutdown] has no concept of this. It closes listeners and
// starts refusing new connections the instant it is called, which is exactly
// wrong for this race: it refuses traffic that was correctly routed to you a
// moment before you disappeared. This package inserts a propagation delay
// between "readiness starts failing" and "stop accepting connections", which
// is the only place that fixes the race — see [Shutdown] for why that delay
// cannot be zero.
//
// # Dependency-ordered shutdown
//
// Resources have to close in dependency order: stop accepting new work, let
// in-flight work finish, then close the database — closing the pool while
// requests are still running turns a clean shutdown into a burst of errors on
// whatever was still in flight. [Shutdown.Resource] models this as phases:
// the request drain is always phase zero, every resource depends on it by
// default, and resources with the same satisfied dependencies close in
// parallel. A single deadline covers the whole thing; whatever hasn't closed
// when it expires is reported as timed out and the process exits anyway.
package drainorder

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Default configuration used by [New] when the corresponding option is not
// supplied.
const (
	// DefaultPropagationDelay is used when [WithPropagationDelay] is not
	// passed to [New]. It is deliberately not zero — see [Shutdown] docs for
	// why zero defeats the point of this package.
	DefaultPropagationDelay = 5 * time.Second
	// DefaultDeadline is used when [WithDeadline] is not passed to [New].
	DefaultDeadline = 30 * time.Second
)

// Exit codes passed to the configured Exit function (see [WithExit]) once a
// signal-driven shutdown finishes.
const (
	// ExitOK means the shutdown report is OK: every phase closed cleanly.
	ExitOK = 0
	// ExitUnclean means shutdown ran to completion but at least one phase
	// failed or timed out — see the returned Report for which one.
	ExitUnclean = 1
	// ExitForced means a second signal arrived while a shutdown was already
	// in progress. The drain was skipped entirely; this is the documented
	// "I don't care, kill it now" escape hatch, not a bug.
	ExitForced = 2
)

// livenessGrace is the extra time, past the shared deadline, that the
// liveness watchdog allows before declaring the process stuck. Every phase
// this package itself schedules is already bounded by the shared deadline
// context, so under normal operation this never fires; it exists only as a
// safety net against a bug in the scheduler itself, not to police resource
// CloseFuncs (those are already bounded by the same deadline).
const livenessGrace = 500 * time.Millisecond

// ErrAlreadyTriggered is returned by [Shutdown.Trigger] when shutdown has
// already been triggered (by a signal, or by a previous Trigger call).
var ErrAlreadyTriggered = errors.New("drainorder: shutdown already triggered")

// Shutdown coordinates graceful shutdown of an [net/http.Server]: it fails
// readiness, waits out a propagation delay, drains in-flight requests, and
// then closes registered resources in dependency order, all within a single
// overall deadline.
//
// # Why the propagation delay cannot be zero
//
// At a propagation delay of zero, "fail readiness" and "stop accepting
// connections" happen in the same instant. That reproduces the exact bug
// this package exists to fix: the readiness probe fails and the listener
// closes at the same moment, so anything already routed to you in that
// instant gets a refused or reset connection. There is no version of
// "propagate a state change across a fleet of proxies" that completes in
// 0ms. Set it to something a real network round trip and probe interval can
// fit inside — a few seconds is typical — and keep it comfortably less than
// your pod's terminationGracePeriodSeconds minus however long the rest of the
// drain can take.
type Shutdown struct {
	server           *http.Server
	propagationDelay time.Duration
	deadline         time.Duration
	signals          []os.Signal
	onEvent          func(Event)
	exit             func(code int)

	mu        sync.Mutex
	resources map[string]*resourceConfig
	order     []string

	state     atomic.Int32
	live      atomic.Bool
	triggered atomic.Bool

	sigCh chan os.Signal
}

// Option configures a [Shutdown] created by [New].
type Option func(*Shutdown)

// WithPropagationDelay overrides [DefaultPropagationDelay].
func WithPropagationDelay(d time.Duration) Option {
	return func(s *Shutdown) { s.propagationDelay = d }
}

// WithDeadline overrides [DefaultDeadline]. The deadline covers the request
// drain plus every resource phase, measured from the end of the propagation
// delay — it does not include the propagation delay itself.
func WithDeadline(d time.Duration) Option {
	return func(s *Shutdown) { s.deadline = d }
}

// WithSignals overrides the default signal set (SIGTERM, SIGINT). Pass no
// arguments to disable signal handling entirely and drive shutdown only
// through [Shutdown.Trigger].
func WithSignals(sig ...os.Signal) Option {
	list := append([]os.Signal(nil), sig...)
	return func(s *Shutdown) { s.signals = list }
}

// WithOnEvent registers a callback invoked for every lifecycle [Event]. Wire
// it to your logger. The default does nothing.
func WithOnEvent(fn func(Event)) Option {
	return func(s *Shutdown) { s.onEvent = fn }
}

// WithExit overrides how a signal-driven shutdown ends the process (default
// [os.Exit]). Tests should always override this — the default terminates
// the test binary.
func WithExit(fn func(code int)) Option {
	return func(s *Shutdown) { s.exit = fn }
}

// New creates a [Shutdown] for server. Configure it with [Option] values,
// register resources with [Shutdown.Resource], then either call
// [Shutdown.Listen] to react to OS signals or drive it yourself with
// [Shutdown.Trigger].
func New(server *http.Server, opts ...Option) *Shutdown {
	s := &Shutdown{
		server:           server,
		propagationDelay: DefaultPropagationDelay,
		deadline:         DefaultDeadline,
		signals:          defaultSignals(),
		onEvent:          func(Event) {},
		exit:             os.Exit,
		resources:        make(map[string]*resourceConfig),
	}
	s.state.Store(int32(StateReady))
	s.live.Store(true)
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// State returns the current readiness/liveness state.
func (s *Shutdown) State() State { return State(s.state.Load()) }

// IsReady reports whether the server should still be receiving new traffic.
// It is true only before shutdown starts; wire it into your readiness probe.
func (s *Shutdown) IsReady() bool { return s.State() == StateReady }

// IsLive reports whether the process is healthy. It stays true for the
// entire drain, including resource close — a shutdown in progress is not the
// same thing as a stuck process. It only goes false if shutdown is still
// running well past its own deadline, which under normal operation should
// never happen; wire it into your liveness probe as the signal that this
// specific process is wedged and Kubernetes should kill it.
func (s *Shutdown) IsLive() bool { return s.live.Load() }

func (s *Shutdown) setState(st State) { s.state.Store(int32(st)) }

func (s *Shutdown) emit(e Event) { s.onEvent(e) }

// Trigger runs the shutdown sequence once: fail readiness, wait out the
// propagation delay, drain in-flight requests, then close resources in
// dependency order, all bounded by the configured deadline. It blocks until
// every phase has finished or been marked timed out, then returns a
// [Report].
//
// ctx is the parent context for the whole operation; canceling it cuts the
// propagation delay short and lowers the effective deadline, but never
// raises it. Calling Trigger a second time — including a second time via a
// signal — returns [ErrAlreadyTriggered] instead of running again.
func (s *Shutdown) Trigger(ctx context.Context) (*Report, error) {
	return s.trigger(ctx, ReasonManual, nil)
}

func (s *Shutdown) trigger(ctx context.Context, reason Reason, sig os.Signal) (*Report, error) {
	if !s.triggered.CompareAndSwap(false, true) {
		return nil, ErrAlreadyTriggered
	}
	if err := s.validate(); err != nil {
		return nil, err
	}

	start := time.Now()
	s.setState(StateNotReady)
	if reason == ReasonSignal {
		s.emit(Event{Type: EventSignal, Signal: sig})
	}
	s.emit(Event{Type: EventNotReady})

	deadlineAt := start.Add(s.propagationDelay).Add(s.deadline)
	deadlineCtx, cancel := context.WithDeadline(ctx, deadlineAt)
	defer cancel()

	watchdog := time.AfterFunc(time.Until(deadlineAt)+livenessGrace, func() {
		if s.State() != StateStopped {
			s.live.Store(false)
			s.emit(Event{Type: EventForcedExit})
		}
	})
	defer watchdog.Stop()

	s.mu.Lock()
	order := append([]string(nil), s.order...)
	resources := make(map[string]*resourceConfig, len(s.resources))
	for name, rc := range s.resources {
		resources[name] = rc
	}
	s.mu.Unlock()

	done := make(map[string]chan struct{}, len(order)+1)
	done[httpPhase] = make(chan struct{})
	for _, name := range order {
		done[name] = make(chan struct{})
	}

	reports := make(map[string]*PhaseReport, len(order)+1)
	var reportsMu sync.Mutex
	recordReport := func(rep *PhaseReport) {
		reportsMu.Lock()
		reports[rep.Name] = rep
		reportsMu.Unlock()
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done[httpPhase])
		select {
		case <-time.After(s.propagationDelay):
		case <-ctx.Done():
		}
		rep := s.runRequestDrain(deadlineCtx)
		recordReport(rep)
		s.emit(Event{Type: EventRequestDrainEnd, Phase: rep})
	}()

	for _, name := range order {
		name := name
		rc := resources[name]
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done[name])
			for _, dep := range rc.after {
				<-done[dep]
			}
			rep := s.runResource(deadlineCtx, rc)
			recordReport(rep)
			s.emit(Event{Type: EventResourceEnd, Phase: rep})
		}()
	}

	wg.Wait()
	s.setState(StateStopped)

	report := &Report{
		Reason:           reason,
		Signal:           sig,
		PropagationDelay: s.propagationDelay,
		Request:          *reports[httpPhase],
		TotalDuration:    time.Since(start),
	}
	ok := report.Request.Status == StatusClosed
	for _, name := range order {
		rep := *reports[name]
		report.Resources = append(report.Resources, rep)
		if rep.Status != StatusClosed {
			ok = false
		}
	}
	report.OK = ok

	s.emit(Event{Type: EventShutdownComplete, Report: report})
	return report, nil
}

func (s *Shutdown) runRequestDrain(ctx context.Context) *PhaseReport {
	start := time.Now()
	err := s.server.Shutdown(ctx)
	dur := time.Since(start)
	if err == nil {
		return &PhaseReport{Name: httpPhase, Status: StatusClosed, Duration: dur}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// Shutdown gave up waiting for connections to idle out. Force-close
		// whatever is left so the process can actually exit; Shutdown itself
		// never does this for us (it explicitly leaves hijacked connections,
		// like WebSockets, alone even on success).
		_ = s.server.Close()
		return &PhaseReport{Name: httpPhase, Status: StatusTimedOut, Duration: dur, Err: fmt.Errorf("request drain: %w", err)}
	}
	return &PhaseReport{Name: httpPhase, Status: StatusFailed, Duration: dur, Err: fmt.Errorf("request drain: %w", err)}
}

func (s *Shutdown) runResource(ctx context.Context, rc *resourceConfig) *PhaseReport {
	start := time.Now()
	errCh := make(chan error, 1)
	go func() {
		errCh <- rc.close(ctx)
	}()
	select {
	case err := <-errCh:
		dur := time.Since(start)
		if err != nil {
			return &PhaseReport{Name: rc.name, Status: StatusFailed, Duration: dur, Err: fmt.Errorf("%s: %w", rc.name, err)}
		}
		return &PhaseReport{Name: rc.name, Status: StatusClosed, Duration: dur}
	case <-ctx.Done():
		dur := time.Since(start)
		// rc.close is still running in the background; we don't wait for
		// it. That's the point of the deadline: the process reports what
		// didn't close in time and exits anyway, rather than hanging until
		// Kubernetes SIGKILLs it with no report at all.
		return &PhaseReport{Name: rc.name, Status: StatusTimedOut, Duration: dur, Err: fmt.Errorf("%s: %w", rc.name, ctx.Err())}
	}
}
