package drainorder

import (
	"context"
	"errors"
	"fmt"
)

// httpPhase is the reserved name of the request-drain phase. Every resource
// depends on it by default.
const httpPhase = "http"

// CloseFunc closes one resource. It receives the shared shutdown context,
// which carries the deadline for the whole shutdown (request drain plus
// every resource phase); a well-behaved implementation returns as soon as
// ctx is done instead of running to completion regardless.
type CloseFunc func(ctx context.Context) error

// ErrReservedName is returned by [Shutdown.Resource] when the name "http" is
// used; it is reserved for the request-drain phase.
var ErrReservedName = errors.New(`drainorder: "http" is reserved for the request-drain phase`)

// ErrResourceExists is returned by [Shutdown.Resource] when name was already
// registered.
var ErrResourceExists = errors.New("drainorder: resource already registered")

type resourceConfig struct {
	name  string
	close CloseFunc
	after []string
}

// ResourceOption configures a single call to [Shutdown.Resource].
type ResourceOption func(*resourceConfig)

// After declares which phases must finish before this resource's CloseFunc
// runs. Resources sharing the same satisfied dependencies run concurrently.
//
// Called with no arguments, After() makes the resource independent of
// everything else, including the request drain: it starts as soon as
// shutdown is triggered, in parallel with the propagation delay and the
// request drain. This is the escape hatch for something safe to close (or
// notify) regardless of in-flight HTTP requests — telling long-lived
// WebSocket/SSE clients to reconnect elsewhere, for example.
//
// When Resource is called without an After option at all, the default
// dependency is ["http"]: the resource waits for the request drain to
// finish, which is almost always what you want for anything a request
// handler might still touch (a database pool, a cache client).
func After(names ...string) ResourceOption {
	deps := append([]string(nil), names...)
	return func(rc *resourceConfig) {
		rc.after = deps
	}
}

// Resource registers a named resource to close during shutdown. By default
// it runs after the request-drain phase ("http"); pass [After] to declare a
// different dependency set, including After() for "independent of
// everything."
//
// Resource returns an error if name is empty, is the reserved name "http",
// or was already registered. It does not validate the dependency graph
// itself — unknown names and dependency cycles are reported when shutdown is
// triggered, since resources may be registered in any order.
func (s *Shutdown) Resource(name string, close CloseFunc, opts ...ResourceOption) error {
	if name == "" {
		return errors.New("drainorder: resource name must not be empty")
	}
	if name == httpPhase {
		return ErrReservedName
	}
	if close == nil {
		return fmt.Errorf("drainorder: resource %q: close function must not be nil", name)
	}

	rc := &resourceConfig{name: name, close: close, after: []string{httpPhase}}
	for _, opt := range opts {
		opt(rc)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.resources[name]; exists {
		return fmt.Errorf("%w: %q", ErrResourceExists, name)
	}
	s.resources[name] = rc
	s.order = append(s.order, name)
	return nil
}

// validate checks that every declared dependency refers to a registered
// resource (or "http") and that the dependency graph has no cycles. It runs
// once, at the start of Trigger, so registration order never matters.
func (s *Shutdown) validate() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, rc := range s.resources {
		for _, dep := range rc.after {
			if dep == httpPhase {
				continue
			}
			if dep == name {
				return fmt.Errorf("drainorder: resource %q cannot depend on itself", name)
			}
			if _, ok := s.resources[dep]; !ok {
				return fmt.Errorf("drainorder: resource %q depends on unregistered resource %q", name, dep)
			}
		}
	}

	const (
		white = iota
		gray
		black
	)
	color := make(map[string]int, len(s.resources))
	var visit func(name string) error
	visit = func(name string) error {
		color[name] = gray
		for _, dep := range s.resources[name].after {
			if dep == httpPhase {
				continue
			}
			switch color[dep] {
			case gray:
				return fmt.Errorf("drainorder: dependency cycle detected: %q depends on %q", name, dep)
			case white:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[name] = black
		return nil
	}
	for name := range s.resources {
		if color[name] == white {
			if err := visit(name); err != nil {
				return err
			}
		}
	}
	return nil
}
