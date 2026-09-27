package drainorder_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/agentscope-ai-java/drainorder-go"
)

// startServer starts a real *http.Server on a real, OS-assigned localhost
// port and returns it along with its base URL. The caller is responsible for
// shutting it down (that's the whole point of this package).
func startServer(t *testing.T, handler http.Handler) (*http.Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: handler}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("serve: %v", err)
		}
	}()
	return srv, "http://" + ln.Addr().String()
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

// TestRequestServedDuringPropagationDelay proves the core claim of this
// package: a request that arrives after shutdown has been triggered, but
// while still inside the propagation delay, is served normally rather than
// refused.
func TestRequestServedDuringPropagationDelay(t *testing.T) {
	srv, addr := startServer(t, okHandler())
	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(200*time.Millisecond),
		drainorder.WithDeadline(2*time.Second),
	)

	reportCh := make(chan *drainorder.Report, 1)
	go func() {
		rep, err := sh.Trigger(context.Background())
		if err != nil {
			t.Errorf("Trigger: %v", err)
		}
		reportCh <- rep
	}()

	// Readiness must already have failed...
	deadline := time.Now().Add(time.Second)
	for sh.IsReady() {
		if time.Now().After(deadline) {
			t.Fatal("IsReady() never became false after Trigger")
		}
		time.Sleep(time.Millisecond)
	}

	// ...but a request arriving squarely inside the propagation delay must
	// still succeed.
	time.Sleep(80 * time.Millisecond)
	resp, err := http.Get(addr)
	if err != nil {
		t.Fatalf("request during propagation delay failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	<-reportCh
}

// TestNewConnectionRefusedAfterDrainStarts proves the other half: once the
// propagation delay has elapsed and the request drain has begun, a brand new
// connection is refused.
func TestNewConnectionRefusedAfterDrainStarts(t *testing.T) {
	srv, addr := startServer(t, okHandler())
	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(50*time.Millisecond),
		drainorder.WithDeadline(2*time.Second),
	)

	reportCh := make(chan *drainorder.Report, 1)
	go func() {
		rep, _ := sh.Trigger(context.Background())
		reportCh <- rep
	}()

	// Well past the propagation delay: the listener should be gone.
	time.Sleep(200 * time.Millisecond)

	u, err := netURLHostPort(addr)
	if err != nil {
		t.Fatalf("parse addr: %v", err)
	}
	conn, dialErr := net.DialTimeout("tcp", u, 200*time.Millisecond)
	if dialErr == nil {
		conn.Close()
		t.Fatal("dial succeeded after drain started; want refused")
	}

	<-reportCh
}

// TestInFlightRequestCompletes proves a request already being handled when
// shutdown is triggered is allowed to finish rather than being cut off.
func TestInFlightRequestCompletes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
	})

	srv, addr := startServer(t, handler)
	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(10*time.Millisecond),
		drainorder.WithDeadline(2*time.Second),
	)

	type result struct {
		status int
		err    error
	}
	resCh := make(chan result, 1)
	go func() {
		resp, err := http.Get(addr)
		if err != nil {
			resCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		resCh <- result{status: resp.StatusCode}
	}()

	<-started // request is now in flight, handler is blocked on release

	reportCh := make(chan *drainorder.Report, 1)
	go func() {
		rep, _ := sh.Trigger(context.Background())
		reportCh <- rep
	}()

	// Give the propagation delay + request-drain call a moment to actually
	// start waiting on this connection before we let the handler finish.
	time.Sleep(60 * time.Millisecond)
	close(release)

	res := <-resCh
	if res.err != nil {
		t.Fatalf("in-flight request failed: %v", res.err)
	}
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.status)
	}

	rep := <-reportCh
	if rep.Request.Status != drainorder.StatusClosed {
		t.Fatalf("request phase status = %s, want closed", rep.Request.Status)
	}
}

// TestResourcePhaseWaitsForRequestPhase proves the dependency ordering: a
// default resource (after: ["http"]) does not start until the request drain
// has actually finished, even when a slow in-flight request delays that.
func TestResourcePhaseWaitsForRequestPhase(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	})
	srv, addr := startServer(t, handler)

	var httpEndedAt, dbStartedAt time.Time
	var mu sync.Mutex

	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(10*time.Millisecond),
		drainorder.WithDeadline(2*time.Second),
		drainorder.WithOnEvent(func(e drainorder.Event) {
			if e.Type == drainorder.EventRequestDrainEnd {
				mu.Lock()
				httpEndedAt = time.Now()
				mu.Unlock()
			}
		}),
	)

	err := sh.Resource("db", func(ctx context.Context) error {
		mu.Lock()
		dbStartedAt = time.Now()
		mu.Unlock()
		return nil
	}) // default After: ["http"]
	if err != nil {
		t.Fatalf("Resource: %v", err)
	}

	go func() {
		resp, err := http.Get(addr)
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-started

	reportCh := make(chan *drainorder.Report, 1)
	errCh := make(chan error, 1)
	go func() {
		rep, err := sh.Trigger(context.Background())
		reportCh <- rep
		errCh <- err
	}()

	// Let the propagation delay elapse and the request-drain phase actually
	// start waiting on the in-flight request before letting it finish.
	time.Sleep(60 * time.Millisecond)
	close(release)

	rep := <-reportCh
	if err := <-errCh; err != nil {
		t.Fatalf("Trigger: %v", err)
	}

	if rep.Request.Status != drainorder.StatusClosed {
		t.Fatalf("request phase status = %s, want closed", rep.Request.Status)
	}
	if len(rep.Resources) != 1 || rep.Resources[0].Name != "db" {
		t.Fatalf("resources = %+v, want a single db entry", rep.Resources)
	}
	if rep.Resources[0].Status != drainorder.StatusClosed {
		t.Fatalf("db status = %s, want closed", rep.Resources[0].Status)
	}

	mu.Lock()
	defer mu.Unlock()
	if httpEndedAt.IsZero() {
		t.Fatal("EventRequestDrainEnd never fired")
	}
	if !dbStartedAt.After(httpEndedAt) {
		t.Fatalf("db resource started at %v, want strictly after request drain ended at %v", dbStartedAt, httpEndedAt)
	}
}

// TestIndependentResourceRunsDuringPropagationDelay proves that a resource
// registered with After() (no dependencies) starts immediately, in parallel
// with the propagation delay, rather than waiting for the request drain.
func TestIndependentResourceRunsDuringPropagationDelay(t *testing.T) {
	srv, _ := startServer(t, okHandler())
	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(300*time.Millisecond),
		drainorder.WithDeadline(2*time.Second),
	)

	var startedAt atomic.Int64
	start := time.Now()
	err := sh.Resource("tracing", func(ctx context.Context) error {
		startedAt.Store(int64(time.Since(start)))
		return nil
	}, drainorder.After())
	if err != nil {
		t.Fatalf("Resource: %v", err)
	}

	rep, err := sh.Trigger(context.Background())
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rep.Resources[0].Status != drainorder.StatusClosed {
		t.Fatalf("tracing status = %s, want closed", rep.Resources[0].Status)
	}

	got := time.Duration(startedAt.Load())
	if got >= 150*time.Millisecond {
		t.Fatalf("independent resource started after %v, want well before the 300ms propagation delay elapsed", got)
	}
}

// TestDeadlineNamesOffendingResource proves the total deadline forces the
// process onward even when a resource ignores its context entirely, and
// that the report names exactly which resource failed to close in time.
func TestDeadlineNamesOffendingResource(t *testing.T) {
	srv, _ := startServer(t, okHandler())
	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(10*time.Millisecond),
		drainorder.WithDeadline(100*time.Millisecond),
	)

	err := sh.Resource("stuck", func(ctx context.Context) error {
		// Deliberately ignores ctx: simulates a buggy or slow resource.
		time.Sleep(5 * time.Second)
		return nil
	})
	if err != nil {
		t.Fatalf("Resource: %v", err)
	}
	if err := sh.Resource("fine", func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Resource: %v", err)
	}

	triggerStart := time.Now()
	rep, err := sh.Trigger(context.Background())
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	elapsed := time.Since(triggerStart)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Trigger took %v, want it to return promptly at the deadline, not wait for the stuck resource", elapsed)
	}

	if rep.OK {
		t.Fatal("report.OK = true, want false because \"stuck\" timed out")
	}

	var found *drainorder.PhaseReport
	for i := range rep.Resources {
		if rep.Resources[i].Name == "stuck" {
			found = &rep.Resources[i]
		}
	}
	if found == nil {
		t.Fatal("report does not mention the \"stuck\" resource at all")
	}
	if found.Status != drainorder.StatusTimedOut {
		t.Fatalf("stuck status = %s, want timed-out", found.Status)
	}
	if found.Err == nil {
		t.Fatal("stuck resource's report has no error explaining the timeout")
	}
}

// TestValidateUnknownDependency proves a resource depending on a name that
// was never registered is rejected before shutdown runs anything.
func TestValidateUnknownDependency(t *testing.T) {
	srv, _ := startServer(t, okHandler())
	sh := drainorder.New(srv, drainorder.WithPropagationDelay(time.Millisecond), drainorder.WithDeadline(time.Second))

	if err := sh.Resource("a", func(ctx context.Context) error { return nil }, drainorder.After("ghost")); err != nil {
		t.Fatalf("Resource: %v", err)
	}

	_, err := sh.Trigger(context.Background())
	if err == nil {
		t.Fatal("Trigger succeeded with an unregistered dependency; want an error")
	}
}

// TestValidateCycle proves a dependency cycle between two resources is
// rejected rather than deadlocking.
func TestValidateCycle(t *testing.T) {
	srv, _ := startServer(t, okHandler())
	sh := drainorder.New(srv, drainorder.WithPropagationDelay(time.Millisecond), drainorder.WithDeadline(time.Second))

	if err := sh.Resource("a", func(ctx context.Context) error { return nil }, drainorder.After("b")); err != nil {
		t.Fatalf("Resource a: %v", err)
	}
	if err := sh.Resource("b", func(ctx context.Context) error { return nil }, drainorder.After("a")); err != nil {
		t.Fatalf("Resource b: %v", err)
	}

	_, err := sh.Trigger(context.Background())
	if err == nil {
		t.Fatal("Trigger succeeded despite a dependency cycle; want an error")
	}
}

// TestResourceDuplicateName proves registering the same name twice is
// rejected, and that the reserved name "http" cannot be used.
func TestResourceDuplicateName(t *testing.T) {
	srv, _ := startServer(t, okHandler())
	sh := drainorder.New(srv)

	if err := sh.Resource("db", func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("first Resource(db): %v", err)
	}
	if err := sh.Resource("db", func(ctx context.Context) error { return nil }); !errors.Is(err, drainorder.ErrResourceExists) {
		t.Fatalf("second Resource(db) err = %v, want ErrResourceExists", err)
	}
	if err := sh.Resource("http", func(ctx context.Context) error { return nil }); !errors.Is(err, drainorder.ErrReservedName) {
		t.Fatalf("Resource(http) err = %v, want ErrReservedName", err)
	}
}

// TestStateMachine walks the readiness/liveness state machine end to end
// against a real server and asserts every transition a caller's probe
// handlers would depend on.
func TestStateMachine(t *testing.T) {
	srv, _ := startServer(t, okHandler())
	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(30*time.Millisecond),
		drainorder.WithDeadline(time.Second),
	)

	if sh.State() != drainorder.StateReady {
		t.Fatalf("initial state = %s, want ready", sh.State())
	}
	if !sh.IsReady() || !sh.IsLive() {
		t.Fatal("initial IsReady/IsLive must both be true")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = sh.Trigger(context.Background())
	}()

	deadline := time.Now().Add(time.Second)
	for sh.State() != drainorder.StateNotReady {
		if time.Now().After(deadline) {
			t.Fatal("state never reached not-ready")
		}
		time.Sleep(time.Millisecond)
	}
	if sh.IsReady() {
		t.Fatal("IsReady() true while not-ready")
	}
	if !sh.IsLive() {
		t.Fatal("IsLive() false during a normal drain; it should only go false when stuck")
	}

	<-done
	if sh.State() != drainorder.StateStopped {
		t.Fatalf("final state = %s, want stopped", sh.State())
	}
}

// TestTriggerTwiceReturnsError proves a second Trigger call, while one is
// already running (or after one finished), does not run the sequence again.
func TestTriggerTwiceReturnsError(t *testing.T) {
	srv, _ := startServer(t, okHandler())
	sh := drainorder.New(srv, drainorder.WithPropagationDelay(time.Millisecond), drainorder.WithDeadline(time.Second))

	if _, err := sh.Trigger(context.Background()); err != nil {
		t.Fatalf("first Trigger: %v", err)
	}
	_, err := sh.Trigger(context.Background())
	if !errors.Is(err, drainorder.ErrAlreadyTriggered) {
		t.Fatalf("second Trigger err = %v, want ErrAlreadyTriggered", err)
	}
}

// TestSecondSignalForcesExit is the rigour case for the documented escape
// hatch: a second SIGTERM delivered while a shutdown is already in progress
// must force an exit immediately, without waiting for the (deliberately
// long) propagation delay or deadline to elapse. It uses real OS signals
// sent to this test process, so it intentionally does not run in parallel
// with anything else that installs signal handlers.
func TestSecondSignalForcesExit(t *testing.T) {
	srv, _ := startServer(t, okHandler())

	exitCh := make(chan int, 4)
	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(5*time.Second),
		drainorder.WithDeadline(5*time.Second),
		drainorder.WithSignals(syscall.SIGTERM),
		drainorder.WithExit(func(code int) { exitCh <- code }),
	)

	stop := sh.Listen()
	defer stop()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send first SIGTERM: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let the first signal be observed
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send second SIGTERM: %v", err)
	}

	select {
	case code := <-exitCh:
		if code != drainorder.ExitForced {
			t.Fatalf("exit code = %d, want ExitForced (%d)", code, drainorder.ExitForced)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("forced exit never happened within 2s, despite a 5s propagation delay + 5s deadline")
	}
}

// netURLHostPort strips the "http://" prefix a startServer URL always has,
// returning a host:port dial target.
func netURLHostPort(addr string) (string, error) {
	const prefix = "http://"
	if len(addr) > len(prefix) && addr[:len(prefix)] == prefix {
		return addr[len(prefix):], nil
	}
	return "", errors.New("unexpected address format: " + addr)
}
