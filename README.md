# drainorder

Graceful shutdown for a `net/http.Server`, focused on the two things almost
every graceful-shutdown snippet gets wrong.

```
requests sent in the 150ms window right after the signal:

naive        sent   75   ok    0   failed   75
drainorder   sent   75   ok   75   failed    0
```

That's `go run ./examples/drain-demo`, against two real `*http.Server`
instances on real ports, hit with real client traffic paced at one request
every 2ms. "naive" is `server.Close()` called the instant the signal arrives
— the way most examples online do it. "drainorder" is the exact same server,
the exact same traffic, with one difference: a 300ms propagation delay before
it stops accepting. Every single request naive received in that window was
refused; every single one drainorder received was served. The numbers are
deterministic here because the traffic is paced on a ticker rather than a
tight loop — run it yourself and you'll get the same 75/75 split, because
that's exactly how many ticks land inside a 150ms window at a 2ms interval.

## Is this the package you need?

If all you want is "close the `http.Server` when `SIGTERM` arrives," you
don't need this — `server.Shutdown(ctx)` in a signal handler is fine for
that. This package exists for two specific things that plain
`Server.Shutdown`, and most of the libraries built around it, get wrong.

## 1. The readiness-propagation gap

This is the bug almost every "graceful shutdown" example has, including a lot
of production code: the kubelet sends `SIGTERM` to your container at the
*same moment* the Service's endpoint controller starts removing the pod from
Endpoints — and that removal takes time to reach every kube-proxy and every
load balancer in front of it. Some of them are still sending you traffic for
the next several hundred milliseconds to several seconds, entirely correctly,
because they don't know yet that you're going away.

`http.Server.Shutdown` has no concept of this. Straight from its own doc
comment: it works by first closing all open listeners, then closing all idle
connections, and then waiting indefinitely for connections to return to idle
and shut down. There is no pre-shutdown grace period in there at all — it
starts refusing new connections in the same instant it's called. A server
that calls `Shutdown` the moment `SIGTERM` arrives refuses traffic that was
correctly routed to it a moment before — connection-reset errors on a random
slice of requests during every single deploy. Teams see this, don't connect
it to the deploy, and go looking for a client bug or a network flake instead.

The fix is a specific sequence, not a faster `Shutdown` call:

1. **Fail the readiness probe immediately.** Tell Kubernetes to stop routing
   you new traffic. This starts the propagation clock but does not, by
   itself, change anything about what the server does yet.
2. **Keep serving normally for a propagation delay.** Readiness failing takes
   time to reach everything that was still about to send you a request.
   During this window the server behaves exactly as it did before the
   signal.
3. **Only then** stop accepting new connections and drain in-flight ones —
   this is the point where `Server.Shutdown` finally gets called.

`WithPropagationDelay` sets that window, and it is the option that matters
most in this whole package.

### Why it cannot be zero

At a propagation delay of zero, steps 1 and 3 happen in the same instant.
You've reproduced the exact bug this package exists to fix: the readiness
probe fails and the listener closes at the same moment, so anything already
routed to you in that instant gets a refused or reset connection. There is no
version of "propagate a global state change across a fleet of proxies" that
completes in 0ms. Set it to something a real network round-trip and probe
interval can fit inside — a few seconds is typical — and keep it comfortably
less than your pod's `terminationGracePeriodSeconds` minus however long the
rest of the drain (in-flight requests, resource close) can take.

`http.Server.Shutdown` also explicitly does not wait for hijacked
connections — WebSockets, in practice. This package doesn't change that
either; see "What it does not do" below.

## 2. Dependency-ordered shutdown

Resources have to close in dependency order: stop accepting new work, let
in-flight work finish, *then* close the database — closing the pool while
requests are still running turns a clean shutdown into a burst of "use of
closed connection" errors on whatever was still in flight. And a shutdown
that never finishes is just as broken as one that's too abrupt: something has
to enforce a deadline and report clearly what didn't close in time, rather
than hang the pod until Kubernetes SIGKILLs it with no report at all.

Two active, popular Go libraries in this space each get half of this right,
never both:

- **`oklog/run`** runs all of its actors' shutdown functions concurrently as
  soon as one of them returns — there's no way to say "close the database
  after the request drain." It's a great small primitive for coordinating a
  set of independent goroutines, but it does not model dependency order at
  all.
- **`uber-go/fx`**'s `Lifecycle` runs `OnStop` hooks in reverse registration
  order, which *is* a real ordering primitive — but it means adopting fx's
  whole dependency-injection framework for your application just to get
  ordered shutdown, and it still has nothing to say about the readiness-
  propagation gap above.

The "fail readiness, sleep, then shut down" sequence in part 1 exists only as
blog-post advice as far as this package's author could find — no packaged
library for it turned up during research. This package packages both halves
together, because in practice they're the same problem: a shutdown sequence
that has to happen in the right order, with a deadline.

`Resource` models this as phases: the request drain is always phase zero,
every resource you register runs after it by default (`After()` with no
arguments if it's genuinely independent), and resources whose dependencies
are satisfied at the same point run concurrently. One deadline covers the
whole thing, measured from the end of the propagation delay; whatever hasn't
closed when it expires is reported as `timed-out` and the process exits
anyway.

## Install

```sh
go get github.com/__GH_OWNER__/drainorder-go
```

Built and tested against Go 1.27; `go.mod` declares `go 1.27.1`. Standard
library only — no runtime dependencies.

## Use

```go
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/__GH_OWNER__/drainorder-go"
)

func main() {
	mux := http.NewServeMux()
	server := &http.Server{Addr: ":8080", Handler: mux}

	shutdown := drainorder.New(server,
		drainorder.WithPropagationDelay(5*time.Second), // keep serving normally for 5s after SIGTERM
		drainorder.WithDeadline(30*time.Second),        // request drain + resource close, combined
	)

	shutdown.Resource("db", func(ctx context.Context) error {
		return db.Close()
	})
	shutdown.Resource("redis", func(ctx context.Context) error {
		return redisClient.Close()
	})
	shutdown.Resource("tracing", func(ctx context.Context) error {
		return tracerProvider.Shutdown(ctx)
	}, drainorder.After()) // independent of the request drain

	shutdown.Listen() // attach SIGTERM / SIGINT handlers, non-blocking

	// Wire the state into your own probe handlers:
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !shutdown.IsReady() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		if !shutdown.IsLive() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	server.ListenAndServe()
}
```

A second `SIGTERM` (or `SIGINT`) while a shutdown is already in progress
skips the drain entirely and exits immediately — that's the documented
escape hatch for "I don't care, kill it now," not a bug.

### Without `Listen`

`Listen` just attaches signal handlers. Call `shutdown.Trigger(ctx)` yourself
if you want to drive shutdown from something else — a message queue's stop
signal, a test, an admin endpoint:

```go
report, err := shutdown.Trigger(context.Background())
if err != nil {
	log.Fatal(err)
}
if !report.OK {
	log.Printf("unclean shutdown: %+v", report)
}
```

## Options

| Option | Default | What it controls |
| --- | --- | --- |
| `WithPropagationDelay` | 5s | Time spent serving normally, readiness already failing, before the drain starts. See above — this is not a "grace period," it's a wait for external state to catch up. |
| `WithDeadline` | 30s | Total budget for the request drain plus every resource phase, measured from the end of the propagation delay. |
| `WithSignals` | `SIGTERM`, `SIGINT` | Which signals trigger shutdown. Pass no arguments to disable and drive everything via `Trigger`. |
| `WithOnEvent` | no-op | Called for every lifecycle `Event` (`signal`, `not-ready`, `request-drain-end`, `resource-end`, `shutdown-complete`, `forced-exit`). Wire it to your logger. |
| `WithExit` | `os.Exit` | Called to end the process after a signal-driven shutdown. Override in tests — the default terminates the test binary. |

`Resource(name, close, opts...)` — `close` is a `func(ctx context.Context)
error`. By default a resource runs `After("http")`, meaning "after the
request drain." Pass `After()` with no names for something safe to close in
parallel with it, or name other resources to build a deeper dependency graph.
Resources whose dependencies are satisfied at the same point run
concurrently. An unknown dependency name or a dependency cycle is reported as
an error from `Trigger`, before anything is closed.

## Exit codes

| Constant | Value | Meaning |
| --- | --- | --- |
| `ExitOK` | 0 | Every phase closed cleanly. |
| `ExitUnclean` | 1 | Shutdown ran to completion, but the report shows a failed or timed-out phase — see the report for which one. |
| `ExitForced` | 2 | A second signal arrived mid-shutdown; the drain was skipped. |

These are only used by the signal-driven path (`Listen` calling the
configured `Exit` function). A manually driven `Trigger` call just returns
the `Report` and lets you decide.

## Readiness / liveness

- `shutdown.IsReady()` — true only before shutdown starts. Wire it into your
  readiness probe; this is what actually removes you from load-balancer
  rotation.
- `shutdown.IsLive()` — true for the entire drain, including resource close.
  The process is healthy the whole time it's shutting down cleanly; it only
  goes false if shutdown is somehow still running well past its own
  deadline — a bug in a resource, or in this package — which liveness going
  false is meant to catch, not "this process is shutting down."
- `shutdown.State()` — `StateReady | StateNotReady | StateStopped`.

## The report

`Trigger` (and the call `Listen` drives internally on a signal) returns a
`*Report`:

```go
type Report struct {
	OK                bool
	Reason            Reason // ReasonSignal | ReasonManual
	Signal            os.Signal
	PropagationDelay  time.Duration
	Request           PhaseReport // Name == "http"
	Resources         []PhaseReport
	TotalDuration     time.Duration
}

type PhaseReport struct {
	Name     string
	Status   PhaseStatus // StatusClosed | StatusFailed | StatusTimedOut
	Duration time.Duration
	Err      error
}
```

`OK` is false if the request drain or any resource failed or timed out. This
is the "clear report of what failed to close" — read it from `WithOnEvent`
for `EventShutdownComplete` if the process is about to exit and you need to
get it into a log line before that happens.

## What it does not do

- **It does not pick your propagation delay for you.** The right value
  depends on your kube-proxy sync period, your load balancer's health-check
  interval, and your `terminationGracePeriodSeconds`. 5s is a reasonable
  default, not a universal one.
- **It is not a process manager.** It doesn't restart your process, doesn't
  manage systemd/Kubernetes concerns beyond the shutdown sequence itself, and
  doesn't touch `SIGHUP` or config reload.
- **It doesn't track WebSocket/SSE connections specially.** `http.Server.
  Shutdown` explicitly leaves hijacked connections alone; when the total
  deadline expires this package force-closes the listener and remaining
  connections via `Server.Close()`, which cuts those off like anything else.
  If you need to tell long-lived clients to reconnect elsewhere first, do
  that in a resource registered with `After()` (independent), so it runs in
  parallel with the HTTP drain instead of after it.
- **Signals are Unix-only.** `WithSignals`' defaults (`SIGTERM`, `SIGINT`)
  don't exist on Windows; this package targets the containerized-Linux
  environments graceful shutdown actually matters for. `Trigger` itself has
  no OS dependency and works anywhere.
- **A resource that ignores its context can still leak a goroutine.** The
  deadline stops `Trigger` from waiting on it and reports it as timed out,
  but the goroutine running that `CloseFunc` keeps running in the
  background if it never returns. This is unavoidable without forcibly
  killing goroutines, which Go doesn't support — write `CloseFunc`s that
  respect `ctx`.

## Test

```sh
go test -race ./...
```

Every test in the suite runs against a real `*http.Server` on a real,
OS-assigned localhost port, with real short timers (tens to low hundreds of
milliseconds) — no mocked server, no mocked clock. This includes: a request
arriving during the propagation delay being served, an in-flight request
completing, a new connection after drain starts being refused, the resource
phase waiting for the request phase, an independent resource running during
the propagation delay, the deadline firing and naming the offending
resource, and a second real `SIGTERM` forcing an immediate exit.

## Develop

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .              # must print nothing
go run ./examples/drain-demo
```

## License

MIT
