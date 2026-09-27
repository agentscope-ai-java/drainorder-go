// Command drain-demo measures requests lost in the window right after a
// shutdown signal, naive versus drainorder, against real *http.Server
// instances on real ports with real client traffic. Run it with:
//
//	go run ./examples/drain-demo
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	drainorder "github.com/agentscope-ai-java/drainorder-go"
)

const (
	signalDelay     = 200 * time.Millisecond // traffic runs this long before the "signal" fires
	windowDur       = 150 * time.Millisecond // window whose requests we count
	tailBuffer      = 150 * time.Millisecond // traffic continues this long past the window, unmeasured
	requestInterval = 2 * time.Millisecond   // one request fired every tick, from a fresh goroutine

	fixPropagation = 300 * time.Millisecond // propagation delay used for the "drainorder" run
)

type counters struct {
	sent, ok, failed int64
}

func (c *counters) String() string {
	return fmt.Sprintf("sent %4d   ok %4d   failed %4d", c.sent, c.ok, c.failed)
}

func listen() net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	return ln
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

// runTraffic fires one request every requestInterval, each from its own
// goroutine so a slow or refused request never delays the next tick, from
// now until totalEnd. Only requests fired inside [signalAt, signalAt+windowDur)
// are counted — that's "the 150ms window right after the signal." Every
// request uses a fresh, non-keepalive connection, so a refusal at the
// listener is what actually fails the request, the same way a real client
// on a real network would see it.
func runTraffic(addr string, signalAt time.Time, totalEnd time.Time) *counters {
	c := &counters{}
	windowEnd := signalAt.Add(windowDur)
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   2 * time.Second,
	}

	var wg sync.WaitGroup
	ticker := time.NewTicker(requestInterval)
	defer ticker.Stop()
	for fireTime := range ticker.C {
		if !fireTime.Before(totalEnd) {
			break
		}
		inWindow := !fireTime.Before(signalAt) && fireTime.Before(windowEnd)
		wg.Add(1)
		go func(count bool) {
			defer wg.Done()
			resp, err := client.Get(addr)
			if resp != nil {
				defer resp.Body.Close()
			}
			if !count {
				return
			}
			atomic.AddInt64(&c.sent, 1)
			if err == nil && resp.StatusCode == http.StatusOK {
				atomic.AddInt64(&c.ok, 1)
			} else {
				atomic.AddInt64(&c.failed, 1)
			}
		}(inWindow)
	}
	wg.Wait()
	return c
}

// runNaive simulates the shutdown code most examples online write: close the
// server the instant the signal arrives, with no propagation delay at all.
func runNaive() *counters {
	ln := listen()
	srv := &http.Server{Handler: okHandler()}
	go srv.Serve(ln)
	addr := "http://" + ln.Addr().String()

	t0 := time.Now()
	signalAt := t0.Add(signalDelay)
	totalEnd := signalAt.Add(windowDur).Add(tailBuffer)

	resCh := make(chan *counters, 1)
	go func() { resCh <- runTraffic(addr, signalAt, totalEnd) }()

	time.Sleep(time.Until(signalAt))
	_ = srv.Close() // the naive move: stop accepting and drop everything, now

	return <-resCh
}

// runDrainOrder runs the exact same traffic pattern against a server managed
// by this package, with a real (non-zero) propagation delay.
func runDrainOrder() *counters {
	ln := listen()
	srv := &http.Server{Handler: okHandler()}
	go srv.Serve(ln)
	addr := "http://" + ln.Addr().String()

	sh := drainorder.New(srv,
		drainorder.WithPropagationDelay(fixPropagation),
		drainorder.WithDeadline(2*time.Second),
	)

	t0 := time.Now()
	signalAt := t0.Add(signalDelay)
	totalEnd := signalAt.Add(windowDur).Add(tailBuffer)

	resCh := make(chan *counters, 1)
	go func() { resCh <- runTraffic(addr, signalAt, totalEnd) }()

	time.Sleep(time.Until(signalAt))
	go sh.Trigger(context.Background())

	return <-resCh
}

func main() {
	fmt.Println("requests sent in the 150ms window right after the signal:")
	fmt.Println()

	naive := runNaive()
	fmt.Printf("naive        %s\n", naive)

	order := runDrainOrder()
	fmt.Printf("drainorder   %s\n", order)
}
