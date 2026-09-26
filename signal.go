package drainorder

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// defaultSignals returns the signals a [Shutdown] reacts to when
// [WithSignals] is not supplied: SIGTERM (the one Kubernetes sends) and
// SIGINT (Ctrl-C, for running locally). Both are Unix signals; this package
// targets the containerized-Linux environments graceful shutdown actually
// matters for.
func defaultSignals() []os.Signal {
	return []os.Signal{syscall.SIGTERM, syscall.SIGINT}
}

// Listen attaches signal handlers for the configured signal set (see
// [WithSignals]) and returns immediately without blocking. When one of those
// signals arrives, it runs the full shutdown sequence exactly as
// [Shutdown.Trigger] would, then calls the configured Exit function (see
// [WithExit]) with [ExitOK] or [ExitUnclean] depending on the resulting
// Report.
//
// A second matching signal, delivered while that shutdown is still in
// progress, skips the drain entirely and calls Exit with [ExitForced]
// immediately. This is the documented "I don't care, kill it now" escape
// hatch for an operator who has decided the graceful path is taking too
// long — it is not a bug.
//
// Calling Listen more than once, or after the signal set is empty, is a
// no-op; the returned function stops signal delivery and may be called to
// release the handler (for example in tests).
func (s *Shutdown) Listen() (stop func()) {
	s.mu.Lock()
	if s.sigCh != nil {
		ch := s.sigCh
		s.mu.Unlock()
		return func() { signal.Stop(ch) }
	}
	if len(s.signals) == 0 {
		s.mu.Unlock()
		return func() {}
	}
	ch := make(chan os.Signal, 2)
	s.sigCh = ch
	sigs := append([]os.Signal(nil), s.signals...)
	s.mu.Unlock()

	signal.Notify(ch, sigs...)

	go func() {
		sig, ok := <-ch
		if !ok {
			return
		}

		go func() {
			if sig2, ok := <-ch; ok {
				s.emit(Event{Type: EventForcedExit, Signal: sig2})
				s.exit(ExitForced)
			}
		}()

		report, err := s.trigger(context.Background(), ReasonSignal, sig)
		if err != nil {
			s.exit(ExitUnclean)
			return
		}
		if report.OK {
			s.exit(ExitOK)
		} else {
			s.exit(ExitUnclean)
		}
	}()

	return func() { signal.Stop(ch) }
}
