package drainorder

// State is the readiness/liveness state of a [Shutdown]. It moves forward
// only, in one direction: Ready -> NotReady -> Stopped.
type State int32

const (
	// StateReady means no shutdown has been triggered. Wire [Shutdown.IsReady]
	// into your readiness probe: this is the only state in which it returns
	// true, and it is what keeps you in load-balancer rotation.
	StateReady State = iota
	// StateNotReady means shutdown has been triggered: the readiness probe
	// should already be failing, but the server is still serving requests
	// normally for the propagation delay, and then draining in-flight ones.
	StateNotReady
	// StateStopped means the request drain and every resource phase have
	// finished (successfully, with a failure, or by timing out) and the
	// process is about to exit.
	StateStopped
)

// String returns the lower-kebab-case name used in events and reports, e.g.
// "not-ready".
func (s State) String() string {
	switch s {
	case StateReady:
		return "ready"
	case StateNotReady:
		return "not-ready"
	case StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}
