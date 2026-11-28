package executor

import "time"

// Gang members' containers (issue #47, phase 3): each is on its
// generation's segment of the fabric through a second NIC, with its
// address there. The runner puts the NIC in (Incus: a bridged NIC on the
// segment's bridge, made with the container; wslc: the container connected
// to the segment's network once it exists), and the container's init gives
// it its address where the runtime did not, then waits at the barrier
// (gangBarrier) before it starts the argv: until it has reached every other
// member on the segment and every other member has reached it. NCCL and
// gloo are told the NIC (NCCL_SOCKET_IFNAME, GLOO_SOCKET_IFNAME).

const (
	// gangNIC is the name of an Incus member's NIC on the segment.
	gangNIC = "gang0"
	// gangBarrierPort is where members say hello to each other on the
	// segment before the argv starts (not torchrun's 29400 or 29500).
	gangBarrierPort = 29399
	// gangAddressWait bounds how long the init waits for its NIC on the
	// segment; gangBarrierWait how long for the other members (their
	// images may still be coming: the members pull theirs apart).
	gangAddressWait = 2 * time.Minute
	gangBarrierWait = 30 * time.Minute
	// segmentWait bounds putting a segment on the node at launch.
	segmentWait = 2 * time.Minute
	// shimGangUnmet: the init gave up before the argv started: its NIC on
	// the segment never came, or a member was not reached (or did not reach
	// it) in time. The gang's other members are then stopped (one fate).
	shimGangUnmet = 124
)
