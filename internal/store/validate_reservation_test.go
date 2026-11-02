package store

import "testing"

// A reservation of two GPUs for two different requests is valid when each
// GPU meets the request it was chosen for, although not the other's.
func TestValidateReservationChecksEachGPUAgainstItsOwnRequest(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 100)
	c.observe("gpu-GPU-a-1", 24*gib, 5*gib) // too little free for the 10 GiB request
	spec := planSpec("two-requests", "p", 1, 0, 1000, 1)
	spec.Exclusive = []ExclusiveRequest{
		{Kind: "gpu", Count: 1, SameNode: true, Device: "GPU-a-1", OnExternalClaim: "wait", OnUnattributedActivity: "wait", OnStaleObservation: "wait"},
		{Kind: "gpu", Count: 1, SameNode: true, MinObservedFreeMemoryBytes: 10 * gib, OnExternalClaim: "wait", OnUnattributedActivity: "wait", OnStaleObservation: "wait"},
	}
	c.submit(spec)
	reservation := c.reserve("exec-a")
	if reservation == nil || len(reservation.Resources) != 2 {
		t.Fatalf("reservation: %+v", reservation)
	}
	if err := c.store.ValidateReservation(c.ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatalf("a valid reservation was refused: %v", err)
	}
	// The pinned GPU's memory drops below nothing it is asked for, but the
	// other one no longer has 10 GiB free: refused.
	c.observe("gpu-GPU-a-0", 24*gib, 4*gib)
	if err := c.store.ValidateReservation(c.ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err == nil {
		t.Fatal("a reservation whose GPUs no longer meet their requests was accepted")
	}
}
