// Package hwbench is the hardware bench of Kairo: the driver a bench run's
// execution runs (`kairo hardware bench-run`), and the selection of an
// operating point from the profile benches leave.
//
// The driver runs inside the bench's attempt and talks to the daemon with
// the attempt token (Daemon, client.go): it can see and change only its own
// run and its own lease's bench setting. For each kind it starts the
// measurement primitive (`kairo bench run`, kairo-bench-gpu) once, reads its
// kairo.bench/v1 records, and uses the stream itself as its clock: it asks
// for a setting, waits until the device's controller reports that setting
// applied, lets the device settle, then measures over a window of intervals.
// References at the defaults open, split and close every sweep; a kind whose
// references disagree is not trusted. Every setting it asks for belongs to
// its lease, so the lease's release returns the device to its defaults even
// when the driver does not get to clear it.
//
// A run is a preemptible task: before every point the driver polls its
// attempt's commands, as an SDK's safe point does, and leaves at a suspend
// with a checkpoint naming the kinds it is done with (Continuation); the
// task's continuation resumes the run with the others. A stop signal (the
// graceful phase of a force stop) leaves at once, publishing nothing.
//
// Selection (select.go) is pure: from a device's curves it picks, for a
// tolerance, the setting that keeps every candidate kind within it for the
// least energy per unit of work, and gives the fallback that holds without a
// profile.
package hwbench
