# Kairo

Kairo coordinates revisioned ML task queues on one Windows host. SQLite is the
source of truth for plans, leases, attempts, authorizations, commands, epochs,
and the append-only event log. Provider observations remain the source of truth
for physical GPU, memory, CPU, disk, and process state.

## Start

```powershell
go run ./cmd/kairo serve --config examples/kairo.toml
go run ./cmd/kairo plan validate --json examples/queue.toml
go run ./cmd/kairo plan apply --create --expected-revision 0 examples/queue.toml
go run ./cmd/kairo resource status --json
```

Every later apply supplies the current revision. Reapplying the same normalized
digest is idempotent. A different manifest with a stale expected revision gets
HTTP 409.

`observe_only = true` inventories resources, observations, and external claims
without reserving or launching work. This is the recommended first deployment
mode on a host with unmanaged training processes.

CPU, RAM, and disk reservations are admission accounting, not hard operating
system limits. GPU exclusion is a coordination guarantee among Kairo
participants; external processes are detected and excluded but cannot be
physically fenced.

## Scope boundary

Kairo is a pilot coordination layer for cooperative ML jobs on one Windows
host, not a general workflow engine. The manifest may express static execution
dependencies, but project code owns the reason a task should run. Conditions,
gates, artifact semantics, dynamic fan-out, and cleanup policy intentionally
remain outside the manifest and are rejected as unknown fields.

Multi-node execution, cloud provisioning, RBAC/TLS, service supervision,
cron/fair-share scheduling, and hard CPU/RAM enforcement are also outside the
pilot contract.

## Pilot acceptance contract

The pilot is considered proven only after these scenarios pass with real
workloads:

1. Observe-only inventory of an existing LLM pretrain never creates a lease.
2. A one-GPU job completes checkpoint request, publication, process exit,
   quiescence, lease release, and continuation resume in that order.
3. Daemon/process failure at each transition never permits duplicate GPU
   ownership or loses an acknowledged continuation.
4. A short high-priority job preempts a long job and the long job resumes.
5. Redelivery of one command cannot change its published continuation.
6. A two-GPU DDP job receives an all-or-nothing gang lease and quiesces all
   registered ranks before reconciliation releases it.

The store and provider tests cover the deterministic portions of this contract;
real NVIDIA, fpmvslm, process-kill, and DDP runs remain pilot integration tests.

## Worker SDK

```python
from kairo_sdk import AttemptSession, ProgressEnvelope, SuspendResult

with AttemptSession.from_environment() as kairo:
    for step in train():
        if kairo.safe_point(
            progress=ProgressEnvelope("step", step, total_steps, detail={"loss": loss}),
            checkpoint=lambda command: SuspendResult(save_checkpoint(command.command_id)),
        ):
            break
```

The context manager sends rank-0 heartbeats in the background every ten
seconds. Safe points handle at-least-once commands separately. The PyTorch
`DistributedAdapter` lazily imports PyTorch, broadcasts commands from rank 0,
and acknowledges a checkpoint only after every rank's callback and barrier.

## Verification

```powershell
go test ./...
go test -race ./...
uv run --project sdk/python pytest -q
```

Actual NVIDIA and WSL launches are intentionally outside normal tests and
should run as a host-specific integration profile.
