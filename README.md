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
