# Kairo

> **Kairo coordinates executions; projects control workflows. Kairo never
> owns or interprets project-specific state transitions.**

Kairo is a single-Windows-host pilot for safely coordinating cooperative ML
executions. Project, queue, and task names are opaque coordination scopes;
they are not Kairo workflow objects. SQLite records coordination facts,
providers observe physical reality, executors own process lifecycle, and the
Python SDK implements the cooperative suspend protocol.

Kairo deliberately has no task success/failure model, retry policy, DAG,
workflow condition, metric gate, artifact dependency, dynamic fan-out,
cleanup policy, or automatic resume. A terminal execution reports raw facts
such as exit code, signal, checkpoint reference, and quiescence. Project code
interprets those facts and explicitly submits any next execution with the
desired opaque input continuation.

## Start

The V3 coordination schema is intentionally fresh-only. Archive a V1/V2
database before starting this version; semantic workflow state is not migrated.

```powershell
go run ./cmd/kairo serve --config examples/kairo.toml
go run ./cmd/kairo execution submit examples/execution.toml
go run ./cmd/kairo execution list --project llm-develop
go run ./cmd/kairo project pause llm-develop
go run ./cmd/kairo project resume llm-develop
go run ./cmd/kairo resource status
```

Pause closes the selected coordination scope, fences concurrent launches, and
waits by default until all captured executions are quiesced and their leases
are released. `--no-wait` returns after the durable pause operation is stored.
Resume only reopens admission; it never recalls a suspend command or decides
which execution should run next.

`observe_only = true` inventories resources and external claims but never
reserves, launches, suspends, or releases work. It is the recommended first
deployment mode on a host with unmanaged training processes.

CPU, RAM, and disk reservations are admission accounting rather than hard OS
limits. GPU exclusion is a coordination guarantee among Kairo participants;
external processes are observed and excluded but cannot be physically fenced.

## Coordination model

- Execution request: immutable and one-shot (`waiting`, `authorized`,
  `started`, `terminal`). A request can start at most once.
- Attempt: process lifecycle facts (`authorized`, `running`, `quiescing`,
  `exited`, `quiesced`, `lost`).
- Lease: ownership coordination (`reserved`, `prepared`, `active`,
  `releasing`, `released`, `stale`, `revocation_requested`).
- Command: at-least-once cooperative `suspend` delivery (`pending`,
  `accepted`, `checkpointing`, `checkpointed`, `rejected`).
- Scope gate: admission is `open` or `closed`, with a generation fence checked
  at reservation, authorization, and activation.

Checkpoint publication does not prove process exit, and process exit does not
prove distributed quiescence. Kairo releases a lease only after every
registered process is absent and provider observations taken after that
absence show resources unclaimed. An expired or executor-lost lease becomes
stale; it is never made free based on time alone.

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

Managed attempts require `KAIRO_EXECUTION_ID`, `KAIRO_ATTEMPT_ID`,
`KAIRO_LEASE_ID`, `KAIRO_COORDINATION_EPOCH`, and `KAIRO_API_URL`. Rank zero
polls and acknowledges commands; the PyTorch adapter broadcasts the command,
runs every rank's callback, and only publishes the continuation after the
distributed barrier. Commands may be redelivered, so checkpoint publication
must remain idempotent by command ID.

## Pilot acceptance contract

1. Observe-only inspection of an existing LLM pretrain creates no lease.
2. A one-GPU job passes `run -> suspend -> checkpointed -> process exit ->
   quiesced -> lease release`; the project then explicitly submits a resumed
   execution.
3. Daemon/process failure at every transition causes neither duplicate GPU
   ownership nor loss of an acknowledged continuation.
4. A project-selected short high-priority execution can trigger cooperative
   preemption; the project decides whether and how to resume the old work.
5. Redelivery of one command cannot corrupt checkpoint publication.
6. A two-GPU DDP execution receives an all-or-nothing gang lease and all ranks
   quiesce before release.

Multi-node execution, cloud provisioning, RBAC/TLS, service supervision,
cron/fair-share scheduling, and hard CPU/RAM enforcement remain outside this
pilot.

## Verification

```powershell
go test ./...
go test -race ./...
go vet ./...
uv run --project sdk/python pytest -q
git diff --check
```

Real NVIDIA, WSL, process-kill, and DDP runs are host-specific integration
tests rather than ordinary unit tests.
