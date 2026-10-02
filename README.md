# Kairo

> **Kairo owns generic Project/Task orchestration semantics, but never
> interprets project-specific state.**

Kairo is a single-Windows-host pilot for safely orchestrating cooperative ML
work. It has two layers. A small declarative layer stores a ProjectSpec,
reconciles a static Task DAG, and applies an explicitly selected generic task
policy. The existing V3 coordination layer receives immutable Execution
requests and continues to own Attempt, Lease, Command, provider, and executor
mechanics.

Task is a durable logical object and can have a sequence of immutable
Executions. Execution, Attempt, and Lease remain distinct physical lifecycle
objects. At the lower V3 boundary, project, queue, and task names are still
opaque coordination scopes; the orchestration layer deliberately maps its
logical Tasks onto those scopes.

The orchestration layer understands only generic facts: dependencies are
satisfied, an execution exited, a continuation is permitted by the selected
policy, or a task succeeded, failed, or became blocked. It does not
interpret metrics, model quality, artifact contents, datasets, or other
domain-specific state. DAG topology is static: there is no dynamic fan-out,
runtime DAG mutation, artifact-derived topology, metric gate, cron scheduling,
or cleanup language.

## Start

The V3 coordination schema is intentionally fresh-only. Archive a V1/V2
database before starting this version; semantic workflow state is not migrated.

```powershell
go run ./cmd/kairo serve --config examples/kairo.toml
go run ./cmd/kairo project validate examples/project-spec.toml
go run ./cmd/kairo project apply --api http://127.0.0.1:7474 examples/project-spec.toml
go run ./cmd/kairo project status --api http://127.0.0.1:7474 example-project
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

`project apply` stores the declaration once; the daemon performs subsequent
reconciliation. The ProjectSpec contract lives at `/orchestration/v1` and is
versioned independently of `/v2/executions`, whose V3 coordination semantics
are unchanged. Applying the same normalized declaration is idempotent. A
declared Task is immutable; a later complete ProjectSpec may add Tasks but
cannot mutate an existing Task, and omission does not cancel it.

The initial `RunToCompletion@v1` policy waits for every dependency to succeed,
submits the Task's initial Execution, and marks the Task successful only after
a zero exit, Attempt quiescence, and Lease release. A checkpointed cooperative
suspend with an opaque continuation creates the next immutable Execution.
Lost Attempt identity blocks the Task, and an unsuccessful dependency blocks
its dependants. See `examples/project-spec.toml` for the declaration format.
A top-level `[defaults]` table (`queue`, `policy`, `execution`) is merged into
every Task by the CLI before validation: the Task's own value wins, tables
merge key by key, arrays and scalars are replaced, and an omitted `depends_on`
becomes `[]`. Digests are those of the fully written Task, so `[defaults]`
never changes what the daemon stores.

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

### Node quarantine

`kairo node quarantine <NODE_ID|--all> --reason ...` stops all work on a node
(or every node) until `kairo node release <NODE_ID|--all>`:

- its executors reserve nothing and trigger no priority preemption;
- a running checkpointable attempt gets a `suspend` command (origin
  `node_quarantine`) and later continues from its checkpoint;
- any other running attempt is terminated by its executor, launcher and
  descendants (Windows `taskkill /T /F`; Linux SIGTERM then SIGKILL to the
  attempt's process group), and its task runs again from the start
  (orchestration reason `quarantine_restart`) instead of failing.

`kairo node quarantine-status` shows each quarantine with the attempts still
alive on it; the quarantine has taken full effect once that count is zero.
Every step is recorded as a coordination event.

### Force stop

`kairo task pause --force|-f [--grace 30s] [--reason ...] PROJECT QUEUE TASK`
(also `queue` / `project pause --force`) is a pause operation that ends what
it captures instead of suspending it, checkpointable or not:

- a started execution's attempt gets a force stop that its executor (in the
  daemon or a node agent) carries out on the whole process tree: a graceful
  stop first (Windows: CTRL_BREAK to the launcher's process group; Linux and
  WSL: SIGTERM to the process group and to every process carrying the
  attempt's `KAIRO_ATTEMPT_ID`), then, after the grace period, a kill of
  everything left (Windows: the attempt's job object and `taskkill /T /F`;
  Linux/WSL: SIGKILL). The execution ends with terminal cause
  `force_stopped`; a suspend in progress is not waited for.
- an execution that has not started is withdrawn (`withdrawn_before_start`),
  a reserved lease revoked as for any pause.
- leases are released only through the ordinary quiescence proof, and the
  operation is `quiesced` once the executors also confirm the trees are gone.
- the task ends in state `stopped`: no continuation or restart is planned, and
  its dependants are blocked as for a failed task. The task result records the
  operation, actor and reason.

Force stops are delivered like other executor work (`force-stop-orders` /
`force-stop-ack` on the agent API, acknowledged `signalled` then
`terminated`). One that no executor picks up within 30 s blocks its pause
target with `agent_lacks_force_stop` (the node agent is polling but predates
force stop: upgrade it) or `executor_unreachable` (it has not been seen). A
daemon predating force stop rejects `--force` instead of pausing without it.

### Gang executions (multi-node)

`[tasks.execution.gang]` runs a task as `size` ranks on distinct nodes (DDP
training across hosts, a pipeline-parallel server). Every rank asks for the
task's resources on its own node; `[[tasks.execution.gang.ranks]]` overrides
`argv`, `cwd` or `executor` labels of single ranks:

```toml
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
argv = ["/root/.local/bin/uv", "run", "train.py"]
cwd = "/srv/project"
executor = { labels = { environment = "linux" } }
```

- Each rank is an ordinary execution; rank 0 (the leader) is the one
  orchestration tracks, and the task's outcome is read over every rank.
- Placement is all or nothing: every rank gets a lease on a distinct node in
  one transaction. Rank 0's node needs an `interconnect_addr` label.
- No rank launches before every rank is prepared. A placement not prepared
  within two minutes, or losing a lease before launch, is aborted and the ranks
  wait again.
- Every rank is launched with `RANK`, `WORLD_SIZE`, `MASTER_ADDR` (rank 0's
  `interconnect_addr`), `MASTER_PORT` (29500-29999, free on that node) and
  `KAIRO_GANG_*`; an executor labelled `interconnect_ifname` also sets
  `NCCL_SOCKET_IFNAME` and `GLOO_SOCKET_IFNAME`.
- One fate: a rank that stops other than by exit 0 or a checkpoint gets the
  others terminated; a `suspend` to one rank (pause, preemption, quarantine)
  is sent to every rank. A rank that failed to launch after others started
  restarts the gang (orchestration reason `gang_restart`).
- A waiting gang does not preempt; its running ranks can be preempted.

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

### Low-level project-owned controller policy

Projects submitting directly to the V3 execution API can still opt into
`ResumeAfterKairoPreemption`. The policy snapshot and canonical submission are
written to the project's controller journal before the initial API call:

```python
from kairo_sdk import ControllerJournal, KairoControllerClient, ProjectController

controller = ProjectController(
    KairoControllerClient("http://127.0.0.1:7474"),
    ControllerJournal("local/llm-controller.db"),
)
execution_id = controller.submit_with_resume_after_kairo_preemption(
    activity_id="jalm-300m-v3",
    project="llm-develop",
    submission=execution_request,
)
controller.run_forever()
```

Use one controller journal per project; activity IDs are local to that
project-owned journal.

Only a `priority_preemption` command with an acknowledged continuation for
that exact command, a quiesced attempt, and a released lease can trigger the
policy. The controller journals a deterministic decision before submitting
the successor. API retries reuse the same client request ID, so controller
crashes and complete event replay cannot create another successor. Event
cursors are therefore only an optimization; the durable decision is the
correctness boundary. Delegation validity comes from the immutable policy
snapshot attached to the logical activity's current execution, not from a
server-owned workflow state or semantic default.

## Pilot acceptance contract

1. Observe-only inspection of an existing LLM pretrain creates no lease.
2. A one-GPU job passes `run -> suspend -> checkpointed -> process exit ->
   quiesced -> lease release`; an explicitly selected task policy may then
   submit a resumed immutable Execution.
3. Daemon/process failure at every transition causes neither duplicate GPU
   ownership nor loss of an acknowledged continuation.
4. A project-selected short high-priority execution can trigger cooperative
   preemption; a versioned Task policy decides whether to resume orchestrated
   work, while direct V3 users retain the project-owned controller option.
5. Redelivery of one command cannot corrupt checkpoint publication.
6. A static Task DAG submits a node only after all dependencies succeed, and
   replay cannot create a duplicate Execution for one policy decision.
7. A two-GPU DDP execution receives an all-or-nothing gang lease and all ranks
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
