# Execution coordination redesign

Status: implemented as the V3 runtime model; host-specific pilot acceptance
tests remain.

## Authority boundary

> **Kairo coordinates executions; projects control workflows. Kairo never
> owns or interprets project-specific state transitions.**

Kairo must not need to understand a project's state machine in order to
schedule, pause, quiesce, or resume its executions.

A project controller is authoritative for all semantic decisions, including:

- what work exists and what should happen next;
- dependencies, gates, stages, and completion;
- whether an exit is success, failure, retryable, or expected;
- whether and when to submit another execution;
- which continuation belongs to that new execution.

Kairo is authoritative only for execution coordination facts:

- which execution request is waiting or has received launch authority;
- which processes constitute an attempt;
- which resources are observed, reserved, leased, or stale;
- whether a suspend command was acknowledged and checkpointed;
- whether every process exited and an attempt quiesced;
- whether a coordination scope admits new executions.

Project names and optional queue/task names are coordination scopes. Their
presence in Kairo does not make them workflow entities and gives them no
semantic lifecycle.

## States Kairo must not own

The live Kairo model has no states or transitions named:

```text
task succeeded
task failed
task retrying or backoff
workflow complete
dependency satisfied
next stage
retired task
```

Exit code, signal, checkpoint reference, timestamps, and process absence are
facts. Their meaning belongs to the project controller.

Kairo Server never automatically retries a process which started, never
propagates a downstream revision, and never decides to resubmit a quiesced
execution. A project may explicitly delegate a versioned policy to the
project-owned SDK controller; the SDK then journals and executes that declared
decision without transferring semantic authority to the server.

## Coordination scopes and admission gates

Scopes form an optional hierarchy:

```text
project
  queue
    task
```

`queue` and `task` are opaque project-provided keys retained only for grouping,
filtering, admission, and pause fan-out. Kairo does not infer a queue or task
state from executions in that scope.

Each scope owns only a coordination gate:

```text
admission: open | closed
generation: monotonically increasing integer
```

An execution request may receive a new lease or launch authorization only when
every gate in its scope path is open. Closing a parent gate does not overwrite
child gates. Reopening a project therefore cannot reopen a queue or task gate
that was closed independently.

Status uses coordination language:

```text
admission=open
admission=closed
active_attempts=N
quiescing_attempts=N
pause_operation=requested|quiescing|quiesced|blocked
```

It never reports that the project itself is running, failed, completed, or
paused as a semantic workflow state.

## Execution request

A project controller submits one immutable execution request for one permitted
process execution:

```text
execution_requests
  id
  client_request_id      project-provided idempotency key
  scope_id               project with optional queue/task descendants
  state                  waiting | authorized | started | terminal
  argv
  cwd
  executor_selector
  resource_request
  priority
  checkpointable
  preemptible
  input_continuation_ref opaque, optional
  submitted_at
  authorized_at
  started_at
  terminal_at
```

The request state is a coordination fact, not a workflow result. `terminal`
means only that this request cannot start another process. The associated
attempt records raw exit information.

Before launch authorization, resource and provider races may leave a request
waiting. Once authorization is issued, Kairo never returns that request to
waiting and never executes it a second time. A coordination failure after
authorization becomes a terminal fact for the project controller to interpret.

Submitting the same `client_request_id` with the same normalized request is
idempotent. Reusing it with different content is rejected. Changing a command,
resource request, or continuation requires a new execution request.

A project may withdraw a request only while it has not started. Withdrawal is
a coordination revocation, not a semantic cancel operation, and makes the
request terminal with a raw `withdrawn_before_start` cause. A started execution
is stopped through scope pause and cooperative quiescence, never through a
worker `cancel` command.

## Attempt

An attempt represents the physical process set created for one execution
request:

```text
authorized -> running -> quiescing -> exited -> quiesced
                    \----------------> exited -> quiesced
authorized/running/quiescing -> lost
```

Recorded facts include:

- launch authorization and coordination epoch;
- executor and process identities for every rank;
- heartbeat and opaque progress payload;
- checkpoint acknowledgement and opaque continuation reference;
- launcher exit, rank process absence, exit code, and signal;
- quiescence timestamp.

`exited` means the launcher produced a terminal result. `quiesced` additionally
means every registered process identity was verified absent. Launcher exit
alone is not proof that DDP ranks exited.

Kairo stores no `succeeded`, `failed`, `retryable`, or `backoff` disposition.

## Lease

Lease state remains a pure coordination state machine:

```text
reserved -> prepared -> active -> releasing -> released
    |          |            |             |
    +----------+----> revocation_requested |
    +----------+--------------------------> stale
```

A lease is ownership granted by Kairo; a provider observation is physical
reality. Neither substitutes for the other. Expiry alone cannot release a
stale lease. Provider evidence taken after the identity-loss or process-exit
boundary and verified process absence remain required for reconciliation.

CPU, RAM, and disk requests are admission accounting, not hard enforcement.
GPU leases are exclusive among Kairo participants; unmanaged activity is
observed and fails closed unless explicitly allowed by the request.

## Suspend command

The worker command protocol contains only `suspend`:

```text
pending -> accepted -> checkpointing -> checkpointed
                                   \--> rejected
```

Commands are delivered at least once. A project checkpoint callback must be
idempotent by command ID. `checkpointed` means the project-side checkpoint was
published; it does not mean the process exited, the attempt quiesced, or the
lease was released.

There is no worker `cancel` command and no command `completed` state which
duplicates attempt quiescence. Pause-operation status joins the checkpoint,
attempt, and lease facts when an operator needs an end-to-end view.

## Terminal facts and project decisions

For example, Kairo may emit:

```text
execution_id = ex_123
attempt_id = att_456
exit_code = 75
checkpoint_ref = "..."
attempt_state = quiesced
lease_state = released
```

Kairo does not classify exit 75. An LLM-Develop controller may interpret it as
restartable and submit a new execution request with the checkpoint reference.
A different project may interpret the same exit as terminal failure. Neither
decision changes Kairo.

The executor passes `input_continuation_ref` to the process unchanged. Kairo
does not select a previous checkpoint automatically and does not attach one to
a future request unless the project explicitly supplied it.

## Scheduling

The scheduler considers only waiting execution requests whose effective
admission gate is open. It may use coordination attributes:

- request priority and submission order;
- executor labels;
- exclusive GPU count and memory constraints;
- admitted CPU, RAM, and disk capacity;
- fresh provider observations and external claims.

It does not inspect project events, predecessor completion, metrics, artifacts,
exit codes, or research policy.

Automatic priority preemption remains possible only for requests which opt in
as checkpointable and preemptible. Preemption produces a suspend command and a
terminal/quiesced execution fact. Kairo does not requeue or resume the victim;
the project controller decides whether to submit a replacement request.

## Project-wide pause

`project pause` is a coordination operation, not a project state transition.
It means:

1. Atomically close the project admission gate and persist an idempotent pause
   operation.
2. Stop issuing new reservations and launch authorizations in that scope.
3. Revoke reservations and unconsumed authorizations which have not started.
4. Send one durable suspend command to every running checkpointable attempt in
   the scope.
5. Wait for checkpoint publication, launcher exit, every registered process to
   disappear, attempt quiescence, and lease release.
6. Report exact blockers instead of claiming quiescence prematurely.

Queue and task scope pause use the same operation on a narrower set of
execution requests. Descendant scope gates are not changed.

Pause operations are durable coordination records:

```text
pause_operations
  id
  scope_id
  scope_generation
  request_id
  actor
  state          requested | quiescing | quiesced | blocked
  created_at
  updated_at

pause_targets
  operation_id
  execution_id
  attempt_id     optional for a pre-start request
  lease_id       optional
  command_id     optional
  state
  blocker_reason
```

An independent control reconciler repeatedly converges unfinished operations.
Database uniqueness makes reconciliation, daemon restart, and command
redelivery idempotent.

The gate is checked when reserving, after provider preparation, when issuing a
launch authorization, and when consuming that authorization. This closes the
pause-versus-launch races. A process spawned immediately before authorization
consumption is terminated by the executor if the closed gate invalidated that
authorization; pause is not reported quiesced until its absence is verified.

A non-checkpointable running attempt, rejected checkpoint, live child rank,
stale lease, stale observation, or unresolved external claim makes the pause
operation `blocked`. Kairo does not silently escalate a cooperative pause into
a hard kill.

In observe-only mode Kairo may persist a closed gate and report what would need
to quiesce, but does not issue commands or revoke resources. The operation
reports an `observe_only` blocker.

## Resume

`project resume` only reopens the project admission gate. It does not recreate,
requeue, or restart a terminal execution and does not interpret a checkpoint.

Waiting requests which the project had already submitted may become eligible
after the gate opens. A quiesced request remains terminal. If it should run
again, the project controller submits a new request with a new idempotency key
and the chosen continuation.

Resume never recalls a pause operation or a suspend command which was already
issued. That attempt continues to quiescence even if the worker has not accepted
the command yet. The open gate permits other already-waiting requests subject
to their leases and resource availability. This keeps resume limited to
admission and avoids turning it into command-lifecycle policy.

## Event interface

Projects need raw coordination facts without transferring workflow ownership
to Kairo. Kairo exposes its append-only event sequence:

```text
GET /v2/events?project=PROJECT&after=SEQUENCE
```

Relevant events include request submission/authorization/start/terminal,
attempt heartbeat/checkpoint/exit/quiescence/loss, lease transitions, gate
changes, pause targets, and reconciliation. Events carry stable IDs and raw
payloads. Consumers persist their own cursor and handle redelivery
idempotently.

The project controller consumes these facts, advances its own state machine,
and submits any next execution. A convenience SDK may implement cursor polling
and idempotent submission, but it is never authoritative for project state.

## Submission API and CLI

The current plan-oriented API is replaced by execution submission:

```text
POST /v2/executions
GET  /v2/executions
GET  /v2/executions/{id}
POST /v2/executions/{id}/withdraw   # only before start

GET  /v2/scopes
GET  /v2/scopes/{id}
POST /v2/scopes/{id}/pause
POST /v2/scopes/{id}/resume
GET  /v2/pause-operations/{id}

GET  /v2/events
GET  /v2/resources/status
```

CLI surface:

```text
kairo execution submit execution.toml
kairo execution list --project PROJECT
kairo execution show EXECUTION_ID
kairo execution withdraw EXECUTION_ID

kairo project pause PROJECT
kairo project resume PROJECT
kairo queue pause PROJECT QUEUE
kairo queue resume PROJECT QUEUE
kairo task pause PROJECT QUEUE TASK
kairo task resume PROJECT QUEUE TASK
```

Pause waits for quiescence or a blocker by default. `--no-wait` returns the
operation ID immediately. Resume reports only that admission reopened; it never
claims that project work resumed.

## Execution request format

TOML may remain as a CLI serialization format, but it describes one execution,
not a workflow plan:

```toml
schema_version = 2
client_request_id = "llm-develop/train/2026-09-06T01"
project = "llm-develop"
queue = "training"
task = "jalm-300m-v3-dociso-mb1-ddp"
argv = ["uv", "run", "python", "train.py"]
cwd = "/mnt/d/Dev/LLM-Develop"
priority = 10
checkpointable = true
preemptible = true
input_continuation_ref = "/mnt/d/checkpoints/step_00067696.pt"

[executor]
labels = { environment = "wsl2" }

[[exclusive]]
kind = "gpu"
count = 2
same_node = true
min_observed_free_memory_bytes = 10000000000

[capacity]
cpu_millis = 8000
ram_bytes = 12884901888
```

There is no DAG, `depends_on`, retry policy, workflow condition, task revision,
or completion policy in this format. Normalization and idempotency remain
useful because they protect execution submission, not workflow semantics.

## Managed SDK

The worker SDK retains:

- attempt, lease, and coordination-epoch fencing;
- background heartbeat and opaque progress;
- rank process registration;
- at-least-once suspend polling and acknowledgement;
- single-process and distributed checkpoint coordination.

It receives `KAIRO_EXECUTION_ID` and an optional opaque
`KAIRO_CONTINUATION_REF`. The legacy task/workload identity aliases are removed.

Local STOP-file handling is allowed only for unmanaged sessions. Managed
sessions reject `stop_paths`; otherwise a local file could terminate a process
without a durable closed gate or suspend command and make the exit look like an
ordinary terminal fact.

## What remains and what is removed

Keep and strengthen:

- providers and physical observation;
- external/unattributed GPU claim handling;
- resource reservation, prepare, lease, and launch authorization;
- process identity and coordination epoch fencing;
- attempt heartbeat, progress, checkpoint, exit, and quiescence facts;
- stale lease quarantine and explicit reconciliation;
- suspend command delivery and DDP coordination;
- priority and resource-based coordination;
- append-only event log and observe-only mode.

Remove or replace:

- queue manifests as durable workflow plans;
- task and task-revision lifecycle state;
- DAG dependencies and downstream revision propagation;
- Kairo retry/backoff policy;
- succeeded/failed/cancelled task classification;
- runnable-task derivation from predecessor state;
- automatic continuation inheritance and resume;
- manifest removal/retirement semantics;
- worker cancel commands;
- `plan apply`, `plan history`, and plan-owned queue lifecycle.

## Transition from the current V1

This is a runtime-model replacement, not a compatibility layer. The repository
must contain only one live state machine after the transition.

Upgrade preflight requires all attempts quiesced, all leases released, and all
commands either checkpointed or rejected. It refuses to proceed with running,
quiescing, lost, stale, or otherwise unfinished coordination.

At pilot scale, the safe transition is to archive the complete V1 database
read-only and create a fresh coordination database. The running Kairo code
never opens the archive, so this does not create a compatibility runtime or a
second state machine.

Only project/queue/task names may be imported as scope identities. An operator
may explicitly map a previously paused scope to a closed admission gate as a
fail-safe. Pending/backoff/failed/succeeded tasks, DAG edges, retry timers,
plans, and continuations never become live execution requests. Historical
attempt, lease, command, and event facts remain available in the archive or a
static audit export, not in scheduling queries.

Projects submit all post-upgrade execution requests explicitly. Old plan APIs
and tables are removed rather than translated.

The currently quiesced LLM-Develop checkpoint can be supplied by its project
controller in the first new execution request; Kairo will not choose it by
itself.

## Acceptance tests

The redesign is incomplete until all of these hold:

1. No live schema, API, or Go/Python type exposes task success, failure,
   backoff, retry, dependency, retired, or workflow completion state.
2. Exit codes 0, 1, and 75 all produce raw terminal facts and never cause a
   server-owned retry or next execution.
3. A terminal or quiesced execution request can never start a second process.
4. A project controller can consume a checkpoint event and idempotently submit
   a new execution with that opaque continuation.
5. Project pause closes admission across every queue/task scope, suspends each
   running checkpointable attempt once, and waits for every lease release.
6. Project resume opens admission but creates no execution request and does not
   restart a quiesced one.
7. Independently closed child gates remain closed across project resume.
8. Reservation, prepare, authorization, spawn, and activation races cannot
   cross a closed gate without becoming a tracked pause target.
9. Daemon restart and reconciler repetition create no duplicate command or
   launch.
10. Non-checkpointable work, rejected checkpoint, live DDP rank, lost attempt,
    stale lease, and observe-only mode prevent a false quiesced result.
11. Launcher exit does not mark registered child ranks absent.
12. Managed STOP files are rejected; unmanaged STOP compatibility remains.
13. Priority preemption produces a quiesced terminal fact. The server never
    auto-resumes the victim; a project-selected SDK policy may idempotently
    submit its successor from the formally acknowledged continuation.
14. Migration creates no live request from a legacy pending/retry/task state.
15. A real two-GPU DDP execution completes project pause, checkpoint
    publication, all-rank exit, quiescence, and lease release; only an explicit
    project submission starts its continuation.

## Non-goals

Kairo Server does not add a project controller, workflow engine, model-specific
retry policy, artifact semantics, metric gates, dynamic fan-out, or general
hard-kill facility. The Python SDK may provide reusable project-controller
mechanics and opt-in policy implementations, but the project owns their journal
and selects their meaning. Multi-node execution, cloud provisioning, RBAC/TLS,
service supervision, and hard CPU/RAM enforcement also remain outside this
redesign.
