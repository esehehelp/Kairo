# Kairo

> **Kairo owns generic Project/Task orchestration semantics, but never
> interprets project-specific state.**

Kairo safely orchestrates cooperative ML work on a few machines: one daemon
(optionally with its own executors) and node agents on other hosts. A
control-only daemon, with no `[[executors]]` or `[[providers]]` (only its
`[node]` identity), is valid: it serves the API and runs the reconcilers while
every host that runs jobs runs `kairo agent`. It has two layers. A small declarative orchestration layer stores
a ProjectSpec, reconciles a static Task DAG, and applies an explicitly
selected generic task policy. The coordination layer receives immutable
Execution requests and owns Attempt, Lease, Command, provider, and executor
mechanics.

Task is a durable logical object and can have a sequence of immutable
Executions. Execution, Attempt, and Lease remain distinct physical lifecycle
objects. At the coordination boundary, project, queue, and task names are
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

```powershell
# TLS material: a private CA and a server certificate for every address
# clients use to reach the daemon (kairo tls issue re-issues it later).
go run ./cmd/kairo tls init --dir local/tls --host 127.0.0.1 --host localhost --host 192.168.1.12
go run ./cmd/kairo serve --config examples/kairo.toml     # creates the database
# In another terminal: an operator token, saved where the CLI looks for it.
go run ./cmd/kairo token create --config examples/kairo.toml --name operator --role admin --save
go run ./cmd/kairo doctor
go run ./cmd/kairo project validate examples/project-spec.toml
go run ./cmd/kairo project apply examples/project-spec.toml
go run ./cmd/kairo project status example-project
go run ./cmd/kairo task list example-project
go run ./cmd/kairo task wait example-project train
go run ./cmd/kairo task logs example-project train --follow
go run ./cmd/kairo execution submit examples/execution.toml
go run ./cmd/kairo execution list --project llm-develop
go run ./cmd/kairo project pause llm-develop
go run ./cmd/kairo project resume llm-develop
go run ./cmd/kairo resource status
go run ./cmd/kairo machine apply examples/machines.toml
go run ./cmd/kairo machine show
```

The example configuration listens on loopback over plain HTTP (with
`observe_only = true`), so its CLI needs `--api http://127.0.0.1:7474` or
`KAIRO_API`; a daemon reachable from other hosts needs the TLS settings below.
The CLI sends a token over plain HTTP only to a loopback address. Flags go
before positional arguments.

Pause closes the selected coordination scope, fences concurrent launches, and
waits by default until all captured executions are quiesced and their leases
are released. `--no-wait` returns after the durable pause operation is stored.
Resume only reopens admission; it never recalls a suspend command or decides
which execution should run next.

`project apply` stores the declaration once; the daemon performs subsequent
reconciliation. Applying the same normalized declaration is idempotent. A
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

`[tasks.execution.env]` (and `[env]` in an execution TOML) sets environment
variables of the attempt, instead of `powershell -Command '$env:...'` or
`env A=B` wrappers. `[defaults.execution.env]` merges key by key, and `env` on
a `[[tasks.execution.gang.ranks]]` entry merges over the task's for that rank.
The attempt inherits the executor's environment, then the declared variables
(which may replace `PATH` and the like; on Windows names compare without
case), then what Kairo sets (`KAIRO_*`, gang variables, `CUDA_VISIBLE_DEVICES`
for bound GPUs), which a declaration cannot override. Names starting with
`KAIRO_` and `WSLENV` are rejected, and so is `CUDA_VISIBLE_DEVICES` on an
execution with an exclusive GPU request (a CPU task may set it to `-1`). WSL
executors pass the variables through `WSLENV` as written (`/u`, no path
translation). `argv[0]` is still looked up on the executor's own `PATH`.
Env is part of the task's digest and is stored and shown in plain text to any
reader of the API: it is not a place for secrets.

`kairo task cancel [--dry-run] PROJECT [QUEUE] TASK` ends a Task that is not
running: one still waiting for its dependencies, a pending or blocked one, or
a paused one. It withdraws every execution of the Task that has not started
(every gang rank included) and marks the Task `cancelled`; dependants are
blocked as for a failed Task. It refuses while an execution is started and
not terminal (pause the Task first, or `task pause --force`) and leaves
checkpoints and other outputs alone. An execution withdrawn with `execution
withdraw` before it started also leaves its Task `cancelled`.

`kairo task rerun PROJECT [QUEUE] TASK` runs the same declaration of a
`failed`, `stopped`, `cancelled` or `blocked` Task again, as its next
Execution (reason `rerun`), once all of its executions have ended. Dependants
that were blocked only because a dependency did not succeed return to
`pending` and run if their dependencies now succeed. A changed declaration
still needs a new Task name. `kairo project prune [--write]
FILE` reports (or, with `--write`, rewrites FILE to) the Tasks that can still
run and the dependencies they need, keeping each kept Task's digest.

`observe_only = true` inventories resources and external claims but never
reserves, launches, suspends, or releases work. It is the recommended first
deployment mode on a host with unmanaged training processes.

Resource observations describe the present: only each resource's latest one
is used, and the daemon prunes the others once they are an hour old.
`kairo db compact --config kairo.toml` (with the daemon stopped) prunes them
at once and runs `VACUUM`.

CPU, RAM, and disk reservations are admission accounting rather than hard OS
limits. GPU exclusion is a coordination guarantee among Kairo participants;
external processes are observed and excluded but cannot be physically fenced.

## Following tasks from the CLI

Every command group answers `help`, `--help` and `-h` (`kairo task --help`,
`kairo task wait --help`); a mistyped subcommand gets a suggestion. QUEUE may
be left out of task commands for Tasks declared by a ProjectSpec.

| Command | What it prints |
|---|---|
| `kairo task list PROJECT [--state S,..] [--match PREFIX] [--live] [--since D] [--all] [--json]` | a table (name, queue, state, execution, start, elapsed, exit or reason or progress) of live Tasks and those updated in the last 24 h (`--all`: every Task), then the project's counts by state |
| `kairo task show PROJECT [QUEUE] TASK [--json]` | one Task: declaration, env, dependencies and dependants, scope gates, every execution with its attempts, nodes, exit, log paths and sizes, gang ranks, and why a waiting execution waits |
| `kairo task logs PROJECT [QUEUE] TASK [--stream stdout\|stderr\|both] [--tail N] [-f] [--attempt latest\|all\|N] [--rank R]` | the last N lines (default 50, `0` all) of the latest attempt's logs, also for attempts on agent nodes; `-f` follows until the attempt has exited and its logs stopped growing |
| `kairo task wait PROJECT [QUEUE] TASK [TASK...] \| PROJECT --match PREFIX [--until terminal\|started] [--timeout D] [--stall D]` | one line per state change (`10:01:33 hello running -> succeeded`), `DOWN`/`UP` while the daemon is unreachable, `STALL` when a running Task's logs stop growing for D; exits 0 when all succeeded (or started), 1 when one ended otherwise, 2 on timeout, 3 on a usage or lookup error |
| `kairo project watch PROJECT [--match PREFIX] [--exit-when-idle]` | state changes, progress (once a minute per Task) and a summary every 10 min; with `--exit-when-idle` it exits when nothing is pending or running (1 if a watched Task did not succeed) |
| `kairo execution logs EXECUTION_ID`, `kairo logs ATTEMPT_ID` | the same logs by execution or attempt |
| `kairo version` | the CLI's and the daemon's build; `doctor` warns when they differ |

The line-oriented `wait` and `watch` suit a background monitor: each line is
one event and is flushed at once. `project status PROJECT` still prints JSON
and takes `--task NAME` (repeatable), `--match`, `--state`, `--live` and
`--since`. `project apply` prints the Tasks it declared (`--json` for the
response) and, on a conflict, names every Task declared before with different
content; `project validate` prints one line (`--json` for the expanded Tasks).
A command the daemon does not know (a CLI newer than the daemon) says so.

## Authentication and TLS

Every request needs `Authorization: Bearer <token>`, from loopback too; only
`GET /health` is open. A missing, unknown or revoked token gets 401, a token
whose role does not allow the route 403. Tokens are random 256-bit values
stored only as SHA-256 hashes; revocation takes effect immediately.

| Role | May call | Used by |
|---|---|---|
| `read` | every operator `GET` | dashboards |
| `admin` | every operator route | the CLI, kairo-monitor, project controllers |
| `node` | `/api/agent/*`, for one node | a node agent |
| worker | `/api/worker/*`, for one attempt | the processes of an attempt |

Operator and node tokens are managed on the daemon host by opening its
database directly (local file access is the root of trust; the daemon may be
running):

```powershell
kairo token create --config kairo.toml --name operator --role admin --save
kairo token create --config kairo.toml --name dashboard --role read --out dashboard.token
kairo token create --config kairo.toml --name pve0-neo-train --role node --node pve0-neo-train
kairo token list --config kairo.toml
kairo token revoke --config kairo.toml pve0-neo-train
```

The plaintext is shown once. `--save` writes it to the user's config
directory (`%APPDATA%\kairo\token`, `~/.config/kairo/token`).

The CLI and the SDK's `KairoControllerClient` find the daemon, token and CA the
same way:

- URL: `--api`, else `KAIRO_API`, else `https://127.0.0.1:7474`;
- token: `KAIRO_TOKEN`, else the file named by `KAIRO_TOKEN_FILE`, else
  `<config dir>/kairo/token`;
- CA: `KAIRO_CA_FILE`, else `<config dir>/kairo/ca.pem` if present, else the
  system roots.

Control traffic never goes through `HTTP(S)_PROXY` and never follows a
redirect.

A node token is bound to its node: the agent may register only that node, and
every operation is checked against the node owning the executor, lease,
attempt, provider or log it names. An executor, provider or resource never
moves to another node. Against an observe-only daemon an agent's polls find
nothing to do and its actuating operations are refused (423).

Each attempt gets its own worker token at launch authorization, in
`KAIRO_ATTEMPT_TOKEN`. It identifies the attempt (and the lease and epoch it
was issued under) to the worker API, and stops working once the attempt has
quiesced or was fenced (for instance marked lost by a daemon restart). The
executor never passes its own process's `KAIRO_*` variables to an attempt.

### TLS

`kairo tls init --dir DIR --host H...` writes `ca.pem`, `ca-key.pem`,
`server.pem` and `server-key.pem` (ECDSA P-256; the server certificate covers
every `--host`, IP or DNS name). `kairo tls issue --dir DIR --host H...`
re-issues the server certificate from the same CA, for a new address. Keep
`ca-key.pem` on the daemon host only. Private keys and token files written by
`kairo` are restricted to the current user, and the daemon restricts its
database (and WAL) the same way at start: anyone who can write the database
can mint tokens. On Windows, where file modes are ignored, this replaces the
inherited ACL with one granting only the user and SYSTEM.

The daemon refuses to start when its certificate does not chain to
`tls_ca_file` or does not cover the `advertise_url` host. During a CA rotation
`tls_ca_file` (and an agent's `ca_file`) may hold the old and the new CA;
attempts receive every certificate in it.

Daemon configuration:

```toml
listen = "0.0.0.0:7474"
advertise_url = "https://192.168.1.12:7474"  # what attempts are told
tls_cert_file = "local/tls/server.pem"
tls_key_file = "local/tls/server-key.pem"
tls_ca_file = "local/tls/ca.pem"             # handed to attempts
```

The daemon serves TLS 1.3 only. It refuses to listen on an address other hosts
can reach without TLS unless `insecure_http = true`, and with TLS
`advertise_url` must be https. Clients trust the daemon through `ca.pem`:
copy it to `<config dir>/kairo/ca.pem` (or point `KAIRO_CA_FILE` at it) for the
CLI and controllers, set `ca_file` for a node agent, and start Node-based
tools with `NODE_EXTRA_CA_CERTS=ca.pem`. Attempts receive the CA itself in
`KAIRO_API_CA` (base64 DER), so native, WSL and remote attempts need no path.
The certificates carry no revocation endpoint, so Windows `curl.exe`
(Schannel) needs `--ssl-no-revoke` with `--cacert ca.pem`.

A node agent runs `kairo agent --config agent.toml`. Its configuration:

```toml
server_url = "https://192.168.1.12:7474"
token = "kairo_node_..."
ca_file = "/etc/kairo/ca.pem"
# attempt_api_url = "https://192.168.1.12:7474"  # KAIRO_API_URL of this node's attempts
```

The rest (`log_directory`, `observe_only`, `observation_interval_seconds`,
`[node]`, `[[executors]]`, `[[providers]]`) has the same form as in the daemon
configuration (`examples/kairo.toml`). An agent refuses an `http://` URL
unless `insecure_http = true`.

## API

| Method | Path | Role |
|---|---|---|
| GET | `/health` | none |
| GET | `/api/whoami` | any token |
| POST | `/api/projects` (apply a ProjectSpec) | admin |
| GET | `/api/projects/{project}` | read |
| GET | `/api/projects/{project}/tasks/{task}` | read |
| POST | `/api/projects/{project}/tasks/{task}/cancel`, `/rerun` | admin |
| POST, GET | `/api/executions` | admin, read |
| GET | `/api/executions/{id}` | read |
| POST | `/api/executions/{id}/withdraw` | admin |
| GET | `/api/attempts`, `/api/events`, `/api/pause-operations/{id}` | read |
| GET | `/api/attempts/{id}/log` | read |
| GET | `/api/scopes`, `/api/scopes/{id}` | read |
| POST | `/api/scopes/{id}/pause`, `/api/scopes/{id}/resume` | admin |
| GET | `/api/nodes/quarantines` | read |
| POST, DELETE | `/api/nodes/{id}/quarantine` | admin |
| GET | `/api/resources` | read |
| POST | `/api/resources/{id}/enable`, `/quarantine`, `/reconcile` | admin |
| GET, POST | `/api/machines` (the machine declaration) | read, admin |
| POST | `/api/agent/{operation}`, `/api/agent/logs` | node |
| POST | `/api/worker/heartbeat`, `/api/worker/processes` | worker |
| GET | `/api/worker/commands` | worker |
| POST | `/api/worker/commands/{command}/acks` | worker |

Errors are `{"error": ..., "code": ...}`. An empty `actor` in a pause, resume,
quarantine, cancel or rerun request is recorded as the token's name.
`/health` and `/api/whoami` include the build `version`.

`GET /api/projects/{project}` takes the filters `task` (repeatable),
`match` (name prefix), `state` (comma list), `live=1` (pending or running) and
`since` (RFC 3339, by `updated_at`); `state_counts` counts every Task whatever
the filter. Each Task has `created_at` and `updated_at`, and each execution its
attempt, executor, timestamps and latest progress without `detail`.
`GET /api/projects/{project}/tasks/{task}` adds the declaration (`spec`),
dependencies, dependants, scope gates, and per execution its attempts (with
log paths and sizes), gang ranks and `waiting` reasons. Apply answers
`{"project", "spec_digest", "idempotent", "created", "unchanged"}`. Cancel
takes `{"actor", "dry_run"}` and answers `{"task", "state", "withdrawn",
"dry_run"}`; rerun takes `{"actor", "request_id"}` (a repeated `request_id`
returns the first answer) and answers `{"task", "state", "ordinal",
"unblocked"}`; a Task in the wrong state is `409 task_conflict`.

`GET /api/attempts/{id}/log?stream=stdout|stderr&offset=N&limit=M` returns an
attempt's log as the daemon holds it (written by its own executor or shipped
by an agent), as raw bytes: `stream` defaults to `stderr`, `limit` to 1 MiB
(at most 8 MiB); `tail=N` instead returns the last N bytes. `Kairo-Log-Size`
is the current size and `Kairo-Log-Next-Offset` where to continue (the body
started at that offset minus its length); an offset past the end or a log
not written yet returns an empty
body, and an unknown attempt or log is `not_found`. Only files inside the
daemon's `log_directory` are served. `kairo logs
[--stream stdout|stderr] [--tail N] [--follow] ATTEMPT_ID` prints it (`--tail`
the last N bytes, `--follow` polls every 2 s until Ctrl-C), and the Python
SDK's `KairoClient.attempt_log` reads one slice.

Clients follow the contract in `sdk/conformance` (how to find the daemon, when
a token may be sent, how to classify each error); every SDK's tests replay it.
Codes: `unauthorized` 401, `forbidden` 403, `not_found` 404, `stale_epoch` and
other conflicts 409, `gate_closed`/`observe_only` 423, `bad_request` 400 (fix
the request), and `internal` 500/503 (a daemon-side failure such as a busy
database: transient, retry). `GET /health` returns `{"ok": true, "api": 1}`;
`api` is the contract version and changes only on a breaking change.

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

### Executor ticks

An executor's tick is bounded in time. Every external call on it has a
deadline: each `nvidia-smi` run 20 s (a `pmon` that does not return fails that
GPU observation), each `wsl.exe` liveness run 15 s, each `taskkill` 30 s, and
each provider call (observe, prepare, release) 45 s. A call that cannot be
cancelled is abandoned at its deadline, and no further call on that provider
starts until it returns. A step of the tick that runs for 30 s is logged at
WARN as `executor step stalled` (with `executor_id`, `step` and
`age_seconds`), and its end at INFO as `executor step resumed`. External
commands that take longer than 2 s are logged at INFO with their duration; a
timeout is logged at WARN with the command.

### Machine declaration

Which node holds which GPU is declared, not discovered. `kairo machine apply
FILE` replaces the whole declaration: `[[nodes]]` tables with an `id` (a node
known to the daemon) and `gpus`, NVIDIA UUIDs as `nvidia-smi -L` prints them
(see `examples/machines.toml`). Providers only observe. A GPU a provider
reports that is not declared for its node, whether declared for another node
or for none, is not registered and not scheduled; the rest of that poll still
applies, and the GPU is listed as undeclared by `kairo machine show` and in
`undeclared_gpus` of `kairo resource status` (a registered GPU in that
situation has `derived_state` `undeclared`).

To move a GPU between nodes, declare it for its new node. It stays registered
on the old node, unschedulable, until the new node's provider reports it,
which takes it over (event `gpu_ownership_transferred`); the old node's open
claims on it are cleared when the declaration changes (event
`gpu_declaration_changed`). Apply refuses to move or drop a GPU that a live
lease holds, and to declare an unknown node. `--dry-run` prints the changes
(`+` declared, `~` moved, `-` dropped) without applying them. `machine show`
lists each declared GPU as `observed`, `stale`, `pending` (not yet reported by
its node) or `unseen`.

`POST /api/machines` takes `{"nodes": [{"id", "gpus"}], "dry_run"}` and
answers `{"changes": [{"uuid", "from", "to"}], "unchanged", "dry_run"}`; a
leased GPU is `409 machine_conflict`. `GET /api/machines` answers `{"gpus",
"undeclared"}`.

### Node quarantine

`kairo node quarantine --reason ... <NODE_ID>` (or `--all`) stops all work on a
node (or every node) until `kairo node release <NODE_ID>` (or `--all`):

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
target with `force_stop_not_picked_up` (the executor polls but has not picked
it up: check that node's agent log) or `executor_unreachable` (it has not been
seen).

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
  wait again. The abort reason (kept on the placement and logged) names each
  rank's executor, whether that executor received the rank's lease, and the
  executor's `last_seen_at`.
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

Managed attempts receive `KAIRO_API_URL`, `KAIRO_EXECUTION_ID`,
`KAIRO_ATTEMPT_ID`, `KAIRO_ATTEMPT_TOKEN` and, when the daemon serves TLS,
`KAIRO_API_CA`; also `KAIRO_NODE_ID`, `KAIRO_EXECUTOR_ID`,
`KAIRO_RESOURCE_IDS`, `KAIRO_RESOURCE_BINDINGS` and `CUDA_VISIBLE_DEVICES` (a leased GPU by its UUID, never by index),
`KAIRO_CONTINUATION_REF` when resuming, and the gang variables for a gang
rank. WSL attempts get them through `WSLENV`, never on the `wsl.exe` command
line. Rank zero polls and acknowledges commands; the PyTorch adapter broadcasts the command,
runs every rank's callback, and only publishes the continuation after the
distributed barrier. Commands may be redelivered, so checkpoint publication
must remain idempotent by command ID.

A safe point acknowledges a `suspend` `accepted` and `checkpointing`, runs the
checkpoint callback, and publishes its continuation with `checkpointed`. A
callback that fails, or returns no continuation, is acknowledged `rejected`;
unknown command kinds are skipped. After `install_stop_signals()` a SIGTERM /
CTRL_BREAK (the graceful phase of a force stop) makes the next safe point run
the callback once (reason `stop_signal`, no command id, nothing acknowledged)
and return true, so the work can still be resumed by hand. A worker whose
token is refused (the attempt quiesced: retired) or fenced stops reporting.
Python also offers `ProgressReporter`, `GangContext`, `continuation_or`,
`CommandJournal` (idempotent checkpoints by command id), `provenance.capture`
and the operator `KairoClient`.

The Rust crate `sdk/rust/kairo-sdk` (blocking, ureq + rustls) implements the
same worker side: `Attempt::from_env`, a `Session` with one heartbeat thread
and `safe_point`, the process-wide `reporter(unit)` / `phase` progress
handles, and `kairo_sdk::Progress` compatible with neo-ime's former
`kairo-progress`. Depend on it by git:

```toml
kairo-sdk = { git = "https://github.com/esehehelp/Kairo", rev = "<commit>" }
```

Every SDK replays `sdk/conformance/*.json` in its tests.

### Low-level project-owned controller policy

Projects submitting directly to the execution API can still opt into
`ResumeAfterKairoPreemption`. The policy snapshot and canonical submission are
written to the project's controller journal before the initial API call:

```python
from kairo_sdk import ControllerJournal, KairoControllerClient, ProjectController

controller = ProjectController(
    KairoControllerClient(),  # KAIRO_API / KAIRO_TOKEN / KAIRO_CA_FILE, or the config dir
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
   work, while direct execution API users retain the project-owned controller
   option.
5. Redelivery of one command cannot corrupt checkpoint publication.
6. A static Task DAG submits a node only after all dependencies succeed, and
   replay cannot create a duplicate Execution for one policy decision.
7. A two-GPU DDP execution receives an all-or-nothing gang lease and all ranks
   quiesce before release.

Cloud provisioning, roles beyond the four above, service supervision,
cron/fair-share scheduling, and hard CPU/RAM enforcement remain outside this
pilot.

## Upgrading a running installation

A daemon (or node agent) that starts marks the attempts its executors had
running as lost: it cannot vouch for processes a previous incarnation
launched. Drain before replacing either binary: pause the projects, wait
until no attempt is authorized, running or quiescing, then stop the agents
and the daemon, back up the database (with its `-wal` and `-shm` files),
replace the binaries and configurations, start the daemon, then the agents,
check `kairo doctor` and `kairo resource status`, and resume. Code that queued
executions will load (scripts, the editable SDK) must not change while they
wait.

Replace the agents together with the daemon: an agent older than the daemon
starts attempts without their declared `env`. A daemon opening an older
database migrates it in place (the `cancelled` task state, reruns, the `env`
column, the machine declaration, which starts out declaring every registered
GPU for the node it is registered on); keep the backup, since an older binary would read a rerun Task's
earlier execution as its outcome again.

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
