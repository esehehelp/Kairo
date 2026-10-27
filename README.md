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
go run ./cmd/kairo hardware show
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
translation). `argv[0]` is still looked up on the executor's own `PATH`, and
argv reaches the program word for word on every executor: no shell sits in
between (a WSL attempt runs through `wsl.exe --exec`), so `$VAR` in an argument
is expanded only by a shell the argv itself starts.
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

`kairo resource status` prints one row per resource, by node (GPUs, then
cpu, ram and disk): its state (with a quarantine's reason), total and free as
last observed (a GPU's memory and utilization; a cpu's cores), what live
leases reserve of a cpu, ram or disk, the latest power sample (POWER: a GPU's
draw and the power limit in force, `289/320 W`, a cpu's CPU package draw;
CLOCKS: a GPU's SM clock, P-state and active clock event reasons, `1905 MHz
P2 sw-power`; see Power and energy) and the tasks holding it (with the lease
state unless `active`); then the external claims with their evidence, and the
GPUs reported but not declared for their node. `--json` prints the response of
`GET /api/resources` instead, whose resources also carry `total_bytes`,
`free_bytes`, `utilization`, `reserved`, `holders` and `device` (the latest
sample). `kairo doctor` ends with the same table.

Resource observations describe the present: only each resource's latest one
is used, and the daemon prunes the others once they are an hour old. Device
samples are kept for 72 hours (each device's latest always). `kairo db
compact --config kairo.toml` (with the daemon stopped) prunes both at once and
runs `VACUUM`.

CPU, RAM, and disk reservations are admission accounting rather than hard OS
limits (see Admission for how they are counted). GPU exclusion is a coordination guarantee among Kairo participants;
external processes are observed and excluded but cannot be physically fenced.

## Following tasks from the CLI

Every command group answers `help`, `--help` and `-h` (`kairo task --help`,
`kairo task wait --help`); a mistyped subcommand gets a suggestion. QUEUE may
be left out of task commands for Tasks declared by a ProjectSpec.

| Command | What it prints |
|---|---|
| `kairo task list PROJECT [--state S,..] [--match PREFIX] [--live] [--since D] [--all] [--json]` | a table (name, queue, state, execution, start, elapsed, exit or reason or progress or pause) of live Tasks and those updated in the last 24 h (`--all`: every Task), then the project's counts by state |
| `kairo task show PROJECT [QUEUE] TASK [--json]` | one Task: declaration, env, dependencies and dependants, scope gates (a closed one with who closed it and when), every execution with its attempts, nodes, exit, log paths and sizes, log issues, the energy its GPUs used (`energy: GPU 1.23 kWh (partial)`), gang ranks, and why a waiting execution waits |
| `kairo task logs PROJECT [QUEUE] TASK [--stream stdout\|stderr\|both] [--tail N] [-f] [--attempt latest\|all\|N] [--rank R]` | the last N lines (default 50, `0` all) of the latest attempt's logs, also for attempts on agent nodes; `-f` follows until the attempt has exited and its logs stopped growing |
| `kairo task wait PROJECT [QUEUE] TASK [TASK...] \| PROJECT --match PREFIX [--until terminal\|started] [--timeout D] [--stall D]` | one line per state change (`10:01:33 hello running -> succeeded`), `DOWN`/`UP` while the daemon is unreachable, `STALL` when a running Task's logs stop growing for D; exits 0 when all succeeded (or started), 1 when one ended otherwise, 2 on timeout, 3 on a usage or lookup error |
| `kairo project watch PROJECT [--match PREFIX] [--exit-when-idle]` | state changes, progress (once a minute per Task) and a summary every 10 min; with `--exit-when-idle` it exits when nothing is pending or running (1 if a watched Task did not succeed) |
| `kairo execution logs EXECUTION_ID`, `kairo logs ATTEMPT_ID` | the same logs by execution or attempt |
| `kairo version` | the CLI's and the daemon's build; `doctor` warns when they differ |

A pending or running Task held back by a closed gate of its project, queue or
task keeps its state; its note says `paused: task gate closed by alice` once
nothing of it runs (its continuation waits behind the gate), or `pausing`
while its execution is still being suspended. A pause is a view of the gates,
not a stored Task state, so `--state` does not select it.

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
| `node` | `/api/agent/*`, for one node | a node agent, a host's controller |
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

The rest (`log_directory`, `log_min_free_mib`, `observe_only`,
`observation_interval_seconds`, `[node]`, `[[executors]]`, `[[providers]]`)
has the same form as in the daemon configuration (`examples/kairo.toml`). An
agent refuses an `http://` URL unless `insecure_http = true`. A host's device
controller (`kairo controller`, see Hardware settings) connects the same way.

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
| GET | `/api/hardware` (`?host=`) | read |
| POST, DELETE | `/api/hardware/{host}/{device}/settings` | admin |
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
dependencies, dependants, scope gates (a closed one with `closed_by` and
`closed_at`, from the pause operation that closed it), and per execution its
attempts (with log paths and sizes, `log_problems` and `energy`), gang ranks and
`waiting` reasons. A live Task held back by a closed gate has `paused`, the
innermost such gate. Apply answers
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

A compute process a provider sees on a GPU (`nvidia-smi pmon`) is an
`external_process` claim unless it is the work of the attempt holding that GPU
(on any node's row of it): one of the attempt's registered processes, a
descendant of one (by its parent chain, each parent alive and started no later
than its child), or, on Linux, a process carrying the attempt's
`KAIRO_ATTEMPT_ID` (read from `/proc/PID/environ`, as force stop finds the
attempt's processes; Windows reads only the parent chain). That holds only
while the attempt is `running` or `quiescing` with a registered process alive:
a descendant that outlives them is a claim again, and the lease is not
released while it runs. A process that cannot be tied to the lease holder (one
of another attempt, one whose parent has exited or whose environment cannot be
read) stays a claim, and a GPU with a claim is not given to a preempting
execution.

### Admission

Which waiting execution runs where is decided by one planning pass over the
whole installation: every waiting execution whose scope gates are open, every
node with an executor that has polled in the last minute (or not yet), every
lease and claim. Each executor's reserve and preemption call runs the same
pass, and the pass, not the executor that polls first, chooses; an execution
placed on another node waits for that node's executor (`awaiting_executor`).
Executors of one node share its resources, so any of them that matches the
labels may take what is placed there.

- Order: higher `priority` first. Within a priority, the project holding the
  smallest share of the installation's GPUs, CPU or RAM (whichever share is
  largest) goes next, then the earlier submission.
- Placement: among the nodes an execution fits on, the one left with the
  fewest free GPUs (CPU work stays off GPU nodes, GPU work packs onto as few
  as it can), then the most free CPU. A gang's ranks go on distinct nodes, all
  or none.
- Backfill: an execution that fits nowhere holds, on the node it would run on
  once the leases in its way end, what it can already use there; executions
  after it run on everything nobody holds. It holds nothing when what stops it
  does not end by itself: a process Kairo did not start on a GPU, activity no
  process explains, a stale observation, a node too small for it, the disk
  watermark.
- Preemption: an execution that fits nowhere (any priority, a gang too) may
  suspend executions of lower priority that are `checkpointable` and
  `preemptible` and whose GPUs or RAM are what stops it, the fewest needed
  (already suspending ones first, then the lowest priority, single executions
  before gangs, the most recently started). Too little CPU or disk is waited
  for, never a reason to suspend anything.
- Accounting: CPU is the node's total minus the reservations on it. RAM is
  the lesser of total minus reservations and observed free memory minus the
  part of each reservation its attempt does not use yet (what an attempt
  uses, sampled by its executor, counts for two minutes; without a sample the
  whole reservation counts). Disk is observed free space minus every
  reservation on it: bytes written overstate what stays on disk (a rewritten
  checkpoint counts twice), so they never shrink a reservation. A disk
  request keeps its `min_free_after_bytes` and the watermark free.
- Elastic CPU: `cpu_millis_min` (at most `cpu_millis`) under `[capacity]`
  (`[tasks.execution.capacity]`) is the least an execution keeps. The rest of
  its CPU reservation is lent to executions of the same or lower priority,
  which then run beside it and slow it down instead of stopping it: a server
  declared with `cpu_millis = 16000` and `cpu_millis_min = 4000` lets 12 CPUs
  of evaluations run next to it.
- Disk watermark: a node with a registered filesystem under
  `[scheduler] disk_min_free_bytes` of the daemon configuration (default
  5 GiB, at most 5% of the filesystem; `0` turns it off) admits nothing until
  space is freed. Each such filesystem is kept in `node_alerts` and announced
  by the events `disk_low_watermark` and `disk_watermark_cleared`.

Every waiting execution's latest reason is kept: `task show` prints it under
`waiting:` (for example `gpu_leased: pve0: 1 GPU(s) of >= 11.0 GiB needed, 0
usable; leased by glm/maya-r3 (priority -100)`), and `task list` adds its
code to the NOTE (`execution waiting: insufficient_ram`). The codes are
`no_executor`, `awaiting_executor`, `gpu_leased`, `held_for_higher_priority`,
`gpu_memory_low`, `insufficient_gpu`, `insufficient_cpu`, `insufficient_ram`,
`insufficient_disk`, `external_claim`, `unattributed_activity`,
`stale_observation`, `disk_low_watermark` and `preempting`.

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

Every 15 s an executor samples what each attempt it launched takes up, for
admission to count that part of the attempt's RAM reservation once
(`attempt-usage` on the agent API; bytes written are recorded but do not
change disk admission, see Admission). The sample runs beside the tick, one at a
time, bounded to 15 s, so it never holds the tick. RAM is the memory the
attempt's processes hold, bytes written what they have written, never less
than an earlier sample showed:

- Linux: the processes carrying the attempt's `KAIRO_ATTEMPT_ID` and the
  descendants of its launcher; the sum of their PSS (`smaps_rollup`) and of
  their `write_bytes` less `cancelled_write_bytes` (`/proc/PID/io`).
- Windows: the processes of the attempt's job object, their private working
  sets and the bytes the job has written; without a job object, the launcher
  and its descendants by parent pid and what they have written while alive.
- WSL: the Linux processes carrying `KAIRO_ATTEMPT_ID`, as on Linux, read by
  one `wsl.exe` run per sample.

A process that cannot be read (another user's, a kernel without
`smaps_rollup`) makes that figure unknown, and admission then counts the
whole reservation. Attempts launched by an earlier instance of the executor
(before an agent restart) are not sampled.

### Attempt logs

An attempt's stdout and stderr are files in its executor's `log_directory`
(`<attempt>.stdout.log`, `<attempt>.stderr.log`), written by the attempt's
processes themselves. A node agent ships them into the daemon's
`log_directory`, which `task show`, `task logs` and `GET
/api/attempts/{id}/log` read.

- An executor does not launch while the filesystem holding its
  `log_directory` has less than `log_min_free_mib` free (default 1024, in the
  daemon and the agent configuration): the execution ends `launch_aborted:
  launch failed before activation: the filesystem of the attempt log
  directory ... has 12.0 MiB free, below the minimum of 1.0 GiB`, which `task
  show` and `task list` print. So does a free space measurement that fails,
  and a log file that cannot be created.
- While its attempts run, the executor checks that space every 15 s and
  records on each attempt when it falls below the minimum (`log_space_low`)
  or runs out (`log_space_exhausted`: output is being lost), and checks once
  more as an attempt exits. A write the attempt's own process makes cannot be
  seen to fail, so the space is what is watched.
- An agent ships every log, an empty one too, and retries one the daemon
  does not take (the daemon unreachable, or its own disk full, answered as an
  `internal` failure) at every sweep from where the daemon's copy ends, so
  nothing the node-side file holds is lost. One that has not reached the
  daemon for 30 s is recorded on its attempt (`log_forward_failed`, per
  stream), and so is its catching up. As an attempt exits, its logs are
  shipped before its exit is recorded.

Each problem is a coordination event (`attempt_log_problem`,
`attempt_log_problem_resolved`) and a `log issue:` line of the attempt in
`task show` (`log_problems` in the API), such as `log issue: stderr not
shipped to the daemon: ... (10-27 10:01 to 10-27 10:04, caught up)`.

### Power and energy

Providers record what a device draws and how it runs, with the time they read
it; Kairo keeps these facts and judges none of them against a specification.
It only reads: no power limit or clock is ever set.

- GPU: each `nvidia-smi` poll also reads the board power
  (`power.draw.average`, else `power.draw`), the power limit in force
  (`enforced.power.limit`), the SM and memory clocks, the P-state, the active
  clock event reasons (a bitmask: `0x1` idle, `0x4` software power cap, `0x40`
  hardware thermal slowdown, ...), the PCIe generation and width, memory
  utilization and fan speed, and, kept once per GPU, its default, least and
  greatest power limit, its greatest clocks and its greatest PCIe link. A
  field a GPU does not report (`[N/A]`, `[Not Supported]`) is left out, never
  an error. A driver that rejects these fields is asked by their older names
  (`clocks_throttle_reasons.active`, `pcie.link.gen.current`), then for the
  inventory alone.
- CPU package: a node's host provider reads the power of its CPU packages,
  averaged since its previous poll: on Windows the `\Energy Meter(*)\Power`
  performance counter (instances `rapl_package*_pkg`; no elevation needed),
  on Linux the RAPL counters `/sys/class/powercap/*-rapl:N/energy_uj`
  (wraps handled with `max_energy_range_uj`). Where they cannot be read (an
  unprivileged container, WSL, a CPU without them) the node reports none,
  silently. The reader is `internal/cpupower`.

Samples are kept per physical device (a GPU by its UUID), so the providers of
a shared GPU add to one series. As each GPU sample arrives, the energy since
the device's previous sample, the trapezoid of their power, is added to the
attempts whose lease holds that GPU (on any node's row of it), cut at the
attempt's start and at its exit, its lease's release or its lease going
stale; a GPU is counted once whichever provider reports it. An interval the
samples do not vouch for is not integrated and makes the attempt's total
`partial`: a gap longer than the earlier sample's validity
(`observation_ttl_seconds`, default 15 s), a sample without power, a run that
began before the GPU's first sample or went on after its lease went stale.
`task show` prints `energy: GPU 1.23 kWh (partial)` per attempt (`energy`
with `gpu_joules`, `gpu_seconds_covered`, device-seconds, and `partial` in the
API). A CPU package is shared by whatever runs on its node, so its power is
kept for the node only.

The samples are the tables `device_samples` and `device_properties`, the
totals `attempt_energy`. Samples are kept for 72 hours (about 5 MB a day per
GPU and provider reporting it at the default 5 s poll); the totals are kept
with their attempts.

### Machine declaration

Which node holds which GPU is declared, not discovered. `kairo machine apply
FILE` replaces the whole declaration: `[[nodes]]` tables with an `id` (a node
known to the daemon) and `gpus`, NVIDIA UUIDs as `nvidia-smi -L` prints them
(see `examples/machines.toml`). Providers only observe. A GPU a provider
reports that is not declared for its node, whether declared for another node
or for none, is not registered and not scheduled; the rest of that poll still
applies, and the GPU is listed as undeclared by `kairo machine show` and
`kairo resource status` (`undeclared_gpus` with `--json`; a registered GPU in
that situation has `derived_state` `undeclared`).

To move a GPU between nodes, declare it for its new node. It stays registered
on the old node, unschedulable, until the new node's provider reports it,
which takes it over (event `gpu_ownership_transferred`); the old node's open
claims on it are cleared when the declaration changes (event
`gpu_declaration_changed`). Apply refuses to move or drop a GPU that a live
lease holds, and to declare an unknown node. `--dry-run` prints the changes
(`+` declared, `~` moved, `-` dropped) without applying them. `machine show`
lists each declared GPU as `observed`, `stale`, `pending` (not yet reported by
its node) or `unseen`.

A GPU listed under several nodes is SHARED: nodes on one host (containers
that all see the card, each with its agent) may each schedule it. Each node
registers a row of its own (the first under the provider's id, the others as
`<id>@<node>`), and a lease on any row of the device excludes the others:
scheduling, admission checks, lease release and `resource status` count the
leases and claims of every row of it. A provider cannot see another
container's processes, so while one node's row is leased, the other nodes'
claims of activity without a process (`unattributed_activity`) on it are the
lease holder's work and are not recorded; the lease holder's own provider
still reports what it cannot attribute, and a process another node does see
there stays an `external_process` claim unless it is the lease holder's own
work (see Coordination model). Priority preemption reaches a
lower-priority victim holding the device through another node's row. Apply
refuses to drop a node from a GPU whose row on that node a live lease holds;
`from` / `to` of a shared GPU's change list its nodes, comma-separated.
Containers share the host's RAM and CPUs as well, which Kairo accounts per
node: keep their memory limits within the host's.

Optional `[[hosts]]` tables declare the physical machines, and only what
observation cannot tell: policy and authority. A host has an `id`, the `nodes`
on it (containers, or the machine itself; a node is on at most one host), a
`controller` (the node of the `kairo controller` that may change its devices'
settings; none: its devices are only observed) and `max_slowdown`, the share
of maximum performance its devices may give up for efficiency, in [0, 0.5)
(omitted: 0, the hardware stays at its defaults). Its devices are every GPU
declared for one of its nodes and `cpu` (its CPU packages); a GPU whose nodes
are on two hosts is an error. `[[hosts.devices]]` overrides `max_slowdown` or
sets `control = false` (never changed, never benched) for one device, a GPU
UUID or `cpu`, listed once. Host nodes and controllers must be known nodes
(a controller registers its node when it starts), and a controller controls
one host. What a device is, how fast it clocks and how much power it draws is
observed by providers and measured by benches, never declared. A host is
replaced as a whole on apply; `--dry-run` prints `+ host`, `~ host` (each
changed field, `from -> to`) and `- host`, and the device settings the apply
removes (`- setting`: the device is no longer on a host, is `control = false`,
or its host lost its controller). `machine show` adds the hosts and the
effective policy of each device (its own value, else its host's; event
`host_declaration_changed`).

`POST /api/machines` takes `{"nodes": [{"id", "gpus"}], "hosts": [{"id",
"nodes", "controller", "max_slowdown", "devices": [{"id", "max_slowdown",
"control"}]}], "dry_run"}` and answers `{"changes": [{"uuid", "from", "to"}],
"unchanged", "host_changes": [{"host", "from", "to"}], "unchanged_hosts",
"cleared_settings", "dry_run"}`; a leased GPU is `409 machine_conflict`. `GET
/api/machines` answers `{"gpus", "undeclared", "hosts", "devices"}`, the
devices with `host_id`, `device_id`, `kind`, `max_slowdown`, `control` and
`controller`.

### Hardware settings

A device of a host (a GPU by UUID, or `cpu`) can be given settings: a GPU's
highest graphics clock (`graphics_clock_max_mhz`), a locked memory clock
(`memory_clock_mhz`) and a power limit (`power_limit_w`); a CPU's highest
frequency (`max_freq_mhz`, Windows and Linux cpufreq) or highest performance
percentage (`max_perf_pct`, Linux intel_pstate). A setting left out is the
hardware's default; no setting at all is the defaults. Kairo stores what is
desired; the host's controller carries it out and reports what is in place.
Nothing here holds back admission or a running job: a setting that fails is a
recorded fact.

- Owners: a setting is `manual` (an operator's), `bench` or `policy` (a bench
  run's or the operating-point policy's, each belonging to a lease), one per
  owner kind and device. The one in effect is manual, else bench, else policy,
  so a bench run overrides the policy on a device it has leased. A bench or
  policy setting needs a reserved, prepared or active lease on a node of the
  host that, for a GPU, holds that GPU, and is refused (`409
  device_setting_conflict`) while another lease's setting of that kind is
  there; a policy setting is refused on a device whose `max_slowdown` is 0.
  When a lease is released, by any path, its settings are removed in the same
  transaction: no setting outlives its lease.
- Capabilities: the controller reports what it can change on each device and
  within which range (`nvidia-smi -q -d SUPPORTED_CLOCKS`, the power limit
  range and default; the CPU's frequency range or intel_pstate's
  `min_perf_pct`). A setting the device does not support, a value outside its
  range, or one for a device whose controller has not reported yet is refused
  with `400`.
- Generation: every change of a device's setting in effect increments its
  generation; the controller reports the generation it applied, so `hardware
  show` tells `in sync` from `pending` and `failed` (with the error).
- Events: `device_setting_requested`, `device_setting_cleared` (with why: an
  operator, a released lease, the declaration), `device_setting_applied` and
  `device_setting_failed` (once per distinct error).

| Command | What it does |
|---|---|
| `kairo hardware show [HOST] [--json]` | per host: its nodes and controller (when last seen), then per device its policy, the setting in effect and its owner, what is applied, and `in sync`, `pending`, `failed`, `reset` (the controller returned it to defaults on its own), `not reported`, `observe only` or `not controlled`; then the errors, the settings overridden and what each device supports |
| `kairo hardware set HOST DEVICE [--graphics-clock-max MHz] [--memory-clock MHz] [--power-limit W] [--cpu-max-freq MHz \| --cpu-max-perf PCT]` | sets the device's manual setting (as a whole: what is left out is the default) |
| `kairo hardware reset HOST DEVICE` | clears the manual setting: the device follows a bench or policy setting, else returns to its defaults |

`POST /api/hardware/{host}/{device}/settings` takes `{"settings", "owner":
{"kind", "ref"}, "actor"}` (owner defaults to manual, whose ref is the actor;
a bench or policy ref is its lease id) and `DELETE` the same without
settings; both answer `{"host_id", "device_id", "generation", "effective",
"cleared"}`. `GET /api/hardware` answers `{"hosts"}` as `hardware show
--json` prints them. In-process components use the store's
`SetDeviceSetting`, `ClearDeviceSetting` and `DevicePolicies`.

#### The controller

`kairo controller --config kairo-controller.toml` (see
`examples/kairo-controller.toml`) runs on the physical host, with `server_url`,
`token`, `ca_file` and `insecure_http` as for an agent and a `[node]` of its
own: give it a node token (`kairo token create --role node --node
pve0-host`), start it once so its node is known, then name that node as the
host's `controller`. Its node has no executors and is never scheduled; a node
that already exists (one that also runs an agent) is left as it is. Every
`poll_interval_seconds` (10) it fetches the settings of the devices it
controls, which is every device with `control = true` of the host naming it
and nothing else, changes what differs, reports what is in place, what failed
and what each device supports, and counts as its heartbeat.

- GPUs, by UUID with `nvidia-smi -i`: `-lgc LOWEST,MAX` / `-rgc`, `-lmc M,M` /
  `-rmc`, `-pl W` / `-pl` `power.default_limit`. A reset the card does not
  offer (exit code 3) is already the default.
- Linux CPUs: intel_pstate's `max_perf_pct` when the driver has it (default
  100), else cpufreq's `scaling_max_freq` of every CPU (default its
  `cpuinfo_max_freq`).
- Windows CPU: the AC value of `PROCFREQMAX` (and `PROCFREQMAX1` where Windows
  has it) of the active power scheme, which is then re-activated. What the
  user had there cannot be read back after a change, so the controller writes
  it to its state file (`state_file`, default `<config name>.state.json` next
  to the configuration) before its first change of a scheme, never with values
  it set itself, and writes it back on reset. A state file it cannot read
  stops it: fix or remove it by hand.

It errs toward the hardware's defaults:

- On start every controlled device is brought to its desired setting, or
  reset to its defaults when it has none (a crash may have left settings).
- On SIGTERM or Ctrl-C it resets every device it may have changed, then
  reports.
- When the daemon has not answered for `reset_after_seconds` (120), it resets
  every device, and applies the desired settings again once the daemon is
  back. The state file also lists the controlled devices, so a controller
  restarted while the daemon is down resets them too.
- A change that fails is reported and retried after 5 s, doubling up to 5 min
  (a new request is tried at once); it never stops the loop.
- A device that leaves its control (dropped from the host, `control = false`,
  another controller) is reset at once if it may have changed it (a failed
  reset is retried, five times at most), then never touched again.

A setting in place is carried out again every 10 minutes, in case a driver
reload or another controller's reset undid it. An observe-only daemon tells a
controller it controls nothing: it changes no setting, and only returns the
devices it may have changed (those its state file lists) to their defaults.
Changing GPU clocks, power limits and processor settings needs
privileges the daemon does not have:

- Linux (a Proxmox host, as root; containers see the GPU but may not change
  it): a systemd unit,

  ```ini
  # /etc/systemd/system/kairo-controller.service
  [Unit]
  Description=Kairo device controller
  After=network-online.target nvidia-persistenced.service
  Wants=network-online.target

  [Service]
  ExecStart=/usr/local/bin/kairo controller --config /etc/kairo/kairo-controller.toml
  Restart=on-failure
  RestartSec=10
  KillSignal=SIGTERM
  TimeoutStopSec=150

  [Install]
  WantedBy=multi-user.target
  ```

  then `systemctl enable --now kairo-controller`. Keep the NVIDIA persistence
  daemon running: without it the driver may drop clocks and limits when no
  process holds the GPU.
- Windows (elevated; the daemon itself runs unelevated): a scheduled task
  with the highest run level, started at boot,

  ```powershell
  $action = New-ScheduledTaskAction -Execute "D:\Kairo\kairo.exe" -Argument "controller --config D:\Kairo\kairo-controller.toml" -WorkingDirectory "D:\Kairo"
  $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
  $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
  Register-ScheduledTask -TaskName "kairo-controller" -Action $action -Trigger (New-ScheduledTaskTrigger -AtStartup) -Principal $principal -Settings $settings
  ```

  `Stop-ScheduledTask` and a shutdown end it without its graceful stop: the
  GPUs return to their defaults with the driver at boot, and the processor
  settings when the controller next starts (from its state file). Ctrl-C in
  a console is a graceful stop.

The controller warns at start when it is not elevated (Windows) or not root
(Linux); its changes then fail and are reported as such.

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
`argv`, `cwd`, `executor` labels, `exclusive` or `capacity` of single ranks:

```toml
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
argv = ["/root/.local/bin/uv", "run", "train.py"]
cwd = "/srv/project"
executor = { labels = { environment = "linux" } }
[[tasks.execution.gang.ranks.exclusive]]   # this rank's node has two GPUs
kind = "gpu"
count = 2
same_node = true
[tasks.execution.gang.ranks.capacity]
ram_bytes = 17179869184
```

A rank's `exclusive` (`exclusive = []` for no GPU) or `capacity` replaces the
task's for that rank as a whole; a rank without them asks for the task's. The
task's `env` reaches every rank, so it may not set `CUDA_VISIBLE_DEVICES` when
any rank leases a GPU.

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
- A waiting gang preempts like a single execution (each rank on its node,
  see Admission); its running ranks can be preempted.

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

## Hardware benchmarks

Measurement primitives for hardware efficiency: each runs one fixed,
deterministic workload for a given time and streams what it did, so an
orchestrator can window the intervals, compare them with a reference run and
combine them with energy, at each device setting it sweeps (clock caps,
power limits). They record measured facts, stamped with the date and the
workload version, and never compare with vendor specifications. A bench is
an ordinary execution: its argv is

```text
kairo bench run WORKLOAD [--seconds S] [--interval S] [workload flags]
kairo-bench-gpu WORKLOAD --device GPU-UUID [--seconds S] [--interval S] [flags]
kairo bench serve --listen HOST:PORT        # the peer of net-latency / net-bandwidth
```

`--seconds` (default 60; 0 runs until stopped) ends the run, as does SIGTERM,
Ctrl-C or CTRL_BREAK: the run stops issuing work, waits for the requests in
flight, writes its final interval and end record, cleans up and exits 0. A
usage error (a bad or unsafe parameter) exits 2 and writes no record; a run
refused by the machine (too little free space or memory, no such device) or
failing writes an end record and exits 1. `kairo bench run WORKLOAD --help`
lists a workload's flags.

| Workload | Work | Unit |
|---|---|---|
| `cpu-compute` | per iteration 8 FP64 FMA chains + one xorshift64 step in registers (22 ops), `--threads` (default every logical CPU) | op |
| `cpu-single` | the same on one thread pinned to `--cpu` (start says whether the pin held) | op |
| `cpu-stream` | STREAM triad `a=b+3c` over three `--bytes` arrays (default 1 GiB each; 24 bytes per element: 2 reads + 1 write, write-allocate not counted) | byte |
| `disk-seq` | 1 MiB sequential `--mode read\|write` at `--qd 1\|32` | byte |
| `disk-rand` | 4 KiB random `--mode read\|write`; `--qd 1` for latency, 32 for IOPS (`ops`) | byte |
| `disk-fsync` | 4 KiB write + fdatasync / FlushFileBuffers, one at a time | byte |
| `disk-sustained` | 1 MiB sequential writes (default `--qd 32`) until `--max-write-bytes`: per-interval bandwidth shows SLC cache cliffs | byte |
| `net-latency` | 64-byte TCP ping-pong (TCP_NODELAY), each echo checked | request |
| `net-bandwidth` | 128 KiB writes (`--dir send`) or reads (`--dir recv`) on `--streams 1\|4` connections, application bytes | byte |
| `gemm` (GPU) | cuBLAS bf16 tensor-core GEMM, fp32 accumulate, `--n` 8192: 2n³ per op | flop |
| `copy` (GPU) | device-to-device copy, `--bytes` total footprint (default 2 GiB): read + write counted | byte |
| `gemv` (GPU) | bf16 n×n matrix times `--batch 1\|8` vectors (memory-bound, like LLM decode) | byte, flop |
| `pcie` (GPU) | pinned host memory `--dir h2d\|d2h`; `--mode bandwidth` 64 MiB transfers, `latency` 4 KiB transfers with percentiles | byte |

### Records

One JSON object per line. Every record has `schema` (`kairo.bench/v1`),
`type` (`start`, `interval`, `end`), `run_id` (16 hex digits), `program`
(`kairo` or `kairo-bench-gpu`), `workload`, `wall` (UTC, RFC 3339 with
nanoseconds) and `mono_ns` (monotonic nanoseconds since the start record;
QueryPerformanceCounter on Windows, whose Go clock follows the timer tick).

- `start`, written once setup is done (files prefilled, memory allocated,
  connections open, GPU warm), right before the timed work: `version` (the
  workload definition; it changes when a unit of work changes), `build` (VCS
  revision), `params` (every effective parameter), `units` (primary first),
  `host` (`hostname`, `os`, `os_version` (kairo only), `arch`, `cpu_model`,
  `logical_cpus`), `device`
  (what identifies the device under test, below) and `setup` (facts of the
  setup: `direct_io`, `prefill_ns`, `pinned`, the GPU batch size...).
- `interval`, every `--interval` seconds (default 1) at fixed boundaries
  `start + k·interval`: `index`, `begin_ns`, `end_ns` (each interval begins
  where the previous ended; the last is partial: kairo credits it the
  requests in flight at the stop, kairo-bench-gpu ends it at the deadline or
  signal and drains its in-flight batches uncounted), `work` (`{unit: count}`
  completed in the interval, credited when an operation completes), `ops`
  (operations: requests, round trips, kernel chunks, GPU ops),
  `device_busy_ns` (GPU-measured busy time, CUDA events; null for kairo) and
  `latency` (`count`, `min_ns`, `mean_ns`, `p50_ns`, `p99_ns`, `p999_ns`,
  `max_ns`, or null). GPU intervals add `nvml`: `samples`, `energy_mj` (the
  energy counter's delta over the interval), `power_mw`, `sm_clock_mhz`,
  `mem_clock_mhz`, `temperature_c`, `util_gpu_pct`, `util_mem_pct` (each
  `{min, mean, max}` of the `--sample-ms` samples), `pstate` (`{min, max}`)
  and `clocks_event_reasons` (the OR of the samples); a value NVML cannot
  read is null.
- `end`, always last: `reason` (`duration`, `signal` or `limit` — exit 0;
  `error` or `refused` — exit 1), `limit` and `limit_detail` for a limit,
  `error`, `elapsed_ns` (the measurement window: the last interval's end;
  `mono_ns` is later, after cleanup), `intervals`, the `work`, `ops`, `device_busy_ns` and
  `latency` of the whole run, `energy_mj` (GPU), and for storage
  `bytes_written` (the probe and the prefill included) and `file_removed`. A
  run refused or failing before its start record writes only an end record,
  which then carries `params` and, when known, `device`.

Percentiles are nearest-rank over a log-linear histogram (exact below 128 ns,
then 128 buckets per power of two: within 0.4%); both programs use the same
buckets. `device` is `kind` `cpu` (`model`, `logical_cpus`, `gomaxprocs`,
`fma`), `memory` (the same plus `total_bytes`, `available_bytes`), `storage`
(`path`, `volume_path`, `filesystem`, `free_bytes`, `total_bytes`, sector
sizes, `disk_model`, `disk_serial`, `disk_revision`, `disk_bus`, and on Windows
`volume_serial`, on Linux `mount_source`, `block_device`, `rotational`),
`network` (`local_addr`, `remote_addr`, the `interface` holding the local
address — the one the route chose — its `mtu`, and the OS-reported
`link_speed_tx_bps` / `link_speed_rx_bps`, a fact that a virtual NIC may
report meaninglessly) or `gpu` (below).

### Safety

- Storage: the test file `kairo-bench-<id>.tmp` in `--path` (an existing
  directory on the filesystem to test) is refused unless `--file-size` (1 GiB;
  disk-fsync 64 MiB, disk-sustained 8 GiB) ≤ free space − `--keep-free`
  (5 GiB) − 1 GiB. Every byte written counts against `--max-write-bytes`
  (20 GiB, the prefill included; a prefill that alone would pass it is
  refused), and the run ends with reason `limit` before the write that would
  pass it; a write run also ends when free space falls below `--keep-free`.
  The OS cache is bypassed (Windows `FILE_FLAG_NO_BUFFERING |
  FILE_FLAG_WRITE_THROUGH`, Linux `O_DIRECT`, 4 KiB-aligned buffers or the
  device's sector size); a filesystem that refuses it (tmpfs) runs buffered
  with `setup.direct_io` false. The file is removed on every exit: Windows
  opens it `FILE_FLAG_DELETE_ON_CLOSE` and Linux unlinks it as soon as it is
  open, so even a killed run leaves nothing. Writes are incompressible and
  every 4 KiB sector is distinct.
- `cpu-stream` refuses arrays that, with a 2 GiB margin, exceed the available
  memory.
- `kairo bench serve` echoes, sinks or sources one test per connection (at
  most 16 at a time, each closed after `--max-conn-duration`, default 1h),
  writes one JSON line per connection, and exits after `--idle-timeout`
  (default 5m) without connections. It serves anyone who connects: listen on
  a trusted network.

### GPU bench

`bench/gpu/kairo-bench-gpu.cu` (CUDA C++, cuBLAS, NVML) is built separately
for Ampere (sm_86, RTX 3080 / 3060) with a compute_86 PTX fallback:

```text
cmd /c bench\gpu\build-windows.cmd     # CUDA 13.x + MSVC 2022 x64 (finds vcvars64 through vswhere)
make -C bench/gpu                      # Linux, CUDA_HOME=/usr/local/cuda; CUBLAS_STATIC=1 links cuBLAS statically
```

Both embed the revision (`BUILD_REV=...` overrides it). cudart is linked
statically; NVML is loaded at run time (System32 `nvml.dll`,
`libnvidia-ml.so.1`) and optional — without it every `nvml` value is null.
On Windows cuBLAS is delay-loaded: `gemm` and `gemv` need `cublas64_13.dll`
and `cublasLt64_13.dll` on PATH or beside the exe (a missing one is a clean
`error` end), `copy` and `pcie` need no cuBLAS (their `cublas_version` is
null). On Linux the shared cuBLAS is needed even for `--help` unless linked
statically. The driver must support the toolkit it was built with.

- `--device GPU-UUID` (required, as `nvidia-smi -L` prints it) is matched
  against the visible devices' UUIDs (whatever `CUDA_VISIBLE_DEVICES` is);
  no match is refused, listing the visible UUIDs.
- Flags: `--seconds`, `--interval`, `--sample-ms` (NVML sampling period,
  default 100), `--n` (gemm, gemv; multiple of 64, 256..32768), `--bytes`
  (copy; binary suffixes, a multiple of 2 MiB), `--batch 1|8` (gemv), `--dir
  h2d|d2h` and `--mode bandwidth|latency` (pcie), `--reserve-mib` (512: device
  memory left free). A flag that does not apply to the workload is a usage
  error. `--help`, `--version` (JSON: build, workload versions) and
  `--validate-only` (the effective parameters as one `"type":"params"` line)
  return before any CUDA, cuBLAS or NVML call.
- Memory is checked, never tried: before creating a context NVML's free
  memory must hold the buffers, the reserve and a 400 MiB context estimate;
  after it `cudaMemGetInfo` must hold the buffers and the reserve, and the
  free memory after allocating must still hold the reserve. Otherwise the run
  is refused.
- Inputs are filled on the device with a fixed pseudo-random pattern in
  [-1, 1) (data values change power). Work is issued in batches calibrated to
  about 20 ms, two in flight, bracketed by CUDA events: a batch counts in the
  interval in which its end is seen, and `device_busy_ns` sums the event
  times. pcie latency mode issues and synchronizes each 4 KiB transfer alone
  (spin-wait, `setup.sync` `spin`; the others block) and reports host-measured
  latencies, their sum as `device_busy_ns`.
- `device` (`kind` `gpu`): `uuid`, `name`, `pci_bus_id`,
  `compute_capability`, `sm_count`, `memory_total_bytes`,
  `memory_free_bytes`, `driver_version`, `cuda_driver_version`,
  `cuda_runtime_version`, `cublas_version`, `nvml_available`,
  `power_limit_mw` (enforced), `applications_clock_mhz` and `max_clock_mhz`
  (`sm`, `mem`, as the device reports them), `vbios_version`. The main thread
  reads the energy counter at every interval boundary, so `energy_mj` covers
  exactly the interval's work window.
- SIGTERM, SIGINT, CTRL_C, CTRL_BREAK and CTRL_CLOSE stop it cleanly (exit
  0); a very long op (gemm at n=32768 is ~2 s) delays the stop by up to two
  ops.

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
starts attempts without their declared `env`, and a daemon older than the
agent refuses its log problem reports (the agent logs them) and its
observations, which carry power samples (its resources then go stale). A
daemon opening an older database migrates it in place (the `cancelled` task
state, reruns, the `env` column, the machine declaration, which starts out
declaring every registered GPU for the node it is registered on, and its
(uuid, node) key for shared GPUs, the attempt log problems, the admission
tables, the power and energy tables, the hosts and device settings, which
start out empty); keep the backup, since an older binary would read a rerun
Task's earlier execution as its outcome again.

## Verification

```powershell
go test ./...
go test -race ./...
go vet ./...
uv run --project sdk/python pytest -q
git diff --check
```

Real NVIDIA, WSL, process-kill, and DDP runs are host-specific integration
tests rather than ordinary unit tests, and so are the controller's changes of
real clocks, power limits and processor settings (its tests use fake
actuators). The bench tests run tiny workloads (TempDir files of a few MiB,
loopback); `kairo-bench-gpu` is only built by them, and a GPU bench belongs on
an idle GPU.
