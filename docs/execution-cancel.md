# Cancelling a started execution

## What is missing

`withdraw` is defined for `waiting` only, and correctly so -- it is coordination
revocation, not semantic cancellation, and before start there is no process to
revoke. Once an execution is `started` there is no Kairo-level operation that
ends it.

The remaining paths do not fill the gap:

- `project pause` sends a suspend command. It only works if the process
  implements the cooperative protocol. A plain wrapper script does not, and the
  operation either sits in `pause_quiescing` with nobody to answer or resolves
  to `blocked` / `execution_not_checkpointable`. Both leave the admission gate
  closed, so one unstoppable job also stops everything else in the project.
- Killing the process from outside. This is what actually happens, and it is
  what this document exists to replace.

Cancel and pause are different axes, and neither implies the other:

| | intent | cooperative | resumable |
|---|---|---|---|
| `withdraw` | never start | n/a, no process exists | n/a |
| `pause` | continue later | yes, needs the protocol | yes |
| `cancel` | discard | no | no |

Cancellation therefore should not be opt-in per execution. Anything Kairo
started, Kairo should be able to end. `checkpointable` says whether work can be
resumed; it says nothing about whether an operator may throw it away.

## Evidence from 2026-09-11

Five started executions were ended by killing processes directly, because no
other option existed. All five were operator decisions, not failures:

- a 400-trial pfgen sweep, superseded when the backend changed
- a decoding grid whose tool was edited mid-run
- two configuration screens, one contending with a scheduled job for the GPU,
  one pinned to the wrong GPU by a flag in the submission
- a temperature sweep that became irrelevant

Two consequences showed up immediately.

**Process trees, not processes.** Killing the two shard processes of the
temperature sweep did not stop it: the PowerShell wrapper survived, moved on to
the next temperature cell, launched new shards, and kept the GPU busy. It was
still holding the card while a later measurement ran against it, and those
numbers had to be discarded. A cancel that terminates one process is not a
cancel.

**The history is now misleading.** A cancelled attempt records
`exit_code = 4294967295` and nothing else, so "the configuration was wrong",
"this became unnecessary", "it crashed" and "it ran out of memory" are
indistinguishable after the fact. An experiment log where most entries look like
crashes is hard to audit.

## Proposed slice

1. `POST /v2/executions/{id}/cancel`, valid while `started`.
2. Terminal causes `cancelled_by_operator` and `runtime_limit_exceeded`,
   alongside the existing `withdrawn_before_start`.
3. `[lifetime] max_runtime`, measured from the attempt's `started_at`, and
   optionally `start_deadline`, which withdraws rather than cancels because
   nothing has started. `max_runtime` must not be expressed as a TTL: in a queue
   that holds work for hours, "from submission" and "from start" are very
   different quantities, and the name has to say which.
4. Executor-side termination of the whole process tree. On Windows this means a
   Job Object; a WSL executor has to reach the descendants inside the VM.
5. Lease release only after absence is confirmed.

Point 5 is the one that is easy to get wrong. A cancel that returns 200 has
requested termination, not completed it. The existing invariant --- process exit
is not quiescence, and quiescence is not lease release --- has to hold here too:

```
cancel requested
  -> executor terminates the process tree
  -> registered processes absent
  -> provider observation confirms the resource is unclaimed
  -> lease released
  -> execution terminal, cause cancelled_by_operator
```

Releasing the lease when the API returns would let the next job take the GPU
while a surviving child still holds memory on it. That is not hypothetical: it
is exactly the failure above, with the wrapper outliving its shards.

## What not to build

A separate `TemporaryTask` type, or a `cancellable` flag. The requirement is not
a second kind of work; it is an execution-level operation that is missing. A
`--temporary` convenience flag on the CLI is fine as sugar over a normal
submission --- exploratory priority, a `max_runtime`, `checkpointable = false`
--- but it should expand to an ordinary Execution and add nothing to the state
machine.

## Related

Two other issues from the same day compound this one. The scheduler attempts
only the top priority tier, so a stuck high-priority request blocks the whole
queue; and a `[[capacity.disks]] filesystem` value that matches no registered
resource fails admission silently, leaving no `admission_blocks` row. Together
they mean a single mistyped submission can stall a queue with no recorded
reason and, today, no way to cancel it.
