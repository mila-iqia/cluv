# `cluv sh` — run a raw command on all connected clusters

## Problem

There's no quick way to run an ad-hoc shell command (e.g. `squeue --me`, `df -h $SCRATCH`)
across every cluster the user is currently connected to. `cluv run` is close but does
something different: it syncs the project via git, then runs the command wrapped in
`uv run` on a *single* cluster. `cluv sh` is a separate, lighter command: no sync, no
`uv run` wrapping, and it always targets every currently-connected cluster at once.

## Command

```
cluv sh <command...>
```

- `<command...>` is a `nargs=argparse.REMAINDER` positional, same shape as `run`'s
  `command` argument — everything after `sh` is passed straight through.
- No cluster-selection argument. `cluv sh` always targets every cluster that is
  currently connected (see below). There is no way to narrow the set for this command;
  if the user needs a single cluster they can use `cluv run` or SSH directly.
- Examples:
  - `cluv sh squeue --me`
  - `cluv sh df -h '$SCRATCH'`

## Determining "connected" clusters

`cluv.cli.sync.get_active_remotes()` already implements exactly this filtering (and is
already reused by `clean` and `submit`), so `sh()` reuses it rather than reimplementing
the logic:

```python
async def get_active_remotes() -> list[Remote]:
    """Returns the Remotes for each cluster which has an active SSH connection.

    Disabled clusters (see `cluv disable`) are excluded.
    """
```

Internally it does: start from `get_cluv_config().clusters_names`, drop
`current_cluster()` if present, drop disabled clusters (`get_disabled_clusters()`), then
check `control_socket_is_running(hostname)` for each remaining cluster in parallel and
keep only those with an active connection. It never establishes a new connection
(never triggers a 2FA prompt).

`sh()` calls it like `sync`/`clean` do:

1. `disabled = get_disabled_clusters()`, then `print_disabled_clusters(disabled)` — same
   warning shown by `login`/`sync`/`clean`.
2. `remotes = await get_active_remotes()`; take `hostnames = [r.hostname for r in remotes]`.
3. If `remotes` is empty *and* `current_cluster()` is `None` (nothing to run anything
   on, locally or remotely), print a message telling the user to run `cluv login`
   first, and return without invoking `clush`. (Unlike `sync`/`clean`, which `raise
   RuntimeError(...)` in this situation — that produces an unhandled traceback since
   nothing in `__main__.py` catches plain `RuntimeError`. `sh()` intentionally prints
   and returns instead, since there's no cluster-selection argument to make raising
   meaningful here.) See "Running locally on the current cluster" below for the case
   where `current_cluster()` is set.

## Running locally on the current cluster

`get_active_remotes()` excludes `current_cluster()` from its result (you don't need an
SSH connection to yourself), so if `cluv sh` is invoked *from* a cluster (e.g. the user
is on Mila and runs `cluv sh squeue --me` there), that cluster's own output would never
be shown by `clush` alone. To fix this, `sh()` also runs `command` directly as a local
subprocess (no SSH, no `clush`) whenever `current_cluster()` is set, in addition to (not
instead of) the `clush` call for whichever other clusters are connected.

The local run and the `clush` run happen **sequentially** (local first, then `clush`),
not concurrently — this keeps each command's live output as a clean, uninterleaved
block, consistent with this codebase's existing preference for sequential over
concurrent execution when live output/prompts could otherwise collide (e.g. `login`
connects to clusters sequentially to avoid overlapping 2FA prompts).

Both are skipped/included independently based on what's actually available:
- Not on a cluster (`current_cluster()` is `None`) and no clusters connected: print the
  "not connected" message and return, as before.
- Not on a cluster, but some clusters connected: run `clush` only (today's behavior).
- On a cluster, but no other clusters connected: run the command locally only, no
  `clush` invocation at all (no hostfile is written).
- On a cluster, and other clusters are connected: run locally first, then `clush`.

If the local run fails (non-zero exit code) but `clush` doesn't get skipped as a result
— both always run when both apply. The process exits with the first non-zero return
code encountered, in run order (local's code takes precedence over `clush`'s if both
fail), via the same `sys.exit(returncode)` mechanism described below.

## Running `clush`

1. Write the connected hostnames to a `tempfile.NamedTemporaryFile`, one per line.
2. Run:
   ```
   uvx --from=clustershell clush -S --hostfile <tmpfile> <command...>
   ```
   `-S`/`--maxrc` is required for `clush` to exit with the largest of the per-host
   command return codes — discovered during manual smoke testing that without it,
   `clush` always exits `0` regardless of remote command failures, silently defeating
   the exit-code propagation described below.
   via `asyncio.create_subprocess_exec`, with stdout/stderr **inherited** from the
   parent process (not piped/captured). This lets `clush`'s own TTY detection and
   per-node color-coded output prefixes pass straight through to the user's terminal,
   live, instead of being buffered and reprinted.
3. The temp file is removed automatically when the `with` block exits (after `clush`
   returns).
4. `clustershell` is *not* added as a project dependency — it's fetched on demand via
   `uvx`, the same way `sbatch`/`git`/`rsync`/`ssh` are invoked as external tools rather
   than imported as Python packages. This was verified to work with `uvx` across
   Python 3.11–3.14 (resolves to `clustershell==1.10.1`).
   - Follow-up idea (out of scope for this spec): if a future command (e.g. a real,
     non-mock `cluv status`) needs structured per-node results rather than raw text,
     it may be worth depending on `ClusterShell`'s Python API (`ClusterShell.Task`,
     `NodeSet`) directly at that point.

## Error handling

If `clush` exits non-zero, call `sys.exit(returncode)` directly from within the `sh()`
command function, rather than raising `subprocess.CalledProcessError`. `main()`'s
shared exception handler logs `err.output`/`err.stderr`, which would be misleading here
since output was never captured (it already streamed live, uncaptured, to the
terminal) — the handler would print "No standard output." / "No standard error."
even though the user just watched real output scroll by.

## CLI wiring

- New module: `cluv/cli/sh.py`, exporting an async `sh(command: list[str]) -> None`.
- New `add_sh_args()` in `__main__.py`, following the same shape as `add_run_args()`:
  a single `command` positional with `nargs=argparse.REMAINDER`, registered with
  `_add_v_arg()` like every other subparser.

## Testing

- Unit test the "connected clusters" filtering logic (config clusters minus current
  cluster, minus disabled, minus not-connected) by mocking `control_socket_is_running`
  and `get_disabled_clusters`, similar to existing tests around `login`.
- Unit test the "no connected clusters and not on a cluster" path prints the expected
  message and does not invoke `clush` or run anything locally.
- Unit test that the hostfile passed to `clush` contains exactly the connected
  hostnames, one per line, and that the temp file no longer exists after the call
  returns.
- Unit test that when `current_cluster()` is set, the command is also run locally
  (mocking `current_cluster` and the local-run helper), and that this happens even
  when there are zero other connected clusters (no hostfile/`clush` invocation in that
  case).
- Unit test that local and `clush` runs happen in order (local first) when both apply.
- Unit test that a non-zero return code from either the local run or `clush` results
  in `sys.exit` being called with that code (mock `asyncio.create_subprocess_exec` to
  avoid actually invoking `uvx`/`clush`/local commands in tests).
