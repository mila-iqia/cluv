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
3. If `remotes` is empty, print a message telling the user to run `cluv login` first,
   and return without invoking `clush`. (Unlike `sync`/`clean`, which `raise
   RuntimeError(...)` in this situation — that produces an unhandled traceback since
   nothing in `__main__.py` catches plain `RuntimeError`. `sh()` intentionally prints
   and returns instead, since there's no cluster-selection argument to make raising
   meaningful here.)

## Running `clush`

1. Write the connected hostnames to a `tempfile.NamedTemporaryFile`, one per line.
2. Run:
   ```
   uvx --from=clustershell clush --hostfile <tmpfile> <command...>
   ```
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
- Unit test the "no connected clusters" path prints the expected message and does not
  invoke `clush`.
- Unit test that the hostfile passed to `clush` contains exactly the connected
  hostnames, one per line, and that the temp file no longer exists after the call
  returns.
- Unit test that a non-zero `clush` return code results in `sys.exit` being called
  with that code (mock `asyncio.create_subprocess_exec` to avoid actually invoking
  `uvx`/`clush` in tests).
