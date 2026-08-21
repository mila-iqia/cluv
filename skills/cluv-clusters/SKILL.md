---
name: cluv-clusters
description: Connect to HPC clusters with cluv login (SSH, handling 2FA safely), and temporarily skip a cluster with cluv disable/enable. Use when the user wants to log in, hits an SSH/2FA issue, or wants a down/out-of-allocation cluster skipped for a while without touching the config.
---

# Connecting to and disabling clusters

## `cluv login` — establish SSH connections

Every command that talks to a remote cluster (`sync`, `submit`, `status`, `clean`) needs a live
connection first.

```bash
cluv login               # connect to every cluster in [tool.cluv], skipping ones already connected
cluv login mila narval    # connect to specific clusters only
```

Key behaviors:

- Connections reuse existing SSH **ControlMaster** sockets — `cluv login` checks for a running
  control socket per cluster and only opens a *new* connection for the ones that don't have one.
  Re-running it is cheap and won't retrigger 2FA for clusters you're already connected to.
- New connections are established **sequentially**, deliberately, so multiple 2FA prompts never
  fire at once (which is confusing and can send an OTP to the wrong prompt).
- If the machine running `cluv login` is itself one of the configured clusters (e.g. run from a
  Mila login node), that cluster is skipped.
- Disabled clusters (see below) are skipped automatically, with a warning listing them.
- A connection failure on one cluster doesn't abort the rest — it's logged and login continues.

If the user has no SSH config for the clusters yet, point them at
[milatools](https://github.com/mila-iqia/milatools) to generate one. Cluster hostnames in
`[tool.cluv]` must exactly match the `Host` entries there.

### Why some commands never trigger 2FA

`sync`/`submit`/etc. only use *already-connected* remotes (via a helper that checks for a live
control socket and returns `None` rather than blocking on 2FA if there isn't one). Run `cluv login`
explicitly first if you want those commands to establish new connections rather than silently
skipping clusters that aren't connected yet.

## `cluv disable` / `cluv enable` — temporarily skip a cluster

Use this instead of editing `[tool.cluv]` when a cluster is down or out of allocation for a while —
it's local, reversible state, not a config change.

```bash
cluv disable narval          # skip indefinitely, until explicitly re-enabled
cluv disable narval 2h       # skip for 2 hours
cluv disable narval 1d 6h    # chainable suffixes: d/h/m/s
cluv disable narval 3        # a bare integer means days
cluv disable narval 6:00:00  # Slurm-style HH:MM:SS / D-HH:MM:SS also accepted

cluv enable narval            # re-enable before the period expires
```

- A disabled cluster is skipped by `login`, `sync`, `submit`, and `clean` whenever no explicit
  cluster list is given — and even when it *is* named explicitly, since the disabled check happens
  centrally inside `login`.
- Disabled clusters and their expiry are stored locally (per-machine), not synced anywhere.
- Commands that would normally connect print a warning listing currently disabled clusters and how
  much time is left, so it's easy to see why one was skipped.
- `cluv enable <cluster>` on a cluster that isn't disabled just prints the list of clusters that
  actually are disabled, instead of erroring.

## Related

- `cluv sync` and `cluv submit` both rely on `login` internally to get connections.
- `cluv status` shows a cluster as "disconnected" when there's no live connection to it — which
  also happens for a disabled cluster, since it's skipped the same way.
