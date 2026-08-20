---
name: cluv-clusters
description: Connect to HPC clusters with cluv (SSH + 2FA), and temporarily disable/re-enable clusters so other cluv commands skip them. Use when the user wants to log in, hits an SSH/2FA prompt issue, or wants to stop targeting a cluster that's down or out of allocation for a while.
---

# Managing cluster connections with cluv

## Connecting: `cluv login`

Every command that talks to a remote cluster (`sync`, `submit`, `status`, `clean`) needs a live SSH
connection first. Run this before those commands, or let them fail with a clear connection error.

```bash
cluv login              # connect to every cluster in [tool.cluv], skipping already-connected ones
cluv login mila narval   # connect to specific clusters only
```

Key behaviors:

- Connections reuse existing SSH **ControlMaster** sockets. `cluv login` checks
  `control_socket_is_running` for each cluster first, and only opens a *new* connection for
  clusters that don't already have one — so re-running `cluv login` is cheap and won't
  re-trigger 2FA for clusters you're already connected to.
- New connections are established **sequentially, not in parallel**, specifically to avoid firing
  off multiple simultaneous 2FA prompts (which is confusing and can cause an OTP to be entered
  against the wrong prompt).
- If the current machine already *is* one of the configured clusters (e.g. running `cluv login`
  from a Mila login node), that cluster is skipped — no need to SSH to yourself.
- Disabled clusters (see below) are skipped automatically, with a warning listing them.
- A connection failure for one cluster doesn't abort the others; it's logged in red and login
  continues with the rest.

If the user doesn't have SSH access configured yet, point them at
[milatools](https://github.com/mila-iqia/milatools) to generate a working `~/.ssh/config`.
Cluster hostnames in `[tool.cluv]` must exactly match the `Host` entries there.

### Why some commands avoid triggering 2FA

Commands like `sync` are meant to be run often, sometimes unattended, so they use a helper
(`login.get_remote_without_2fa_prompt`) that only returns a `Remote` for clusters that already
have a live control socket — it never blocks waiting for 2FA. Run `cluv login` explicitly first
if you want `sync`/`submit`/etc. to actually establish new connections rather than silently
skipping unconnected clusters.

## Temporarily skipping a cluster: `cluv disable` / `cluv enable`

Use this instead of editing `[tool.cluv]` when a cluster is down, out of allocation, or otherwise
should be skipped for a while — it's a local, reversible flag, not a config change.

```bash
cluv disable narval          # skip narval indefinitely, until explicitly re-enabled
cluv disable narval 2h       # skip for 2 hours
cluv disable narval 1d 6h    # chainable suffixes: d/h/m/s
cluv disable narval 3        # a bare integer means days
cluv disable narval 6:00:00  # Slurm-style HH:MM:SS / D-HH:MM:SS also accepted

cluv enable narval           # re-enable before the period expires
```

- A disabled cluster is skipped by `login`, `sync`, `submit`, and `clean` whenever they're run
  *without* an explicit cluster list — passing the cluster name explicitly still skips it too,
  since the check happens centrally in `login`.
- Disabled clusters and their expiry are stored locally (not synced anywhere), so this is
  per-machine state.
- Every command that would normally connect prints a warning listing currently disabled clusters
  and how much time is left, so it's easy to notice why a cluster was skipped.
- `cluv enable <cluster>` on a cluster that isn't disabled prints the list of clusters that
  actually are disabled, instead of erroring.

## Related

- [[cluv-sync]] and [[cluv-submit]] both call into `login` internally to get connections.
- [[cluv-status]] shows a cluster as "disconnected" if there's no live connection to it.
