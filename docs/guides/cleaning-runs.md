# Cleaning up run results on the clusters

[`cluv sync`](../commands.md#cluv-sync) copies run results (logs, checkpoints, anything under your
`results_path`) from each cluster back to your machine. It's a one-way copy: it
never deletes anything, on either side. Two things follow from that:

- Deleting a run folder locally and running `cluv sync` again just re-downloads
  it — the files are still sitting on the cluster.
- Every job you run adds another folder under `results_path` (usually somewhere
  under `$SCRATCH`) on every cluster, forever. Nothing ever gets removed from
  there on its own.

[`cluv clean`](../commands.md#cluv-clean) is the other half of that workflow: once you're happy with a run and
have deleted its local folder, `clean` removes the matching folder from the
cluster(s) it came from.

## How it decides what's safe to delete

Your local results folder (the `logs` symlink in your project, pointing at
wherever `results_path` resolves to) is treated as the source of truth for what
you still want to keep. Anything under a cluster's `results_path` that has no
matching folder locally is a *candidate* for deletion — but not automatically,
because that description also matches a run that finished on the cluster five
minutes ago and simply hasn't been fetched yet.

To tell those two cases apart, `cluv clean` only considers a remote folder
deletable if **both** are true:

1. There's no folder with that name in your local results folder.
2. It's older than the last time `cluv sync` successfully pulled results from
   that cluster.

In practice: if you've synced recently, anything that showed up on the cluster
*after* that sync is left alone (it hasn't been fetched yet — `clean` never
deletes something you haven't seen), and anything from *before* that sync which
you don't have locally is treated as something you deliberately deleted, and
gets cleaned up remotely too.

This means `clean` needs at least one prior `cluv sync` for a cluster before it
can safely clean it. Clusters that have never been synced are skipped, with a
note telling you to run `cluv sync` first.

!!! note
    Editing files inside a local run folder (adding notes, running analysis
    scripts that write into it, etc.) doesn't affect whether the folder will be deleted or not — `clean` doesn't look
    at your local folders' modification times, only at whether the folder
    exists.

## Usage

Preview what would be deleted, without deleting anything:

```console
cluv clean --dry-run
```

Clean up every cluster you're currently connected to (and that's been synced
before), with a confirmation prompt:

```console
cluv clean
```

You'll see a list of what's about to be removed, grouped by cluster, before
being asked to confirm.

Clean specific clusters only:

```console
cluv clean rorqual narval
```

Skip the confirmation prompt (useful in scripts):

```console
cluv clean --force
```

## Typical workflow

```console
cluv sync              # fetch results from all clusters
rm -rf logs/12345       # you're done with this run, delete it locally
cluv clean              # remove the matching folder from wherever it ran
```

## Things to know

- **Running or pending jobs aren't specially protected.** If a job's output
  folder happens to look old enough and isn't present locally, `clean` doesn't
  check Slurm queue state before removing it. In practice this is unlikely to
  bite you (an actively running job keeps writing to its folder, which usually
  keeps it looking "new"), but it isn't guarded against explicitly.
- **Same job ID on two clusters is treated as two independent decisions.** If a
  folder with the same name exists on two different clusters, `clean` looks at
  each cluster's copy independently. Keeping the local copy keeps it on
  *every* cluster that has a folder with that name, even if only one of them is
  actually "the" run you meant.
- **Deletion happens over SSH (`rm -rf`) and is not recoverable.** There's no
  remote trash/undo. Use `--dry-run` first if you're unsure.
