# `cluv sweep`: packed, multi-job hyperparameter sweeps

## Context

`cluv` currently has two ways to run something on a cluster: `cluv submit` (one job, one command) and the separate Hydra-based `cluv_launcher.py` plugin, which submits **one `sbatch` job per hyperparameter combination** via Hydra's multirun. That's wasteful for small/short jobs — each combo pays its own queueing/startup cost, and it doesn't use GPU packing.

`cluv sweep` instead expands a comma-list sweep spec (e.g. `--foo=123,456 --baz=eee,fff` → 4 combos) and packs multiple combos onto each GPU via SLURM's `--ntasks-per-gpu` (`srun` launches that many tasks per GPU; each task uses its `$SLURM_PROCID` to figure out which combo it is and patches its own `sys.argv` before its normal arg-parsing runs — no per-combo file/index needs to be synced anywhere).

**Important architectural correction from the first design pass**: this is *not* one giant packed job. `cluv sweep` submits **one job per GPU** on clusters that allow partial-node GPU allocation, or **one job per node** on clusters that require whole-node allocations (`full_node_allocations`) — each such job is sized to exactly one GPU's (or one node's) packing capacity, and cluv submits as many of these small, identically-shaped jobs as needed to cover all combos. This schedules far more easily than one enormous multi-GPU job, and a single GPU failing only takes out its own job.

This means a task's global position in the combo list is **not** determined by `$SLURM_PROCID` alone — `$SLURM_PROCID` is local to *its own job* (e.g. task 2 of the second submitted job is a different combo than task 2 of the first job). Each job is given a small, distinct `CLUV_SWEEP_TASK_OFFSET` env var at submit time (`job_index * job_capacity`), and the runtime combines `offset + $SLURM_PROCID` to get the global combo index. Resumability is unaffected by this: it's still keyed purely on the sweep name + the concrete combo's argument values (not job id, not task index, not this offset), so which job/task a given combo happens to land on this time doesn't matter for where its checkpoint directory ends up.

There's already a **dead stub** for exactly this packing mechanism in `cluv/cli/submit.py:get_sbatch_command()` (`in_job_packing = False; assert not in_job_packing, "todo"`, with an already-written but unreachable `%j_%t`-based output-path branch right below it) — this feature activates that stub, called once per submitted job.

This is split into two phases, confirmed with the user:

- **Phase 1** — the core mechanism, no `--vram`. The user manually passes whatever `--ntasks`/`--gpus`/`--ntasks-per-gpu` sbatch args they want for a *single job's* footprint; cluv computes that job's task capacity from those args (or defaults to capacity=1, i.e. one job per combo, if none are given — this subsumes today's "one job per command" behavior as the simple default), then submits `ceil(n_combos / capacity)` identically-shaped jobs, each offset differently. Ships the runtime API (`patch_argv()`), with sweep-name-based resumable run dirs computed by the *existing* `cluv.job.current_run_info()` — no new parallel run-info type. Fully useful and shippable alone.
- **Phase 2** — adds `--vram <gb>`: instead of the user picking one job's `--ntasks`/`--gpus`/`--ntasks-per-gpu` by hand, cluv derives them from `--vram` + a GPU type the user still must specify explicitly (e.g. `--gres=gpu:h100:1`) + a GPU VRAM lookup (hardcoded table + a per-cluster GPU inventory fetched over SSH and cached ~monthly), **and** decides the per-job granularity itself: one GPU per job normally, or one full node per job when the target cluster's new `full_node_allocations` config flag is set (using the same cached inventory to learn GPUs-per-node). Phase 2 requires no changes to the Phase 1 runtime module — it only changes how `cluv/cli/sweep.py` computes one job's sbatch args/capacity before replicating it across `num_jobs`.

Each phase ships as its own PR — Phase 1's PR lands and is usable on its own (see above) before Phase 2's work starts.

---

## Phase 1 — core sweep mechanism (no `--vram`)

### 1. New file `cluv/sweep.py` — SSH-free runtime module

Imports only stdlib + `cluv.config` + `cluv.utils` (never `cluv.remote`/`cluv.cli.*`/`milatools` — this gets imported inside users' training scripts). Deliberately exposes no run-info type of its own — it only computes and stashes the raw ingredients (sweep name, slug, resolved combo) that `cluv/job.py:current_run_info()` reads (see §2 below).

```python
CLUV_SWEEP_NAME_ENV_VAR = "CLUV_SWEEP_NAME"
CLUV_SWEEP_TASK_OFFSET_ENV_VAR = "CLUV_SWEEP_TASK_OFFSET"   # set per-job by `cluv sweep`, default "0"

def expand_sweep_args(args: list[str]) -> list[list[str]]:
    """Any '--key=v1,v2,...' token (>=2 comma-separated values) is 'swept'; every other
    token is fixed and copied unchanged into every combo. Cartesian product over swept
    flags, in the order they appear. Returns [list(args)] if nothing is swept."""

def patch_argv() -> None:
    """No-op if $CLUV_SWEEP_NAME or $SLURM_PROCID is unset (always safe to call, including
    when running locally or under a plain `cluv submit`). Otherwise:
      combos = expand_sweep_args(sys.argv[1:])
      global_index = int(os.environ.get(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "0")) + int(os.environ["SLURM_PROCID"])
    If global_index >= len(combos): print a message and sys.exit(0) (idle-exit — this is
    the *only* place "out of work" is detected, and it naturally covers both "this job's
    own leftover slots" and any other padding, since it's computed against the full global
    combo list every time). Otherwise mutate sys.argv[1:] in place to combos[global_index],
    and stash the resolved combo + its slug for `_current_sweep_context()` below."""

def _slugify_combo(combo: list[str]) -> str:
    """Filesystem-safe, readable slug: sanitized '--key=value' tokens joined by '-'
    (order preserved), truncated to a sane length, with a short content hash suffix for
    uniqueness/stability under truncation."""

def _current_sweep_context() -> tuple[str, str, list[str]] | None:
    """Private accessor used only by `cluv.job.current_run_info()` (§2). Returns
    `(sweep_name, slug, resolved_combo)` if `patch_argv()` has run in this process and
    resolved a combo (i.e. it wasn't a no-op and didn't idle-exit); `None` otherwise
    (not a sweep, or `patch_argv()` hasn't been called yet)."""
```

Correctness notes to document clearly:
- **Index agreement across processes**: the CLI counts combos over `program_args` (which includes the fixed `python main.py` prefix from the example), while the runtime counts over its own `sys.argv[1:]` (which never includes that prefix — it's consumed before the script's own argv starts). Neither prefix token has a comma, so `expand_sweep_args` treats them as fixed on both sides — combo count and ordering agree either way, so a given global index picks the matching combo on both sides even though the two input lists differ by that constant prefix.
- **Why the offset is required, not optional**: with multiple small jobs, `$SLURM_PROCID` alone only tells a task its position *within its own job*. Two different jobs both have a task with `SLURM_PROCID=2`, but (absent the offset) there'd be no way to tell those two apart or map either to the right global combo — this is exactly the ambiguity flagged during design ("the third task of the second job"). The offset is what disambiguates it: it's a plain integer baked in as an env var at submit time for that one job, computed by `cluv/cli/sweep.py` from `job_index * job_capacity`, so no file or cross-job coordination is ever needed at runtime.
- **One run-info API, not two**: `cluv/job.py`'s existing `current_run_info()`/`RunInfo` — already used by `cluv submit` job scripts, e.g. `examples/pytorch-example/main.py` — is *extended* rather than duplicated (§2). A script written for `cluv submit` needs exactly one added line, a `cluv.sweep.patch_argv()` call before its own arg-parsing, to also work verbatim under `cluv sweep`; that call is a no-op outside a sweep, so the same script keeps working under `cluv submit`/locally too.

### 2. Changes to `cluv/job.py` — extend `current_run_info()` for sweeps

No new dataclass, no new public function. `RunInfo` is unchanged; `current_run_info()` gains one new branch, checked *before* today's job-id-based path:

```python
def current_run_info() -> RunInfo | None:
    if not SLURM_JOB_ID:
        return None
    if SLURM_JOB_ID and not SLURM_PROCID:
        ...  # unchanged: warn, return None

    if sweep_context := cluv.sweep._current_sweep_context():
        sweep_name, slug, combo = sweep_context
        cluster = current_cluster()
        assert cluster, "Example must be run on a cluster."
        cluster_config = cluv.config.current_cluster_config()
        assert cluster_config, "Example must be run on a cluster."
        return RunInfo(
            cluster=cluster,
            run_id=f"{cluster}_sweep-{sweep_name}_{slug}",
            results_path=cluster_config.results_path / "sweeps" / sweep_name / slug,
            command=combo,
        )

    # ...unchanged job-id-based path (current_run_id(), etc.) falls through here
```

`cluv/job.py` gains a plain `import cluv.sweep` at the top — safe, since `cluv/sweep.py` has the same zero-SSH-dependency constraint `cluv/job.py` already has (§1), so there's no import-cycle or remote-plumbing concern.

Note this also finally gives the long-unused `RunInfo.command: list[str]` field (currently always `[]` in `current_run_info()`) real content for the sweep case; the non-sweep path is left exactly as-is (still `command=[]`) since populating it there is out of scope for this design.

### 3. Changes to `cluv/cli/submit.py` — activate the packing stub

- `get_sbatch_command()` (`cluv/cli/submit.py:472-479`): add params `in_job_packing: bool = False, extra_env: dict[str, str] | None = None`. Delete the dead lines at `512-513`; use the `in_job_packing` param instead. The output-path branch at `523-527` (`%j_%t` naming) becomes reachable as-is — `%j`/`%t` are per-job/per-task-within-that-job placeholders, which is exactly right since each sweep job's own stdout/output files only need to be unique *within that job*.
- New warning next to the existing `#SBATCH --output` conflict warning (`528-546`): when `in_job_packing`, scan the job script for `#SBATCH` lines mentioning `--ntasks`/`--gpus`/`--ntasks-per-gpu`/`--nodes` and warn that `cluv sweep`'s computed values for that job override them.
- After building `env_vars` (`500-501`), add `env_vars.update(extra_env or {})` so `CLUV_SWEEP_NAME`/`CLUV_SWEEP_TASK_OFFSET` flow into the `bash --login -c '<env> sbatch ...'` command (`548-555`) — same mechanism `GIT_COMMIT` already uses.
- `submit()` (`84-92`) and `sbatch()` (`560-567`): add the same two params, thread through to their `get_sbatch_command()` calls. `submit_first()` and `Job` are **not** touched — `cluv sweep first` is out of scope (raise `NotImplementedError` in `cluv/cli/sweep.py` if `cluster == "first"`).

### 4. New file `cluv/cli/sweep.py` — CLI orchestration, multi-job fan-out

```python
_NTASKS_RE = re.compile(r"^--ntasks=(\d+)$")
_NTASKS_PER_GPU_RE = re.compile(r"^--ntasks-per-gpu=(\d+)$")
_GPU_FLAG_RE = re.compile(r"^--(gres|gpus|gpus-per-task|gpus-per-node)=(?:gpu:)?(?:[\w.-]+:)?(\d+)$")

def compute_job_capacity(sbatch_args: list[str]) -> int:
    """Task-slot capacity of ONE job, from whatever sizing flags the user put in
    sbatch_args (last match wins, same convention as chunking.py's time parsing):
    - explicit `--ntasks=N` -> N
    - `--ntasks-per-gpu=K` (+ optionally a GPU count parsed from --gres=gpu:.../--gpus=...,
      defaulting to 1 GPU if a GPU flag is present without an explicit count, or if no GPU
      flag is present at all) -> K * gpu_count
    - neither present -> 1 (no packing; one job per combo, i.e. today's default behavior)
    """

def default_sweep_name(job_script: Path) -> str:
    return job_script.stem   # e.g. scripts/job.sh -> "job" (deterministic re-run default)

async def sweep(
    cluster: str,
    job_script: Path | None,
    name: str | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool = False,
    max_concurrent_submissions: int = 8,   # throttle, see cluv.utils.batched() precedent in cluv_launcher.py
) -> list[Job]:
    if cluster == "first":
        raise NotImplementedError("`cluv sweep` does not support cluster='first' yet.")

    combos = expand_sweep_args(program_args)
    n_combos = len(combos)

    resolved_job_script = job_script or get_job_script_path_from_config(cluster)
    resolved_job_script = _check_job_script_exists_locally(resolved_job_script, cluster)
    sweep_name = name or default_sweep_name(resolved_job_script)

    job_capacity = compute_job_capacity(sbatch_args)   # Phase 2 overrides this whole block, see below
    num_jobs = math.ceil(n_combos / job_capacity)

    console.log(
        f"[cluv sweep {sweep_name!r}] {n_combos} combo(s), {job_capacity} per job "
        f"-> submitting {num_jobs} job(s)."
    )

    # Sync once up front (same pattern already used by hydra_plugins/cluv/cluv_launcher.py's
    # run_sweep(), which syncs once then calls submit(..., _skip_sync=True) per job).
    if cluster != current_cluster():
        await sync(clusters=[cluster])

    async def _submit_one(job_index: int) -> Job | None:
        offset = job_index * job_capacity
        return await submit(
            cluster=cluster,
            job_script=resolved_job_script,
            sbatch_args=sbatch_args,
            program_args=program_args,
            autocommit=autocommit,
            chunking=False,
            in_job_packing=True,
            extra_env={
                CLUV_SWEEP_NAME_ENV_VAR: sweep_name,
                CLUV_SWEEP_TASK_OFFSET_ENV_VAR: str(offset),
            },
            _skip_sync=True,
        )

    jobs = await run_with_concurrency_limit(  # or cluv.utils.batched()-based loop; see cluv_launcher.py precedent
        [functools.partial(_submit_one, i) for i in range(num_jobs)],
        max_concurrent=max_concurrent_submissions,
    )
    return [j for j in jobs if j is not None]
```

Notes:
- `compute_job_capacity` defaulting to `1` when the user gives no sizing flags at all means Phase 1 degrades gracefully to "one job per combo" (today's simplest possible behavior) with zero required new flags — packing is opt-in via `--ntasks-per-gpu`.
- Every job gets **identical** `sbatch_args`/`job_script`/`program_args` — only `extra_env`'s offset differs. This is what keeps the mechanism simple: no per-job argument rewriting, just one integer.
- The last job will generally have some idle task slots when `n_combos` isn't a multiple of `job_capacity` (e.g. 10 combos at capacity 4 → 3 jobs, last one only needs 2 of its 4 slots) — those idle-exit cleanly via `patch_argv()`'s existing out-of-range branch. This is an expected, documented remainder, not a bug.
- `ensure_clean_git_state()` (called inside every `submit()`) runs once per job, but after the first run the tree is already clean/committed, so subsequent calls just re-verify and reuse the same commit hash — cheap, and this is the same repeated-call pattern `cluv_launcher.py`'s `run_sweep()` already relies on.
- Concurrency throttling (`max_concurrent_submissions`) reuses/adapts the `cluv.utils.batched()` pattern already used in `hydra_plugins/cluv/cluv_launcher.py` (`array_parallelism`) rather than firing all `sbatch` calls at once — implementer should check whether a ready-made `run_tasks_with_progress_bar`/similar helper from `milatools.utils.parallel_progress` (already used in `cluv/cli/sync.py`) fits better than hand-rolling a batched-gather.

### 5. Changes to `cluv/__main__.py`

- Generalize the `"submit"`-only argv-surgery at `cluv/__main__.py:44-53` to also handle `"sweep"` (loop over both subcommand names, first match wins).
- Register `add_sweep_args()` in `main()` next to `submit_parser` (`88-89`).
- New `add_sweep_args(subparsers)`, modeled on `add_submit_args` (`153-194`): `cluster`, `job_script` (optional positional), `--name` (optional, default `None` → `default_sweep_name`), `--autocommit`, `--max-concurrent-submissions` (optional int, default 8), `sbatch_args` via `argparse.REMAINDER`. **No `--vram` in Phase 1.**
- Generalize the post-parse repair block (`110-128`) to `subcommand in ("submit", "sweep")` for the job-script-vs-flag rectification and `program_args` assignment. Add a small value-flag recovery helper (new — `--name` takes a value, unlike the existing bare-flag loop which only handles `store_true` flags):
  ```python
  def _pop_value_flag(args: list[str], flag: str) -> tuple[str | None, list[str]]:
      """If `--{flag} value` or `--{flag}=value` is present, remove it and return
      (value, remaining_args); else (None, args) unchanged."""
  ```
  ```python
  if subcommand == "sweep" and args_dict.get("name") is None:
      name_val, args_dict["sbatch_args"] = _pop_value_flag(args_dict["sbatch_args"], "name")
      args_dict["name"] = name_val
  ```
  (Mirrors the exact rationale already documented at `cluv/__main__.py:119-121` for why `REMAINDER`-swallowed flags need this kind of post-hoc recovery.)
- Dispatch needs no change — `inspect.iscoroutinefunction()` (line 136) already handles async `sweep()` like async `submit()`.

### 6. Tests (Phase 1)

- **`tests/test_sweep.py`** (new, pure, no I/O): `expand_sweep_args` (passthrough, one/two swept flags, cartesian order, non-`=`/positional tokens ignored); `_slugify_combo` (determinism, sanitization, stable truncation); `patch_argv()` via `monkeypatch` on env + `sys.argv`, covering both no-op cases, a normal patch, **and the offset arithmetic specifically** (e.g. `SLURM_PROCID=2` + `CLUV_SWEEP_TASK_OFFSET=4` picks combo index 6), plus the out-of-range `SystemExit(0)` case computed against the *global* index; `_current_sweep_context()` returning `None` before `patch_argv()` runs / after a no-op / after an idle-exit, and returning `(sweep_name, slug, combo)` after a normal patch.
- **`tests/test_job.py`** (new — `current_run_info()`/`RunInfo`/`get_run_id()` have no dedicated test file today; add one here rather than growing `test_sweep.py` into job.py's territory): the sweep branch of `current_run_info()`, driven by `monkeypatch` calling `cluv.sweep.patch_argv()` first — asserts `run_id`/`results_path`/`command` for a resolved combo, **and** that the same combo yields the same `run_id`/`results_path` under two different offset/procid pairs that resolve to the same global index (simulating a resubmission where a combo lands on a different job/task). Also lock in today's non-sweep behavior (packing/chunking/plain-job `run_id` shapes) as regression coverage, since none exists yet.
- **`tests/test_submit.py`**: extend `get_sbatch_command()` tests with `in_job_packing=True` (assert `%j_%t` output path, `extra_env` present in the generated command string) and the new job-script-header conflict warning.
- **`tests/test_sweep_cli.py`** (new): `compute_job_capacity` (no flags → 1; `--ntasks=N` → N; `--ntasks-per-gpu=K` alone → K; `--ntasks-per-gpu=K` + `--gres=gpu:h100:3` → 3K); `default_sweep_name`; an end-to-end `sweep(...)` test with `submit` monkeypatched to a recording stub, asserting: the right **number** of `submit()` calls for a few `(n_combos, job_capacity)` cases, each call's `extra_env` offset (`0, job_capacity, 2*job_capacity, ...`), and that `sbatch_args`/`program_args` are identical across all calls. `sweep(cluster="first", ...)` raises `NotImplementedError`.
- Extend wherever `__main__.py`'s submit argv-surgery is already tested with a `cluv sweep <cluster> job.sh --name x --ntasks-per-gpu=2 --gres=gpu:h100:1 -- python main.py --foo=1,2` case, asserting the right `args_dict`.

### 7. Docs (Phase 1)

Add `docs/sweep.md` (mirrors `docs/hydra-launcher.md`'s structure): the `--flag=v1,v2` expansion rule, how one job's capacity is derived from `--ntasks`/`--ntasks-per-gpu`, **the one-job-per-GPU submission model and why** (small jobs schedule more easily; a failed GPU only costs one job), the `patch_argv()` → `current_run_info()` call-order requirement (the same `current_run_info()` already used by `cluv submit` scripts, e.g. `examples/pytorch-example/main.py`), and a full worked example. Extend `docs/commands.md` with a `## cluv sweep` section (same format as `## cluv submit`). Register `docs/sweep.md` in `mkdocs.yaml`'s `nav:`. Add `docs/reference/cli/sweep.md` (`::: cluv.cli.sweep`) and `docs/reference/sweep.md` (`::: cluv.sweep`).

---

## Phase 2 — `--vram`-driven automatic sizing (additive)

Nothing here changes `cluv/sweep.py` or `cluv/job.py`'s sweep branch (the runtime modules) — `patch_argv`/`_current_sweep_context`/`expand_sweep_args`/`_slugify_combo`/the offset mechanism/`current_run_info()`'s sweep path are all reused byte-for-byte. Phase 2 only changes how `cluv/cli/sweep.py` decides **one job's** capacity and sbatch args before replicating it, plus new supporting modules/config for GPU VRAM/inventory lookup.

### 1. New file `cluv/gpu_info.py`

```python
GPU_VRAM_GB: dict[str, int] = {  # hardcoded, easy to PR against
    "h100": 80, "a100": 40, "a100l": 80, "l40s": 48,
    "rtx8000": 48, "v100": 16, "a6000": 48, "p100": 16,
}
GPU_INVENTORY_MAX_AGE = timedelta(days=30)

def get_gpu_vram_gb(gpu_type: str) -> int:
    """Look up get_cluv_config().gpu_vram_gb first (override always wins), then
    GPU_VRAM_GB. Raise ValueError with the exact `[tool.cluv] gpu_vram_gb = {...}` TOML
    snippet to add if unknown."""

async def get_gpus_per_node(cluster: str, gpu_type: str, *, force_refresh: bool = False) -> int:
    """Reads/refreshes the cached per-cluster GPU inventory (below) and returns the GPU
    count per node for gpu_type; raises ValueError listing available types if missing."""
```

**GPU inventory SSH fetch** (reuses existing primitives — no new SSH plumbing): move the existing `SINFO_LIST_GPUS` constant from `cluv/cli/status.py:99` into `cluv/slurm.py` (small non-breaking refactor; `status.py` imports it from there). Execution mirrors `cluv/cli/status.py:229-233`'s local/remote branching, wrapped in `bash --login -c '<script>'` exactly as `submit.py:552-554` does. New pure parser in `cluv/slurm.py`, next to `parse_sinfo_nodes` (reusing its `_GRES_RE`/`_normalize_gpu_model` helpers) but keeping **per-node** granularity:
```python
@dataclass(frozen=True)
class GpuNodeInfo:
    gpus_per_node: int
    node_count: int
    total_gpus: int

def parse_sinfo_gpus_per_node(output: str) -> dict[str, GpuNodeInfo]:
    """gpus_per_node = max(per-node GRES count for that model); log a warning if
    heterogeneous across nodes."""
```
**Cache**: extend `CacheContent` (`cluv/cache.py:63-66`) with `gpu_inventory: dict[str, GpuInventoryCache]` (`GpuInventoryCache = {fetched_at: datetime, gpus: dict[str, GpuNodeInfo]}`), reusing the existing `read_cache()`/`write_cache()` YAML mechanism. Staleness: refresh if missing, older than `GPU_INVENTORY_MAX_AGE` (30 days), or `force_refresh=True` (exposed as `cluv sweep --refresh-gpu-inventory`).

### 2. Changes to `cluv/config.py`

- `PartialClusterConfig`/`ClusterConfig`: add `full_node_allocations: bool = False` ("whether jobs on this cluster must request whole nodes"), threaded through `get_cluster_config()` as a plain per-cluster bool.
- `CluvConfig`: add `gpu_vram_gb: dict[str, int] = {}` (global override/addition table for `cluv.gpu_info.GPU_VRAM_GB`).

### 3. Changes to `cluv/cli/sweep.py` — one job's capacity/sbatch-args, `--vram`-driven

`sweep()` gains `vram: float | None = None`. When `vram is None`, behavior is exactly Phase 1 (`compute_job_capacity(sbatch_args)` as already written). When `vram` is given, **this replaces `compute_job_capacity` and also rewrites `sbatch_args` before the fan-out loop** — the fan-out loop itself (`_submit_one`, the offset math, `num_jobs = ceil(n_combos / job_capacity)`) is unchanged:

```python
_GPU_FLAG_RE = re.compile(r"^--(gres|gpus|gpus-per-task|gpus-per-node)=(.+)$")

def parse_gpu_type(sbatch_args: list[str]) -> tuple[str, str]:
    """Scans sbatch_args (last match wins) for --gres=gpu:<type> or
    --gpus[-per-task|-per-node]=<type>. Returns (flag_name, gpu_type). Raises ValueError if:
    - no such flag is present at all, or
    - the flag's value is purely numeric (a count without a type, e.g. `--gpus=1`)."""

async def _compute_vram_job_shape(
    cluster: str, sbatch_args: list[str], vram: float, refresh_gpu_inventory: bool,
) -> tuple[int, list[str]]:
    """Returns (job_capacity, sbatch_args_for_one_job). Strips any user-given GPU-count
    flag first (the count is now derived, not user-chosen)."""
    flag_name, gpu_type = parse_gpu_type(sbatch_args)
    gpu_vram_gb = get_gpu_vram_gb(gpu_type)
    ntasks_per_gpu = int(gpu_vram_gb // vram)
    if ntasks_per_gpu < 1:
        raise ValueError(f"--vram={vram}GB doesn't fit on a single {gpu_type} GPU ({gpu_vram_gb}GB VRAM).")

    stripped = [a for a in sbatch_args if not _GPU_FLAG_RE.match(a)]
    cluster_config = get_cluv_config().get_cluster_config(cluster)

    if cluster_config.full_node_allocations:
        gpus_per_node = await get_gpus_per_node(cluster, gpu_type, force_refresh=refresh_gpu_inventory)
        job_capacity = gpus_per_node * ntasks_per_gpu
        extra_args = [f"--nodes=1", f"--gres=gpu:{gpu_type}:{gpus_per_node}"]
    else:
        job_capacity = ntasks_per_gpu   # exactly one GPU per job
        extra_args = [f"--gres=gpu:{gpu_type}:1"]

    return job_capacity, stripped + extra_args + [
        f"--ntasks={job_capacity}", f"--ntasks-per-gpu={ntasks_per_gpu}",
    ]
```

In `sweep()`:
```python
if vram is not None:
    job_capacity, sbatch_args = await _compute_vram_job_shape(cluster, sbatch_args, vram, refresh_gpu_inventory)
else:
    job_capacity = compute_job_capacity(sbatch_args)
num_jobs = math.ceil(n_combos / job_capacity)
# ... unchanged fan-out loop, logging now also states e.g.
# "packing 10/h100 GPU -> 1 job per GPU, 1 GPU/job -> 4 job(s)" (partial-node) or
# "packing 10/h100 GPU, 4 GPUs/node -> 1 job per node -> 1 job(s)" (full-node)
```
This is exactly what makes "one job per GPU" vs. "one job per node" fall out naturally: `job_capacity` is either "one GPU's worth" or "one node's worth" of packed tasks, and the *same* generic `ceil(n_combos / job_capacity)`-many-identical-jobs loop from Phase 1 handles both without knowing which regime it's in.

Also add `--refresh-gpu-inventory` (bool flag) threaded to `sweep(..., refresh_gpu_inventory: bool = False)`.

**Implementation-time validation flag**: confirm against a real Slurm scheduler that `sbatch --gres=gpu:<type>:<n> --ntasks=<n> --ntasks-per-gpu=<k>` (single-GPU-per-job case, n=k) and the analogous `--nodes=1 --gres=gpu:<type>:<gpus_per_node> --ntasks=<gpus_per_node*k> --ntasks-per-gpu=<k>` (full-node case) are both accepted and bind tasks to GPUs as expected — this is exactly the scenario the pre-existing dead code in `submit.py` was written for but never exercised.

### 4. Changes to `cluv/__main__.py` (Phase 2 delta)

- `add_sweep_args()` gains `--vram` (float, default `None`) and `--refresh-gpu-inventory` (flag).
- Extend the Phase 1 value-flag recovery block to also pop `--vram` the same way `--name` is recovered.

### 5. Tests (Phase 2 delta)

- **`tests/test_slurm.py`**: `parse_sinfo_gpus_per_node` cases (reuse existing fixture strings), including a heterogeneous-count case.
- **New `tests/test_gpu_info.py`**: `get_gpu_vram_gb` (builtin hit, config-override hit, actionable unknown-type error); inventory staleness logic with `monkeypatch` on `cluv.cache.read_cache`/`write_cache` and a faked remote command.
- **Extend `tests/test_config.py`**: `full_node_allocations` default/override/propagation; `gpu_vram_gb` round-trip.
- **Extend `tests/test_cache.py`**: `GpuInventoryCache`/`GpuNodeInfo` round-trip + staleness-boundary test.
- **Extend `tests/test_sweep_cli.py`**: `parse_gpu_type` (all flag spellings; rejects count-without-type; rejects missing GPU flag); `_compute_vram_job_shape`/`sweep(vram=...)` for both `full_node_allocations=False` (→ `job_capacity=ntasks_per_gpu`, 1 GPU/job) and `=True` (→ `job_capacity=gpus_per_node*ntasks_per_gpu`, 1 node/job) fixture clusters, asserting the right `num_jobs` and per-job `sbatch_args`.

### 6. Docs (Phase 2)

Extend `docs/sweep.md` with the `--vram` section: the GPU-type-must-be-explicit requirement, the `[tool.cluv] gpu_vram_gb` override table, the `full_node_allocations` config flag and how it flips the job granularity from "one GPU" to "one node," and `--refresh-gpu-inventory`.

---

## Verification

- `uv run pytest` after each phase — all new/extended tests above should pass, plus the full existing suite (esp. `tests/test_submit.py`, `tests/test_config.py`) should still pass unchanged given the added-but-defaulted params/fields.
- Manual end-to-end check (needs real cluster access): a tiny throwaway script that calls `cluv.sweep.patch_argv()` then prints `sys.argv` and `cluv.job.current_run_info()`. Run e.g. `cluv sweep mila scripts/job.sh --name smoke-test --ntasks-per-gpu=2 --gres=gpu:rtx8000:1 -- python -c "..." --x=1,2,3,4,5` (5 combos, capacity 2 → 3 jobs, last job idle-exits one slot). Confirm: each job's two tasks print different `--x=` values consistent with `offset + SLURM_PROCID`, all 5 combos are covered exactly once across the 3 jobs, and `results_path` is stable and distinct per combo. Resubmit with the same `--name` and confirm the same `results_path` values reappear regardless of which job/task a given combo lands on this time. For Phase 2, repeat with `--vram` instead of manual `--ntasks*`/`--gres`, on a cluster with a known GPU type/VRAM (and separately on a `full_node_allocations` cluster), and confirm the computed `job_capacity`/`--nodes`/number of jobs match hand-computed expectations.
