# CLUV

Name inspired by "Cluster" + "UV" and from  `clush` (clustershell)

> WIP: This isn't even fully designed yet. Nothing is implemented, there are only stubs.

## Project Roadmap

- [ ] Brainstorming phase
  - [x] Pitch the idea for this project to some potential users / devs.
  - [ ] Gather ideas / feedback in a structured way.
- [ ] Design phase
  - [ ] Triage the ideas and create an initial, minimal set of commands / features.
  - [ ] Give an estimate of the time required to implement each command.
  - [ ] Write some stubs for tests for the commands, to help refine the design
  - [ ] Design a proof-of-concept solution for each command.
- [ ] Implementation phase


## Assumptions
- You have a project managed with UV on the **Mila** cluster
	- Could also work later with a local project and using the mila cluster as the "main" cluster, but let's start with this simplifying assumption for now. 
- Want to sync code and dispatch jobs to other clusters (DRAC / PAICE for now) where the user already has access with SSH.
- Want to gather results from other clusters to the Mila cluster.
- Project has a GitHub repo.
- Same structure everywhere:
	- Project in `$HOME/project_name`
	- Checkpoints somewhere in `$SCRATCH`, with a symlink in the project folder at `$HOME/project_name/logs.
- Wandb offline mode used on all the non-mila clusters.

## Proposed Commands

## `cluv init`
Akin to `uv init`, sets up the current project on all configured clusters.
- Prompts to configure which clusters to use (config stored in pyproject.toml of the project)
- Installs UV on all clusters
	- Sets up the DRAC-specific uv.toml files to use the DRAC wheelhouse when necessary.
- Sets up the project repo, creates the $SCRATCH checkpoint folder
#### How it could work (proof-of-concept)
- Over SSH. Setup UV.
- Assume GitHub. Clone the project on each cluster. Configure the git credential-cache if necessary.

## `cluv sync [--cluster=all|<cluster>]`

- Synchronizes code across all clusters. Gathers results on the "main" cluster (mila)
- Does `uv sync` that cluster as well
	- (Important so that jobs can be run in OFFLINE mode)

#### How it could work (proof-of-concept)
- Checks git state
- Push to github
	- TODO: Check syncing without github.
- Over SSH, does a git fetch on all remote clusters
- Gathers results from all other clusters to the Mila cluster using rsync.

## `cluv run [--cluster=cluster] <command>`

Similar in spirit to `uv run`, but runs a command in the synced project on a potentially remote cluster.
- Idea is that this could maybe be a building block for other commands.

## `cluv status`

- Gives you an overview of the state of each cluster, and displays an overview of the state of your jobs across the clusters.
- Displays the number of idle nodes, or the number of idle GPUs, or something similar, for each cluster


## `cluv launch [--cluster=cluster] <job_script.sh>`

- Launch this job script on the given cluster.

To manage different clusters having different SLURM accounts / gpus / etc:
	- **Simplest**: Launches the job script as-is. User has to have different job scripts for each cluster.
	- Stretch goal: `cluv` applies some templating to the job script based on configuration in the `pyproject.toml`

#### How it could work (proof-of-concept)
- Does `cluv sync --cluster=<cluster>`
- Uses something like the `safe_sbatch` script / command instead of `sbatch` , and the job script uses the `code_checkpointing.sh` script to 
#### Stretch goal: `--cluster=auto`
Stretch goal: "auto" will use the "best" cluster for the job.
- the least utilized cluster, or the cluster with fewest jobs in the queue
- `sbatch --test-only` estimated start-time (if reliable)
- (advanced), Based on job resource requirements, find the best cluster for it.


## `cluv dashboard`

Terminal UI showing jobs in a table, for each cluster:
- Job ID
- Job Name
- Job State
- Job Nodes
- Job Resources
- Job command
- Wandb URL


## Questions / commentaires

- attention de pas briser les jobs qui roulent déjà quand on fait `cluv sync`

- Hierarchie de cache avec UV?

- Pourrait aider à inciter les gens à utilier différentes grappes de calcul
	- Ce serait dommage qu'il faille utiliser un job script différent par cluster.

- Launch,  interface terminal interactive pour choisir les clusters, les ressources, etc.
- 





