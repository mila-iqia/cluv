import os
import subprocess
from pathlib import Path

from milatools.cli.init_command import DRAC_CLUSTERS

from cluv.config import find_pyproject, has_cluv_config, load_cluv_config
from cluv.utils import console

JOB_SCRIPT_PATH = "scripts/job.sh"
DEFAULT_RESULTS_PATH = "logs"

CLUV_DEFAUT_CONFIG = [
    "[tool.cluv]",
    f'results_path = "{DEFAULT_RESULTS_PATH}"'
]

CLUV_SLURM_DEFAULT_CONFIG = [
    "[tool.cluv.slurm]",
    "# Environment variables applied when using Slurm commands on all clusters.",
    "UV_OFFLINE = 1",
    'WANDB_MODE = "offline"',
]

CLUV_CLUSTER_MILA_DEFAULT_ARGUMENTS = [
    "UV_OFFLINE = 0",
    'WANDB_MODE = "online"',
]

def init() -> None:
    """
    Initialize a new project for use with cluv.
    """
    console.print()
    console.rule("[bold cyan]cluv init[/bold cyan]")
    console.print()
    # TODO : give path to create project
    
    # 1. Check if the current directory is under the home directory. If not, raise an error and exit.
    if str(Path.cwd()).startswith(str(Path.home())):
        console.print("[green]✅ Current directory is under home directory.[/green]")
    else:
        console.print("[red]❌ cluv init should be run in a directory under your home directory.[/red]")
        raise RuntimeError("cluv init should be run in a directory under your home directory.")

    # 2. Try to run "uv init" to create a new project
    console.print()
    console.print("Initializing uv project: running [bold]uv init[/bold]...")
    console.log("uv init --package --build-backend hatch --python 3.13")
    console.print()
    uv_init = subprocess.run(["uv", "init", "--package", "--build-backend", "hatch", "--python", "3.13"], capture_output=True, text=True)

    ### An expected error is that uv fails if a pyproject.toml file already exists
    if uv_init.returncode == 2:
        if uv_init.stderr.endswith("(`pyproject.toml` file exists)\n"):
            console.print("[green]✅ uv: a project already exists (see pyproject.toml file). Skipping initialization.[/green]")
            check_git()
        else:
            raise RuntimeError("Error occurred while initializing uv project: ", uv_init.stderr)
    else:
        console.print("[green]✅ uv: project initialized.[/green]")

    ### Check if the git repository have access to a remote repository.
    git_remote = subprocess.run(["git", "remote"], capture_output=True, text=True)
    if git_remote.returncode == 0:
        if git_remote.stdout.strip() == "":
            console.print("[yellow]⚠️  Warning: No git remote found. You won't be able to use some features (like syncing or submitting jobs). Consider adding a remote repository to your git config.[/yellow]")
        else:
            console.print(f"[green]✅ Git remote repository found: {git_remote.stdout.strip()}[/green]")

    # 3. Read the pyproject.toml file and try to find a cluv config.
    console.print()
    console.print("Reading pyproject.toml...")
    pyproject_path = find_pyproject()
    results_path = DEFAULT_RESULTS_PATH

    ### If it doesn't exist, add a cluv config section with the default settings and clusters.
    if has_cluv_config(pyproject_path):
        console.print("[green]✅ Project already have a cluv config in pyproject.toml.[/green]")
        config = load_cluv_config(pyproject_path)
        results_path = config.results_path
        console.print(config)
    else:
        console.print("No config found for [bold]cluv[/bold] in the pyproject.toml file. Adding config...")
        console.print("Adding config for cluv tool :")
        add_cluv_config_section(pyproject_path, CLUV_DEFAUT_CONFIG)
        add_cluv_config_section(pyproject_path, CLUV_SLURM_DEFAULT_CONFIG)
        add_cluv_cluster_config("mila", pyproject_path, CLUV_CLUSTER_MILA_DEFAULT_ARGUMENTS)
        for cluster in DRAC_CLUSTERS:
            add_cluv_cluster_config(cluster, pyproject_path)
        console.print("[green]✅ Pyproject config completed.[/green]")

    # 4. Check if project structure is correct
    console.print()
    console.print("Validating project structure...")

    ### Check if the job script exists
    check_job_script(pyproject_path.parent, results_path)
    
    ### Check if the results path is correctly symlinked to scratch
    check_symlink_to_scratch(pyproject_path.parent, results_path)

    console.print()
    console.print(":tada: Your cluv config is ready to go !")

    # 5. Show what the user can do next after the project setup
    console.print()
    console.print("Next steps :")
    console.print("=> [bold] cluv login [/bold] : open a SSH connections to all configured clusters.")
    console.print("=> [bold] cluv sync [/bold]  : synchronize the project on all configured clusters.")
    console.print()


def add_cluv_config_section(pyproject_path: Path, section_lines: list[str]) -> None:
    """
    Write the given lines to the pyproject.toml file.
    """
    console.log(f'{"\n" + "\n".join(section_lines) + "\n"}'.replace("[", "\\["))
    with pyproject_path.open("a") as f:
        f.write("\n" + "\n".join(section_lines) + "\n")


def add_cluv_cluster_config(cluster: str, pyproject_path: Path, vars: list[str] = []) -> None:
    """
    Add a cluster config section for the given cluster to the pyproject.toml file, with the given variables.
    """
    console.print(f"Adding config for cluster [bold]{cluster}[/bold] :")
    section_lines = [f"[tool.cluv.clusters.{cluster}]"] + vars
    console.log(f'{"\n" + "\n".join(section_lines) + "\n"}'.replace("[", "\\["))
    with pyproject_path.open("a") as f:
        f.write("\n" + "\n".join(section_lines) + "\n")


def check_git() -> None:
    """
    Check if the current project is in a git repository. If not, raise an error and exit.
    """
    if os.path.isdir(".git"):   # TODO : Very simple approch. Only works at the project root.
        console.print("[green]✅ Project is in a git repository.[/green]")
    else:
        console.print("[red]❌ No git repository found.[/red]")
        raise RuntimeError("The current project is not a git repository. Try running 'git init' or clone a GitHub project.")


def check_symlink_to_scratch(project_root: Path, results_path: str | None) -> None:
    """
    Check if a symlink from the results_path in the project in $HOME to the corresponding path in $SCRATCH already exists. If not, create it.
    The symlink should be like : $HOME/<project>/<results_path> -> $SCRATCH/<results_path>/<project_name>
    """
    if results_path is None:
        console.print("[yellow]⚠️  Warning: Results path is not configured. Skipping symlink creation.[/yellow]")
        return

    # Generate the expected scratch and symlink path
    scratch_dir = Path(os.path.expandvars(f"$SCRATCH/{results_path}/{project_root.name}"))
    results_dir = project_root / results_path

    if results_dir.is_symlink():
        if results_dir.resolve() == scratch_dir.resolve():
            console.print("[green]✅ Symlink from $HOME results_path to $SCRATCH already exists.[/green]")
            return
        else:
            console.print(f"[red]❌ Symlink from {results_dir} points to {results_dir.resolve()} instead of {scratch_dir}.[/red]")
            raise RuntimeError(f"Symlink from {results_dir} points to {results_dir.resolve()} instead of {scratch_dir}. Please fix this symlink before running cluv.")
    else:
        console.print(f"Creating symlink from {results_dir} to {scratch_dir}")
        scratch_dir.mkdir(parents=True, exist_ok=True)
        results_dir.symlink_to(scratch_dir, target_is_directory=True)


def check_job_script(project_root: Path, results_path: str | None) -> None:
    """
    Check if the job script template to set and run the project on a cluster with SLURM exists. If not, create it.
    """
    if os.path.exists(JOB_SCRIPT_PATH):
        console.print(f"[green]✅ Job template script already exists at '{JOB_SCRIPT_PATH}'.[/green]")
        return

    if results_path is None:
        console.print("[yellow]⚠️  Warning: Results path is not configured. Skipping job template script generation.[/yellow]")
        return
    
    console.print(f"Adding job template script at '{JOB_SCRIPT_PATH}'.")

    project_name = project_root.name
    project_root = str(project_root.relative_to(Path.home()))

    script_content = f"""#!/bin/bash
#SBATCH --output={results_path}/%j/slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --mem=8G
#SBATCH --time=0:05:00

project_name="{project_name}"
results_path="{results_path}"
project_root="{project_root}"
"""

    script_content +=  """
# Minimal test job for cluv submit integration tests.
echo "hostname: $(hostname)"
echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"

# Setup the repo in $SLURM_TMPDIR, so the code can change in the project without affecting the job.
echo "Preparing the repo and virtual environment in $SLURM_TMPDIR"
srun --ntasks-per-node=1 --ntasks=$SLURM_NNODES --input=all bash -e <<END
cd $SLURM_TMPDIR
git clone $project_root
cd $SLURM_TMPDIR/$project_name
git checkout --detach $GIT_COMMIT
exec uv sync
END

# Run the actual job command passed as an argument ('python main.py' for example)
echo "Running command: $@"
# Note: This `--gres-flags=allow-task-sharing` is required to allow tasks on the same node to access
# GPUs allocated to other tasks on that node. Without this flag, --gpus-per-task=1 would isolate
# each task to only see its own GPU, which can cause some mysterious NCCL errors.
srun --gres-flags=allow-task-sharing uv --directory=$SLURM_TMPDIR/$project_name run "$@"

# Copy results (if any) from the local storage back to the results dir (eg in $SCRATCH)
echo "Copying logs from $SLURM_TMPDIR/$project_name/$results_path to $project_root/$results_path"
srun --ntasks-per-node=1 rsync --update --recursive $SLURM_TMPDIR/$project_name/$results_path $project_root/
"""
    os.makedirs(Path(JOB_SCRIPT_PATH).parent, exist_ok=True)
    with open(JOB_SCRIPT_PATH, 'w') as sh_file:
        sh_file.write(script_content)
