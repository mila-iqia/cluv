import os
import subprocess
from pathlib import Path

from cluv.config import find_pyproject, has_cluv_config
from cluv.utils import console

# TODO : not a great import... But it can be helpful instead of updating a const
from milatools.cli.init_command import DRAC_CLUSTERS    

JOB_SCRIPT_PATH = "scripts/job.sh"
RESULTS_DIR_PATH = "logs"

MILA_CLUSTER_DEFAULT_CONFIG = [
    "UV_OFFLINE = 0",
    'WANDB_MODE = "online"',
]

SLURM_DEFAULT_CONFIG = [
    "UV_OFFLINE = 1",
    'WANDB_MODE = "offline"',
]

def init():
    """Initialize the current project across clusters.
    Akin to `uv init`, sets up the current project on all configured clusters.
    """
    console.print()
    console.rule("[bold cyan]cluv init[/bold cyan]")
    console.print()
    # TODO : give path to create project

    # Try to run "uv run" to create a new project
    console.print("Running [bold]uv init[/bold]...")
    console.log("uv init --package --build-backend hatch --python 3.13")
    uv_init = subprocess.run(["uv", "init", "--package", "--build-backend", "hatch", "--python", "3.13"], capture_output=True, text=True)

    # An expected error is that uv fails if a pyproject.toml file already exists
    if uv_init.returncode == 2:
        # If the error isn't about the pyproject.toml, raise an exception
        if not uv_init.stderr.endswith("(`pyproject.toml` file exists)\n"):
            raise RuntimeError("Error occurred while initializing uv project: ", uv_init.stderr)
        console.print("[green]✅ uv: a project already exists (see pyproject.toml file). Skipping initialization.[/green]")
        check_git()
    else:
        console.print("[green]✅ uv: project initialized.[/green]")

    # Read the pyproject.toml file and try to find a cluv config.
    # If it doesn't exist, add a cluv config section with the default clusters and settings.
    console.print()
    console.print("Reading pyproject.toml...")
    pyproject_path = find_pyproject()

    if has_cluv_config(pyproject_path):
        console.print("[green]✅ Project already have a cluv config in pyproject.toml.[/green]")
    else:
        console.print("No config found for [bold]cluv[/bold] in the pyproject.toml file. Adding config...")
        add_tool_config(pyproject_path)
        add_slurm_config(pyproject_path)
        add_cluv_cluster_config("mila", pyproject_path, MILA_CLUSTER_DEFAULT_CONFIG)
        for cluster in DRAC_CLUSTERS: add_cluv_cluster_config(cluster, pyproject_path)
        console.print("[green]✅ Pyproject config completed.[/green]")

    # Check if project structure is correct
    console.print()
    console.print("Validating project structure...")
    if not os.path.exists(JOB_SCRIPT_PATH):
        os.makedirs("scripts")
        console.print(f"Adding job template script at '{JOB_SCRIPT_PATH}'.")
        generate_job_script(pyproject_path, RESULTS_DIR_PATH)   # TODO : split the pyproject path and don't keep the "pyproject.toml" part
    else:
        console.print(f"[green]✅ Job template script already exists at '{JOB_SCRIPT_PATH}'.[/green]")

    if not os.path.exists(RESULTS_DIR_PATH):
        console.print(f"Adding results directory at '{RESULTS_DIR_PATH}'.")
        os.makedirs(RESULTS_DIR_PATH)
    else:
        console.print(f"[green]✅ Results directory already exists at '{RESULTS_DIR_PATH}'.[/green]")

    console.print()
    console.print(":tada: Your cluv config is ready to go !")


    # TODO : synclink for checkpoints ?

    # After the project setup, show what the user can do next
    console.print()
    console.print("Next steps :")
    console.print("=> [bold] cluv login [/bold] : open a SSH connections to all configured clusters.")
    console.print("=> [bold] cluv sync [/bold]  : synchronize the project on all configured clusters.")
    console.print()


def add_tool_config(pyproject_path: Path) -> None:
    """
    TODO
    """
    # TODO : add info about the results dir
    section_lines = ["\n[tool.cluv]"]

    with pyproject_path.open("a") as f:
        f.write("\n".join(section_lines) + "\n")


def add_slurm_config(pyproject_path: Path) -> None:
    """
    TODO
    """
    console.print(f"Adding config for global env vars :")
    section_lines = ["\n[tool.cluv.slurm]", "# Environment variables applied when using Slurm commands on all clusters."] + SLURM_DEFAULT_CONFIG
    console.log(f'{"\n".join(section_lines) + "\n"}'.replace("[", "\\["))    # TODO : Log not displayed correctly ?
    with pyproject_path.open("a") as f:
        f.write("\n".join(section_lines) + "\n")


def add_cluv_cluster_config(cluster: str, pyproject_path: Path, vars: list[str] = []) -> None:
    """
    TODO
    """
    console.print(f"Adding config for cluster [bold]{cluster}[/bold] :")
    section_lines = [f"\n[tool.cluv.clusters.{cluster}]"] + vars
    console.log(f'{"\n".join(section_lines) + "\n"}'.replace("[", "\\["))    # TODO : Log not displayed correctly ?
    with pyproject_path.open("a") as f:
        f.write("\n".join(section_lines) + "\n")


def check_git() -> bool:
    """
    TODO
    """
    if os.path.isdir(".git"):   # TODO : Very simple approch. Only works at the project root.
        console.print("[green]✅ git: project is in git repository.[/green]")
    else:
        console.print("[red]❌ git: no git repository found.[/red]")
        raise RuntimeError("The current project is not a git repository. Try running 'git init' or clone a GitHub project.")


def generate_job_script(project_root: Path, results_dir: str) -> None:
    """
    TODO
    """
    project_name = project_root.name
    project_root = str(project_root.relative_to(Path.home()))

    script_content = f"""#!/bin/bash
    #SBATCH --output={results_dir}/%j/slurm-%j.out
    #SBATCH --ntasks=1
    #SBATCH --mem=8G
    #SBATCH --time=0:05:00

    project_name="{project_name}"
    results_dir="{results_dir}"
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
    echo "Copying logs from $SLURM_TMPDIR/$project_name/$results_dir to $project_root/$results_dir"
    srun --ntasks-per-node=1 rsync --update --recursive $SLURM_TMPDIR/$project_name/$results_dir $project_root/
    """

    with open('scripts/job.sh', 'w') as sh_file:
        sh_file.write(script_content)
