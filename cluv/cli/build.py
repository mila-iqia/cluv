"""Build an Apptainer container on a remote cluster.

Generates pinned requirements from uv.lock, uploads an Apptainer definition,
builds a .sif image, and deploys it to the configured path.
"""

from __future__ import annotations

import logging
from pathlib import Path, PurePosixPath

from cluv.cli.login import login
from cluv.cli.sync import sync
from cluv.config import ContainerConfig, find_pyproject, get_cluv_config
from cluv.utils import console

logger = logging.getLogger(__name__)

__all__ = ["build"]


def generate_def(base_image: str, extra_apt: list[str], extra_pip_args: str) -> str:
    post_lines = []
    if extra_apt:
        pkgs = " ".join(extra_apt)
        post_lines.append(
            f"apt-get update && apt-get install -y --no-install-recommends {pkgs} "
            "&& rm -rf /var/lib/apt/lists/*"
        )
    pip_cmd = "pip install --no-cache-dir"
    if extra_pip_args:
        pip_cmd += f" {extra_pip_args}"
    pip_cmd += " -r /build/requirements.txt"
    post_lines.append(pip_cmd)
    post_lines.append("mv /build/requirements.txt /opt/requirements.txt")
    post_lines.append("rm -rf /build")

    post_body = "\n    ".join(post_lines)

    return (
        f"Bootstrap: docker\n"
        f"From: {base_image}\n"
        f"\n"
        f"%files\n"
        f"    /tmp/cluv-build/requirements.txt /build/requirements.txt\n"
        f"\n"
        f"%post\n"
        f"    {post_body}\n"
        f"\n"
        f"%test\n"
        f'    python -c "import importlib.metadata; print(\'container OK\')"\n'
    )


async def build(cluster: str, extra: str | None = None, no_sync: bool = False) -> str | None:
    """Build an Apptainer container on the given cluster.

    Returns the remote path to the built .sif, or None on failure.
    """
    config = get_cluv_config()
    cluster_config = config.clusters.get(cluster)
    if not cluster_config or not cluster_config.container:
        console.print(
            f"[red]No container config for cluster '{cluster}'.[/red]\n"
            f"Add [tool.cluv.clusters.{cluster}.container] to pyproject.toml."
        )
        return None

    container: ContainerConfig = cluster_config.container

    if not no_sync:
        remotes = await sync(clusters=[cluster])
    else:
        remotes = await login([cluster])

    remote = remotes[0]
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))

    console.print("[bold]Exporting pinned requirements from uv.lock...[/bold]")
    export_parts = [
        "uv export --locked --no-dev --no-hashes --no-annotate --no-header --no-emit-project",
    ]
    if extra:
        export_parts.append(f"--extra {extra}")
    export_parts.append("--format requirements-txt")
    export_cmd = f"bash -l -c 'cd ~/{project_path} && {' '.join(export_parts)}'"
    result = await remote.run(export_cmd, display=True, hide="out")
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "locked" in stderr.lower() or "lock" in stderr.lower():
            console.print(
                "[red]uv.lock is out of sync with pyproject.toml. "
                "Run 'uv lock' locally, commit, and try again.[/red]"
            )
        else:
            console.print(f"[red]uv export failed: {stderr}[/red]")
        return None
    requirements = result.stdout

    console.print("[bold]Uploading build context...[/bold]")
    await remote.run("mkdir -p /tmp/cluv-build", hide=True)
    await remote.run(
        "cat > /tmp/cluv-build/requirements.txt",
        input=requirements,
        hide=True,
    )

    def_content = generate_def(container.base_image, container.extra_apt, container.extra_pip_args)
    await remote.run("cat > /tmp/cluv-build/container.def", input=def_content, hide=True)

    git_sha = await remote.get_output(
        f"git -C ~/{project_path} rev-parse --short HEAD",
    )
    project_name = find_pyproject().parent.name
    sif_name = f"{project_name}-{git_sha}.sif"
    deploy_path = container.deploy_path

    console.print("[bold]Building container (this may take several minutes)...[/bold]")

    # GOMAXPROCS=1 prevents pids.max cgroup kills on DRAC login nodes.
    # Their user.slice cgroup has pids.max=512; Go's default thread-per-CPU
    # overshoots during OCI fetch, killing the build with EAGAIN.
    build_cmd = (
        f"bash -l -c '"
        f"export GOMAXPROCS=${{GOMAXPROCS:-1}} GOMEMLIMIT=${{GOMEMLIMIT:-2GiB}}; "
        f"module load apptainer 2>/dev/null || true; "
        f"cd /tmp/cluv-build && "
        f"apptainer build {sif_name} container.def"
        f"'"
    )
    result = await remote.run(build_cmd, display=True, hide=False)
    if result.returncode != 0:
        console.print("[red]Container build failed.[/red]")
        await _cleanup_build_dir(remote)
        return None

    # Verify the image loads before deploying.
    console.print("[bold]Verifying container...[/bold]")
    verify_script = "import sys; sys.exit(0)"
    verify_cmd = (
        f"bash -l -c '"
        f"module load apptainer 2>/dev/null || true; "
        f"apptainer exec /tmp/cluv-build/{sif_name} python -c \"{verify_script}\"'"
    )
    result = await remote.run(verify_cmd, display=True, hide="out")
    if result.returncode != 0:
        console.print("[red]Container verification failed.[/red]")
        await _cleanup_build_dir(remote)
        return None

    console.print(f"[bold]Deploying to {deploy_path}...[/bold]")
    deploy_cmd = (
        f"bash -l -c '"
        f"mkdir -p {deploy_path} && "
        f"cp /tmp/cluv-build/{sif_name} {deploy_path}/{sif_name} && "
        f"chmod 640 {deploy_path}/{sif_name} && "
        f"ln -sfn {sif_name} {deploy_path}/current.sif"
        f"'"
    )
    result = await remote.run(deploy_cmd, display=True, hide=True)
    if result.returncode != 0:
        console.print("[red]Deploy failed.[/red]")
        await _cleanup_build_dir(remote)
        return None

    await _cleanup_build_dir(remote)

    sif_path = f"{deploy_path}/{sif_name}"
    console.print(f"[green]Container deployed: {sif_path}[/green]")
    console.print(f"[green]Symlink: {deploy_path}/current.sif -> {sif_name}[/green]")
    return sif_path


async def _cleanup_build_dir(remote) -> None:
    await remote.run("rm -rf /tmp/cluv-build", warn=True, hide=True, display=False)
