from pathlib import Path
import paramiko

SSH_CONFIG_PATH = Path.home() / ".ssh" / "config"

def get_ssh_hostnames() -> set[str]:
    if not SSH_CONFIG_PATH.exists():
        return set()
    ssh_config = paramiko.SSHConfig()

    with SSH_CONFIG_PATH.open() as f:
        ssh_config.parse(f)

    return ssh_config.get_hostnames()
