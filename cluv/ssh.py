from pathlib import Path
import paramiko

SSH_CONFIG_PATH = Path.home() / ".ssh" / "config"

def get_ssh_hostnames() -> set[str]:
    if not SSH_CONFIG_PATH.exists():
        return set()
    ssh_config = paramiko.SSHConfig.from_path(SSH_CONFIG_PATH)

    return ssh_config.get_hostnames()
