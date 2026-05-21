from pathlib import Path
import paramiko



def get_ssh_hostnames() -> set[str]:
    # Resolve at call time so test fixtures can monkeypatch `Path.home`.
    ssh_config_path = Path.home() / ".ssh" / "config"
    if not ssh_config_path.exists():
        return set()
    ssh_config = paramiko.SSHConfig.from_path(ssh_config_path)
    try:
        return ssh_config.get_hostnames()
    except KeyError:
        # paramiko<=4.0 raises KeyError on `Match host X exec ...` stanzas
        # because the parsed entry has no `host` key. Fall back to a scan
        # that simply skips those entries.
        return {h for entry in ssh_config._config for h in entry.get("host", ())}
