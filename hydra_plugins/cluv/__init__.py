from hydra.core.config_store import ConfigStore

from .cluv_launcher import (
    CluvLauncher,
    CluvLauncherConfig,
)

ConfigStore.instance().store(
    group="hydra/launcher",
    name="cluv_launcher",
    node=CluvLauncherConfig,
    provider="Mila",
)


__all__ = [
    "CluvLauncher",
    "CluvLauncherConfig",
]
