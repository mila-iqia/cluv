from hydra.core.config_store import ConfigStore

from .cluv_launcher import (
    CluvLauncher,
    CluvLauncherConfig,
)
from .cluv_sweeper import (
    CluvSweeper,
    CluvSweeperConfig,
)

ConfigStore.instance().store(
    group="hydra/launcher",
    name="cluv_launcher",
    node=CluvLauncherConfig,
    provider="Mila",
)


ConfigStore.instance().store(
    group="hydra/sweeper",
    name="cluv_sweeper",
    node=CluvSweeperConfig,
    provider="Mila",
)

__all__ = [
    "CluvLauncher",
    "CluvLauncherConfig",
    "CluvSweeper",
    "CluvSweeperConfig",
]
