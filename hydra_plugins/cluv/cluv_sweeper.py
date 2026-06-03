import hydra_zen
from hydra.plugins.sweeper import Sweeper
from hydra.types import HydraContext, TaskFunction
from omegaconf import DictConfig

# from .cluv_launcher import CluvLauncher


class CluvSweeper(Sweeper):
    # launcher: CluvLauncher

    def __init__(self):
        super().__init__()

    def setup(
        self,
        *,
        hydra_context: HydraContext,
        task_function: TaskFunction,
        config: DictConfig,
    ) -> None:
        self.hydra_context = hydra_context
        self.task_function = task_function
        self.config = config

    def sweep(self, arguments):
        """
        Execute a sweep
        :param arguments: list of strings describing what this sweeper should do.
        exact structure is determine by the concrete Sweeper class.
        :return: the return objects of all thy launched jobs. structure depends on the Sweeper
        implementation.
        """
        raise NotImplementedError(f"This sweeper is not implemented yet. ({arguments=})")


CluvSweeperConfig = hydra_zen.builds(
    CluvSweeper,
    populate_full_signature=True,
    hydra_convert="object",
    zen_dataclass={"cls_name": "CluvSweeperConfig"},
)
