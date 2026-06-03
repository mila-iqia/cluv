# IDEA: Replacement for the --multirun option of Hydra.
# from hydra.main import   # noqa

import logging

from hydra._internal.core_plugins.basic_sweeper import BasicSweeper
from hydra.core.override_parser.overrides_parser import OverridesParser

logger = logging.getLogger(__name__)


def split_arguments_for_each_run(multirun_overrides: list[str]) -> list[list[str]]:
    overrides_objects = OverridesParser.create().parse_overrides(multirun_overrides)
    batches = BasicSweeper.split_arguments(overrides_objects, max_batch_size=None)
    assert len(batches) == 1  # all jobs in a single batch, since `max_batch_size=None`.
    return batches[0]
