"""CLUV: Tool to use UV with multiple clusters."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("cluster-uv")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"
