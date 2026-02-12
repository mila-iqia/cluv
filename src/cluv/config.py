"""Lightweight config system based on a section in the pyproject.toml file.

Should at the very least contain:
- a list of available clusters (hostnames).

Ideally, should also contain:
- A set of overrides for each cluster (which partition, gpu, account, etc to use).

Stretch goal (might be useful):
- Documentation links for each cluster (for LLMs to look at?)
"""
