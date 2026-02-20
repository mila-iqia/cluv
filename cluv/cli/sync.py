def sync(cluster: list[str] = []):
    """Synchronizes the current project across clusters.

    - Synchronizes code across all clusters.
    - Gathers results on the "main" cluster (mila)
    - Does `uv sync` that cluster as well
        - (Important so that jobs can be run in OFFLINE mode)

    ## How it could work (proof-of-concept)
    - Checks git state
    - Push to github
        - TODO: Check syncing without github.
    - Over SSH, does a git fetch on all remote clusters
    - Gathers results from all other clusters to the Mila cluster using rsync.
    """
    raise NotImplementedError("TODO: " + (sync.__doc__ or ""))
