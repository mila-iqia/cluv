def init():
    """Initialize the current project across clusters.

    Akin to `uv init`, sets up the current project on all configured clusters.
    - Prompts to configure which clusters to use (config stored in pyproject.toml of the project)
    - Installs UV on all clusters
            - Sets up the DRAC-specific uv.toml files to use the DRAC wheelhouse when necessary.
    - Sets up the project repo, creates the $SCRATCH checkpoint folder

    ## How it could work (proof-of-concept)
    - Over SSH. Setup UV.
    - Assume GitHub. Clone the project on each cluster. Configure the git credential-cache if necessary.
    """
    raise NotImplementedError("TODO: " + (init.__doc__ or ""))
