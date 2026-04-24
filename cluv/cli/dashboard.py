def dashboard():
    """Launches a dashboard to monitor the status of available clusters and jobs across them.

    !!! warning
        This function is not implemented yet!

    - Similar to `cluv status`, but launches a dashboard that continuously updates with the status of available clusters and jobs across them.
    - Could be implemented as a simple TUI (text-based user interface) using something like Rich or Textual.

    Terminal UI showing jobs in a table, for each cluster:
    - Job ID
    - Job Name
    - Job State
    - Job Nodes
    - Job Resources
    - Job command
    - Wandb URL?
    - DRAC Portal URL when applicable

    Stretch goals:
    - Adding buttons with Textual to cancel jobs.
    """
    raise NotImplementedError("TODO: " + (dashboard.__doc__ or ""))
