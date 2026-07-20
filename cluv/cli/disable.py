"""Commands to disable and re-enable clusters."""

import re
from datetime import datetime, timedelta, timezone

from cluv.cache import (
    DisabledCluster,
    _ensure_utc,
    disable_cluster,
    enable_cluster,
    get_disabled_clusters,
)
from cluv.slurm import parse_slurm_time
from cluv.utils import console

__all__ = ["disable", "enable", "format_remaining", "print_disabled_clusters"]


def parse_duration(period: str) -> timedelta:
    """Parse a duration string into a `datetime.timedelta`.

    Supported formats:

    - An integer (e.g. ``"3"``) → that many **days**.
    - A Slurm-style ``HH:MM:SS`` (e.g. ``"6:00:00"``) or ``D-HH:MM:SS``.
    - Suffixed values: ``"2h"``, ``"30m"``, ``"1d"``, ``"45s"`` (case-insensitive).
      Multiple suffixed tokens can be chained: ``"1d 6h"``.

    Raises:
        ValueError: if the string cannot be parsed.
    """
    period = period.strip()

    # Plain integer → days.
    if re.fullmatch(r"\d+", period):
        return timedelta(days=int(period))

    # Slurm-style: [D-]HH:MM:SS or [D-]H:MM:SS
    try:
        return parse_slurm_time(period)
    except ValueError:
        pass

    # Suffixed tokens: e.g. "1d 6h 30m", "2h", "45s"
    suffix_map = {"d": "days", "h": "hours", "m": "minutes", "s": "seconds"}
    tokens = re.findall(r"(\d+(?:\.\d+)?)\s*([dhms])", period, re.IGNORECASE)
    if tokens:
        # Ensure the whole string is consumed by these tokens.
        reconstructed = "".join(f"{v}{u}" for v, u in tokens)
        if re.sub(r"\s+", "", period).lower() != reconstructed.lower():
            raise ValueError(f"Cannot parse duration: {period!r}")
        kwargs: dict[str, float] = {}
        for value, unit in tokens:
            key = suffix_map[unit.lower()]
            kwargs[key] = kwargs.get(key, 0.0) + float(value)
        return timedelta(**kwargs)

    raise ValueError(
        f"Cannot parse duration {period!r}. "
        "Expected an integer (days), HH:MM:SS, D-HH:MM:SS, or suffixed values like '2h', '1d 6h'."
    )


def format_remaining(disabled_until: datetime) -> str:
    """Return a human-readable string for remaining disable time."""
    remaining = _ensure_utc(disabled_until) - datetime.now(tz=timezone.utc)
    if remaining.total_seconds() <= 0:
        return "expiring now"
    total_seconds = int(remaining.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds and not days:
        parts.append(f"{seconds}s")
    return ", ".join(parts) if parts else "less than a second"


def print_disabled_clusters(disabled: dict[str, DisabledCluster]):
    """Print a warning listing currently disabled clusters with remaining time until re-enabled.

    Prints nothing and returns an empty dict when no clusters are disabled.
    """
    if not disabled:
        return
    parts = []
    for cluster_name, info in disabled.items():
        if info.disabled_until is None:
            parts.append(f"[bold]{cluster_name}[/bold] (indefinitely)")
        else:
            remaining = format_remaining(info.disabled_until)
            parts.append(f"[bold]{cluster_name}[/bold] ({remaining} remaining)")
    console.print(
        f"[yellow]Skipping disabled cluster(s): {', '.join(parts)}.[/yellow] "
        "Run [bold]cluv enable <cluster>[/bold] to re-enable."
    )


def disable(cluster: str, period: str | None = None) -> None:
    """Disable a cluster for a given period, or indefinitely.

    Parameters:
        cluster: The cluster hostname to disable.
        period: How long to disable the cluster. Accepts an integer (days), a
            Slurm-style ``HH:MM:SS`` / ``D-HH:MM:SS`` string, or suffixed values
            like ``"2h"``, ``"1d 6h"``.  When omitted the cluster is disabled
            indefinitely until ``cluv enable <cluster>`` is run.
    """
    from cluv.config import get_cluv_config

    try:
        config = get_cluv_config()
        if cluster not in config.clusters_names:
            available = ", ".join(f"[bold]{c}[/bold]" for c in config.clusters_names)
            console.print(
                f"[red]Error: cluster [bold]{cluster}[/bold] is not defined in the config.[/red] "
                f"Available clusters: {available}."
            )
            return
    except (FileNotFoundError, RuntimeError, ValueError):
        pass  # Config unavailable or invalid — skip validation and proceed anyway.

    disabled_until: datetime | None = None
    if period is not None:
        duration = parse_duration(period)
        disabled_until = datetime.now(tz=timezone.utc).astimezone() + duration

    disable_cluster(cluster, disabled_until=disabled_until)

    if disabled_until is None:
        console.print(
            f"[yellow]Cluster [bold]{cluster}[/bold] has been disabled indefinitely.[/yellow]\n"
            f"Run [bold]cluv enable {cluster}[/bold] to re-enable it."
        )
    else:
        remaining = format_remaining(disabled_until)
        console.print(
            f"[yellow]Cluster [bold]{cluster}[/bold] has been disabled for {remaining}.[/yellow]\n"
            f"It will be automatically re-enabled at {disabled_until.strftime('%Y-%m-%d %H:%M:%S %z')}.\n"
            f"Run [bold]cluv enable {cluster}[/bold] to re-enable it earlier."
        )


def enable(cluster: str) -> None:
    """Re-enable a previously disabled cluster.

    Parameters:
        cluster: The cluster hostname to re-enable.
    """
    was_disabled = enable_cluster(cluster)
    if was_disabled:
        console.print(f"[green]Cluster [bold]{cluster}[/bold] has been re-enabled.[/green]")
        return

    disabled = get_disabled_clusters()
    if disabled:
        names = ", ".join(f"[bold]{c}[/bold]" for c in disabled)
        console.print(
            f"[yellow]Cluster [bold]{cluster}[/bold] was not disabled.[/yellow] "
            f"Currently disabled clusters: {names}."
        )
    else:
        console.print(
            f"[yellow]Cluster [bold]{cluster}[/bold] was not disabled.[/yellow] "
            f"No clusters are currently disabled."
        )
