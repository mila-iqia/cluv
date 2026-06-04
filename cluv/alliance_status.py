"""Scrape https://status.alliancecan.ca/ for Alliance/DRAC service statuses."""
from __future__ import annotations

import asyncio
import html as html_module
import re
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime

__all__ = ["Incident", "ServiceStatus", "fetch_alliance_status", "fetch_alliance_status_async"]

_STATUS_URL = "https://status.alliancecan.ca/"
_INCIDENT_URL = "https://status.alliancecan.ca/view_incident?incident={}"
_USER_AGENT = "Mozilla/5.0 (compatible; cluv)"
_DATE_FMT = "%Y-%m-%d %H:%M"

# Maps the Material Icon name to a canonical status string.
_ICON_TO_STATUS: dict[str, str] = {
    "check": "operational",
    "warning_amber": "degraded",
    "cloud_off": "outage",
    "power_off": "decommissioned",
    "event": "scheduled",
}


@dataclass
class Incident:
    id: str
    title: str
    start: datetime | None = None
    end: datetime | None = None


@dataclass
class ServiceStatus:
    name: str
    # One of: "operational", "degraded", "outage", "scheduled", "decommissioned", "unknown"
    status: str
    incidents: list[Incident] = field(default_factory=list)


def _http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _parse_incident_dates(html: str) -> tuple[datetime | None, datetime | None]:
    """Extract (start, end) datetimes from an incident page."""
    dates: list[datetime] = []
    for raw in re.findall(r'incident_dt_(?:start|end)[^>]*>.*?change_date\("(\d{4}-\d{2}-\d{2} \d{2}:\d{2})"', html, re.DOTALL):
        try:
            dates.append(datetime.strptime(raw, _DATE_FMT))
        except ValueError:
            pass
    start = dates[0] if len(dates) >= 1 else None
    end = dates[1] if len(dates) >= 2 else None
    return start, end


def _parse_main_page(html: str) -> list[tuple[str, str, list[tuple[str, str]]]]:
    """Parse the main table into (name, status, [(incident_id, title), ...]) tuples."""
    table_match = re.search(r"<tbody.*?>(.*?)</tbody>", html, re.DOTALL)
    if not table_match:
        return []

    results = []
    for row in re.findall(r"<tr.*?>(.*?)</tr>", table_match.group(1), re.DOTALL):
        name_match = re.search(r"<a[^>]*>\s*([^<]+?)\s*</a>", row)
        if not name_match:
            continue
        name = html_module.unescape(name_match.group(1).strip())
        if name == "Retired Services":
            continue

        icon_match = re.search(r'<i class="fas material-icons [^"]*">\s*(\w+)\s*</i>', row)
        status = _ICON_TO_STATUS.get(icon_match.group(1), "unknown") if icon_match else "unknown"

        incidents = [
            (m.group(1), html_module.unescape(m.group(2).strip()))
            for m in re.finditer(
                r'href="/view_incident\?incident=(\d+)"[^>]*>\s*(.*?)\s*</a>', row, re.DOTALL
            )
        ]
        results.append((name, status, incidents))

    return results


def _fetch_incident(incident_id: str, title: str) -> Incident:
    html = _http_get(_INCIDENT_URL.format(incident_id))
    start, end = _parse_incident_dates(html)
    return Incident(id=incident_id, title=title, start=start, end=end)


def fetch_alliance_status() -> list[ServiceStatus]:
    """Return current Alliance service statuses scraped from status.alliancecan.ca.

    Incident pages are fetched sequentially.
    Use :func:`fetch_alliance_status_async` for concurrent fetching.
    """
    rows = _parse_main_page(_http_get(_STATUS_URL))
    services = []
    for name, status, raw_incidents in rows:
        incidents = [_fetch_incident(iid, title) for iid, title in raw_incidents]
        services.append(ServiceStatus(name=name, status=status, incidents=incidents))
    return services


async def fetch_alliance_status_async() -> list[ServiceStatus]:
    """Async version of :func:`fetch_alliance_status` with concurrent incident fetching."""
    rows = _parse_main_page(await asyncio.to_thread(_http_get, _STATUS_URL))

    async def _build(name: str, status: str, raw: list[tuple[str, str]]) -> ServiceStatus:
        incidents = list(
            await asyncio.gather(
                *(asyncio.to_thread(_fetch_incident, iid, title) for iid, title in raw)
            )
        )
        return ServiceStatus(name=name, status=status, incidents=incidents)

    return list(await asyncio.gather(*(_build(n, s, i) for n, s, i in rows)))


if __name__ == "__main__":
    import asyncio as _asyncio

    for svc in _asyncio.run(fetch_alliance_status_async()):
        if svc.status == "operational":
            continue
        print(f"[{svc.status}] {svc.name}")
        for inc in svc.incidents:
            start = inc.start.strftime(_DATE_FMT) if inc.start else "?"
            end = inc.end.strftime(_DATE_FMT) if inc.end else "?"
            print(f"  {inc.title}")
            print(f"  {start} → {end}")
