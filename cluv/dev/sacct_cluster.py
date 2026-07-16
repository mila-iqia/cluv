import argparse
import asyncio
from datetime import datetime, timedelta

from cluv.cli.login import login
from cluv.remote import Remote
from cluv.utils import console

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def split_into_weeks(start_time: str, end_time: str) -> list[tuple[str, str]]:
    """Split a [start_time, end_time) range into consecutive week-long periods."""
    start = datetime.strptime(start_time, TIME_FORMAT)
    end = datetime.strptime(end_time, TIME_FORMAT)

    periods = []
    period_start = start
    while period_start < end:
        period_end = min(period_start + timedelta(weeks=1), end)
        periods.append((period_start.strftime(TIME_FORMAT), period_end.strftime(TIME_FORMAT)))
        period_start = period_end
    return periods


async def run_sacct(remote: Remote, start_time: str, end_time: str) -> str:
    """Run sacct on the given time period and return the raw (pipe-separated) output."""
    now = datetime.now()

    sacct_command = f"sacct --format=Account,JobID,JobName,Submit,Start,End,State,User,Timelimit,WorkDir --starttime={start_time} --endtime={end_time} -a -X --parsable2 --allocations --array --noheader | grep cluv- || true"
    output = await remote.get_output(sacct_command, hide=True)

    console.print(
        f"Received output for {start_time} to {end_time} in {datetime.now() - now}. Found {len(output.splitlines())} lines."
    )
    return output.strip()


async def main(cluster: str, start_time: str, end_time: str):
    remotes = await login([cluster])
    if not remotes:
        print(f"Could not connect to cluster {cluster}.")
        return

    remote = remotes[0]

    if not remote:
        console.print(
            f"[red]Could not find an active connection to cluster [bold]{cluster}[/bold].[/red]"
        )
        return

    # Warm up the multiplexed connection with one sequential round-trip before firing
    # off many concurrent commands at the same host. Without this, several `ssh`
    # processes launched at once against the same host can race to become the
    # control master, each triggering its own MFA prompt.
    # await remote.get_output("true", hide=True)

    weeks = split_into_weeks(start_time, end_time)
    console.print(f"Will run sacct on {len(weeks)} week-long periods.")

    outputs = []
    for week_start, week_end in weeks:
        output = await run_sacct(remote, week_start, week_end)
        outputs.append(output)
    # outputs = await asyncio.gather(
    #     *(run_sacct(remote, week_start, week_end) for week_start, week_end in weeks)
    # )

    # To csv, keeping only the first week's header line.
    with open(f"{remote.hostname}_jobs.csv", "w") as f:
        f.write("Account,JobID,JobName,Submit,User\n")
        for output in outputs:
            lines = output.splitlines()
            if len(lines) == 0:
                continue
            f.write("\n".join(lines).replace("|", ",") + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run sacct on a remote cluster.")
    parser.add_argument("cluster", type=str, help="The hostname of the remote cluster.")
    parser.add_argument(
        "start_time",
        type=str,
        help="The start time for the sacct command (format: YYYY-MM-DDTHH:MM:SS).",
    )
    parser.add_argument(
        "end_time",
        type=str,
        help="The end time for the sacct command (format: YYYY-MM-DDTHH:MM:SS).",
    )
    args = parser.parse_args()

    asyncio.run(main(args.cluster, args.start_time, args.end_time))
