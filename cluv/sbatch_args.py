import argparse

SbatchArgs = dict[str, str | int | float | bool]
"""A set of sbatch flags, as a mapping from flag name to value."""


def sbatch_args_to_list(d: SbatchArgs) -> list[str]:
    """Convert a dict of sbatch options to a list of command-line flags.

    Key-to-flag conversion:

    - multi-char key + non-empty string value → ``--key=value``
    - single-char key + non-empty string value → ``-k value`` (two separate args)
    - any key + ``True`` → bare flag (``--key`` or ``-k``)
    - any key + ``""`` or ``False`` → omitted entirely

    >>> sbatch_args_to_list({"time": "2:00:00", "gpus": "1"})
    ['--time=2:00:00', '--gpus=1']
    >>> sbatch_args_to_list({"exclusive": True})
    ['--exclusive']
    >>> sbatch_args_to_list({"N": "2"})
    ['-N', '2']
    >>> sbatch_args_to_list({"gpus": "", "requeue": False})
    []
    >>> sbatch_args_to_list({"n": True})
    ['-n']
    """
    flags: list[str] = []
    for key, value in d.items():
        if value == "" or value is False:
            continue
        is_short_flag = len(key) == 1
        if value is True:
            flags.append(f"-{key}" if is_short_flag else f"--{key}")
        else:
            if is_short_flag:
                flags.extend([f"-{key}", str(value)])
            else:
                flags.append(f"--{key}={value}")
    return flags


def sbatch_args_from_list(sbatch_args_list: list[str]) -> SbatchArgs:
    """Convert a list of sbatch flags (from the CLI) to a dict of sbatch options.

    Behaves like argparse, where if the flags are passed multiple times, the last value is kept.
    Aliases for common commands are also kept.

    >>> sbatch_args_from_args_list(["--time=2:00:00", "-t=00:00:30"])
    {'time': '00:00:30'}
    >>> sbatch_args_from_args_list(["--time=2:00:00", "--gpus=1"])
    {'time': '2:00:00', 'gpus': '1'}
    >>> sbatch_args_from_args_list(["--exclusive"])
    {'exclusive': True}
    >>> sbatch_args_from_args_list(["-N", "2"])
    {'nodes': '2'}
    >>> sbatch_args_from_args_list(["-f", "2"])
    {'f': '2'}
    >>> sbatch_args_from_args_list(["--gpus", "--requeue=False"])
    {'gpus': True, 'requeue': 'False'}
    >>> sbatch_args_from_args_list(["-n"])
    {'n': True}
    >>> sbatch_args_from_args_list(["--array=0-3%2", "-a=0-1%1"])
    {'array': '0-3%2', 'a': '0-1%1'}
    """
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("-c", "--cpus-per-task", dest="cpus-per-task", default=argparse.SUPPRESS)
    parser.add_argument("-t", "--time", dest="time", default=argparse.SUPPRESS)
    parser.add_argument("-N", "--nodes", dest="nodes", default=argparse.SUPPRESS)
    parser.add_argument("-A", "--account", dest="account", default=argparse.SUPPRESS)
    args, unknown = parser.parse_known_args(sbatch_args_list)
    sbatch_args: SbatchArgs = vars(args)

    # First, join any stragglers like ['-f', '2'] into ['-f=2'] so we can parse them consistently.
    # Edge case: ['-f', '-g'] stays the same.
    joined_unknown_args: list[str] = []
    skip_next = False
    for i, arg in enumerate(unknown):
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("-") and i + 1 < len(unknown) and not unknown[i + 1].startswith("-"):
            joined_unknown_args.append(f"{arg}={unknown[i + 1]}")
            skip_next = True
        else:
            joined_unknown_args.append(arg)

    for value in joined_unknown_args:
        if value.startswith("--"):
            key, _, val = value[2:].partition("=")
        elif value.startswith("-"):
            value = value.removeprefix("-")
            key, _, val = value.partition("=")
        else:
            continue
        if not val.strip():
            val = True  # --exclusive --> {exclusive: True}
        if val is not None:
            if key in sbatch_args:
                # remove the value so the ordering is preserved based on the positioning in `sbatch_args_list`.
                sbatch_args.pop(key)
            sbatch_args[key] = val
    return sbatch_args
