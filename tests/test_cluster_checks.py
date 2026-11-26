"""Unit tests for cluv/cli/init_utils/cluster_checks.py."""

import asyncio
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

import pytest
import questionary

from cluv.cache import DisabledCluster
from cluv.cli.init_utils import cluster_checks
from cluv.cli.init_utils.cluster_checks import (
    Check,
    Status,
    check_accounts,
    check_cluster,
    check_clusters,
    check_folder,
    check_github,
    check_multiplexing,
    check_on_cluster,
    check_ssh_entry,
    cluster_panel,
    connect_to_clusters,
    print_keep_alive_tip,
)
from cluv.config import CluvConfig
from cluv.remote import Remote
from cluv.utils import console
from tests.test_init import (
    REPO_ROOT,
    FakeQuestion,
    fake_ssh_config,
    make_cluv_config_with_clusters,
)


def render(renderable) -> str:
    """The text that the console prints for the renderable, without colors."""
    with console.capture() as capture:
        console.print(renderable)
    return capture.get()


class TestCheckCluster:
    """The checks of a cluster that don't need a connection to the cluster."""

    @pytest.fixture(autouse=True)
    def not_on_a_cluster(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cluster_checks, "current_cluster", lambda: None)

    def test_no_ssh_entry_for_a_cluster_of_mila_init(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`mila init` adds the entries of the clusters of the template. When the user doesn't have
        access to the cluster, its `cluster_link` says how to get access."""
        fake_ssh_config(monkeypatch, [])

        check = check_ssh_entry("tamia", "https://docs.alliancecan.ca/wiki/TamIA")

        assert check.status is Status.FAIL
        assert check.hints == (
            "if you have access to tamia, run `mila init` to add it",
            "otherwise, see how to get access: https://docs.alliancecan.ca/wiki/TamIA",
        )

    def test_no_ssh_entry_for_another_cluster(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_ssh_config(monkeypatch, [])

        check = check_ssh_entry("my_cluster", None)

        assert check.status is Status.FAIL
        assert check.hints == (
            "if you have access to my_cluster, add a `Host my_cluster` entry for it",
        )

    @pytest.mark.parametrize(
        ("cluster", "hint"),
        [
            (
                "tamia",
                "run `mila init`, or add these lines to the `Host` entry of tamia in "
                "~/.ssh/config:",
            ),
            ("my_cluster", "add these lines to the `Host` entry of my_cluster in ~/.ssh/config:"),
        ],
    )
    def test_ssh_entry_without_multiplexing(
        self, cluster: str, hint: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without `ControlMaster`, `ControlPath` and `ControlPersist`, cluv commands can open new
        SSH connections, which can ask for a 2FA. `mila init` adds these options to the entries of
        the clusters of the template."""
        fake_ssh_config(monkeypatch, [cluster], without_multiplexing=(cluster,))
        config = make_cluv_config_with_clusters(**{cluster: {}})

        checks = check_cluster(config, cluster, disabled={})

        assert checks == [
            Check(Status.OK, "SSH entry in ~/.ssh/config"),
            Check(
                Status.WARN,
                "SSH entry without ControlMaster, ControlPath and ControlPersist: cluv commands "
                "can ask for a 2FA again",
                (hint,),
                "ControlMaster auto\nControlPath ~/.ssh/cm-%r@%h:%p\nControlPersist yes",
            ),
        ]

    def test_only_the_missing_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The warning only gives the options that the SSH entry doesn't have."""
        options = {"controlmaster": "auto", "controlpath": "/tmp/%h", "controlpersist": "no"}
        monkeypatch.setattr(cluster_checks, "get_ssh_options_for_host", lambda host: options)

        check = check_multiplexing("rorqual")

        assert check is not None
        assert check.message == (
            "SSH entry without ControlPersist: cluv commands can ask for a 2FA again"
        )
        assert check.code == "ControlPersist yes"

    @pytest.mark.parametrize(
        ("disabled_until", "period"),
        [(None, "indefinitely"), (timedelta(days=2, hours=1), "for 2d, ")],
    )
    def test_disabled_cluster(
        self, disabled_until: timedelta | None, period: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_ssh_config(monkeypatch, ["rorqual"])
        config = make_cluv_config_with_clusters(rorqual={})
        now = datetime.now(tz=timezone.utc)
        disabled = DisabledCluster(
            disabled_at=now, disabled_until=disabled_until and now + disabled_until
        )

        checks = check_cluster(config, "rorqual", disabled={"rorqual": disabled})

        assert checks[0] == Check(Status.OK, "SSH entry in ~/.ssh/config")
        assert checks[1].status is Status.WARN
        assert checks[1].message.startswith(f"disabled {period}")
        assert checks[1].message.endswith(": cluv commands skip rorqual")
        assert checks[1].hints == ("run `cluv enable rorqual` to use it again",)


class TestDisplay:
    def test_frame_shows_every_check(self) -> None:
        """Each check has a line with its icon, followed by its hints and by its line to copy."""
        checks = [
            Check(Status.OK, "SSH entry in ~/.ssh/config"),
            Check(Status.WARN, "disabled indefinitely", ("run `cluv enable rorqual`",)),
            Check(
                Status.FAIL,
                "Slurm account def-x isn't one of yours",
                ("your accounts on rorqual: def-y",),
                'sbatch_args = { account = "def-y" }',
            ),
            Check(Status.SKIP, "not connected"),
        ]

        output = render(cluster_panel("rorqual", checks))

        assert "rorqual  ✔ 1  ! 1  ✘ 1  - 1" in output
        for line in [
            "│ ✔ SSH entry in ~/.ssh/config",
            "│ ! disabled indefinitely",
            "│   → run `cluv enable rorqual`",
            "│ ✘ Slurm account def-x isn't one of yours",
            "│   → your accounts on rorqual: def-y",
            '│     sbatch_args = { account = "def-y" }',
            "│ - not connected",
        ]:
            assert line in output

    @pytest.mark.parametrize(
        ("statuses", "color"),
        [
            ([Status.OK, Status.OK], "green"),
            ([Status.OK, Status.SKIP], "dim"),
            ([Status.SKIP, Status.WARN, Status.OK], "yellow"),
            ([Status.WARN, Status.FAIL, Status.SKIP], "red"),
        ],
    )
    def test_frame_has_the_color_of_the_worst_result(
        self, statuses: list[Status], color: str
    ) -> None:
        checks = [Check(status, "message") for status in statuses]

        assert cluster_panel("rorqual", checks).border_style == color

    def test_long_links_are_not_cut(self) -> None:
        """A link longer than the frame continues on the next line, instead of being cut."""
        url = "https://docs.alliancecan.ca/wiki/" + "x" * 100

        output = render(cluster_panel("rorqual", [Check(Status.FAIL, "message", (f"see {url}",))]))

        assert "…" not in output
        assert url in "".join(line.strip("│ ") for line in output.splitlines())

    def test_urls_are_links(self) -> None:
        url = "https://docs.alliancecan.ca/wiki/Narval"

        text = cluster_checks._text(f"see {url} for more")

        assert [(text.plain[span.start : span.end], str(span.style)) for span in text.spans] == [
            (url, f"link {url}")
        ]

    def test_keep_alive_tip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The tip says why it's useful, and lists the clusters whose SSH entry doesn't keep the
        connection open."""
        monkeypatch.setattr(cluster_checks, "current_cluster", lambda: None)
        fake_ssh_config(monkeypatch, ["mila", "rorqual", "narval"], keep_alive=("mila",))

        with console.capture() as capture:
            print_keep_alive_tip(["mila", "rorqual", "narval", "tamia"])

        output = " ".join(capture.get().split())
        assert output.startswith(
            "💡 Optional: a network can close an SSH connection that stays idle for a long time, "
            "and you would then need `cluv login` (and a 2FA) again."
        )
        assert "`Host` entries of rorqual and narval in ~/.ssh/config:" in output
        assert "ServerAliveInterval 120 ServerAliveCountMax 5" in output

    def test_no_keep_alive_tip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cluster_checks, "current_cluster", lambda: None)
        fake_ssh_config(monkeypatch, ["mila"], keep_alive=("mila",))

        with console.capture() as capture:
            print_keep_alive_tip(["mila"])

        assert capture.get() == ""


# The results of the commands on a fake cluster, as (exit code, stdout, stderr).
CONNECTED = (0, "OK\n", "")
GITHUB_ACCESS = (
    1,
    "",
    "Hi me! You've successfully authenticated, but GitHub does not provide...\n",
)
NO_GITHUB_ACCESS = (255, "", "git@github.com: Permission denied (publickey).\n")
# The Slurm associations of a user on a DRAC cluster, with an account for the CPU jobs and another
# one for the GPU jobs.
DRAC_ACCOUNTS = (0, "def-me_cpu\ndef-me_gpu\nrrg-me-ad_cpu\nrrg-me-ad_gpu\n", "")
# A command that never ends, like a command that reads a file system that doesn't answer.
NO_ANSWER = None


class FakeRemote:
    """Stands in for a `Remote`: the result of a command is `results[word]`, for the first `word`
    that is in the command."""

    def __init__(self, hostname: str, results: dict[str, tuple[int, str, str] | None]):
        self.hostname = hostname
        self.results = results

    async def run(self, command: str, **kwargs) -> subprocess.CompletedProcess[str]:
        for word, result in self.results.items():
            if word in command:
                while result is None:  # NO_ANSWER: the command never ends.
                    await asyncio.sleep(3600)
                returncode, stdout, stderr = result
                return subprocess.CompletedProcess(command, returncode, stdout, stderr)
        raise AssertionError(f"Unexpected command on {self.hostname}: {command}")


def fake_clusters(
    monkeypatch: pytest.MonkeyPatch,
    results: dict[str, dict[str, tuple[int, str, str] | None]],
    connected: list[str],
) -> None:
    """Makes the commands on each cluster give the `results` of the cluster, with the clusters of
    `connected` already connected."""

    async def control_socket_is_running(host: str) -> bool:
        return host in connected

    monkeypatch.setattr(cluster_checks, "control_socket_is_running", control_socket_is_running)
    monkeypatch.setattr(
        cluster_checks,
        "Remote",
        lambda hostname: FakeRemote(hostname, results.get(hostname, {})),
    )


def answer_connect_question(monkeypatch: pytest.MonkeyPatch, answer) -> list[str]:
    """Makes questionary give `answer` to the question about connecting to the clusters. Returns
    the questions that get asked."""
    asked = []

    def confirm(message: str, **kwargs) -> FakeQuestion:
        asked.append(message)
        return FakeQuestion(answer)

    monkeypatch.setattr(questionary, "confirm", confirm)
    return asked


NOT_CONNECTED = Check(
    Status.SKIP,
    "not connected: GitHub access, Slurm accounts and folders not checked",
    ("run `cluv login rorqual`, then `cluv init` again",),
)


class TestConnectToClusters:
    config = make_cluv_config_with_clusters(
        mila={}, rorqual={"cluster_link": "https://docs.alliancecan.ca/wiki/Rorqual"}
    )

    async def test_no_question_without_ask(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without `ask` (with `--quiet`, or when the input isn't a terminal), only the clusters
        that are already connected are checked."""
        fake_clusters(monkeypatch, {}, connected=["mila"])
        asked = answer_connect_question(monkeypatch, True)

        connections = await connect_to_clusters(self.config, ["mila", "rorqual"], ask=False)

        assert asked == []
        assert connections == {"mila": Check(Status.OK, "connected"), "rorqual": NOT_CONNECTED}

    async def test_connect_when_the_user_agrees(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_clusters(
            monkeypatch, {"mila": {"echo OK": CONNECTED}, "rorqual": {"echo OK": CONNECTED}}, []
        )
        asked = answer_connect_question(monkeypatch, True)

        connections = await connect_to_clusters(self.config, ["mila", "rorqual"], ask=True)

        assert asked == [
            "Connect to mila and rorqual now, to check them? There may be a 2FA prompt for each "
            "cluster."
        ]
        assert connections == {
            "mila": Check(Status.OK, "connected"),
            "rorqual": Check(Status.OK, "connected"),
        }

    @pytest.mark.parametrize("answer", [False, KeyboardInterrupt, EOFError])
    async def test_no_connection_when_the_user_refuses(
        self, answer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No, Ctrl-C or Ctrl-D: the clusters that aren't connected aren't checked."""
        fake_clusters(monkeypatch, {}, connected=[])
        answer_connect_question(monkeypatch, answer)

        connections = await connect_to_clusters(self.config, ["rorqual"], ask=True)

        assert connections == {"rorqual": NOT_CONNECTED}

    async def test_no_question_when_all_the_clusters_are_connected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_clusters(monkeypatch, {}, connected=["mila", "rorqual"])
        asked = answer_connect_question(monkeypatch, True)

        await connect_to_clusters(self.config, ["mila", "rorqual"], ask=True)

        assert asked == []

    async def test_connection_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The last line of the SSH errors says why the connection failed. The hints give the
        `cluster_link` of the cluster, when the config has one."""
        error = (255, "", "Welcome!\nPermission denied (publickey).\n")
        fake_clusters(
            monkeypatch, {"mila": {"echo OK": error}, "rorqual": {"echo OK": error}}, connected=[]
        )
        answer_connect_question(monkeypatch, True)

        connections = await connect_to_clusters(self.config, ["mila", "rorqual"], ask=True)

        message = "connection failed: Permission denied (publickey)."
        assert connections == {
            "mila": Check(Status.FAIL, message, ("run `ssh mila` to see the details",)),
            "rorqual": Check(
                Status.FAIL,
                message,
                (
                    "run `ssh rorqual` to see the details",
                    "if you don't have access to rorqual yet, see: "
                    "https://docs.alliancecan.ca/wiki/Rorqual",
                ),
            ),
        }


class TestCheckGithub:
    async def test_no_github_access(self) -> None:
        check = await check_github(FakeRemote("rorqual", {"git@github.com": NO_GITHUB_ACCESS}))

        assert check.status is Status.FAIL
        assert check.hints == (
            "see how to set it up: https://mila-iqia.github.io/cluv/troubleshooting/"
            "#setting-up-ssh-access-to-github-from-the-clusters",
        )


class TestCheckAccounts:
    @staticmethod
    async def check(cluster: str, sbatch_args, sacctmgr: tuple[int, str, str]) -> Check:
        config = make_cluv_config_with_clusters(**{cluster: {"sbatch_args": sbatch_args}})
        remote = FakeRemote(cluster, {"sacctmgr": sacctmgr})
        return await check_accounts(remote, config.get_cluster_config(cluster))

    async def test_other_accounts_of_the_user(self) -> None:
        """The check suggests a `sbatch_args` line with all the accounts of the user, starting
        with the accounts of the config."""
        check = await self.check("rorqual", {"account": "rrg-me-ad"}, DRAC_ACCOUNTS)

        assert check == Check(
            Status.OK,
            "Slurm account: rrg-me-ad",
            (
                "you also have def-me. To use all your accounts, set in pyproject.toml, under "
                "[tool.cluv.clusters.rorqual]:",
            ),
            'sbatch_args = [{ account = "rrg-me-ad" }, { account = "def-me" }]',
        )

    async def test_account_of_an_association(self) -> None:
        """An account with the `_gpu` suffix of its association is an account of the user too."""
        check = await self.check("rorqual", {"account": "def-me_gpu"}, DRAC_ACCOUNTS)

        assert check.status is Status.OK
        assert check.hints[0].startswith("you also have rrg-me-ad. ")

    async def test_account_that_isnt_the_users(self) -> None:
        check = await self.check("narval", {"account": "def-bengioy"}, DRAC_ACCOUNTS)

        assert check == Check(
            Status.FAIL,
            "not among your Slurm accounts on narval: def-bengioy",
            (
                "your accounts: def-me, rrg-me-ad. Set in pyproject.toml, under "
                "[tool.cluv.clusters.narval]:",
            ),
            'sbatch_args = [{ account = "def-me" }, { account = "rrg-me-ad" }]',
        )

    async def test_no_account_on_the_mila_cluster(self) -> None:
        check = await self.check("mila", {}, (0, "mila\n", ""))

        assert check == Check(
            Status.OK, "no Slurm account set (usually not needed on the Mila cluster)"
        )

    async def test_no_account_with_a_single_account(self) -> None:
        """Slurm uses the only account of the user. The login shell can print lines that aren't
        accounts, like a welcome message."""
        sacctmgr = (0, "Welcome to Narval!\ndef-me_cpu\ndef-me_gpu\n", "")

        check = await self.check("narval", {}, sacctmgr)

        assert check == Check(
            Status.OK, "no Slurm account set (Slurm uses your only account, def-me)"
        )

    async def test_no_account_with_many_accounts(self) -> None:
        check = await self.check("narval", {}, DRAC_ACCOUNTS)

        assert check == Check(
            Status.WARN,
            "no Slurm account set, but you have several on narval: jobs may need one",
            (
                "to use all your accounts, set in pyproject.toml, under "
                "[tool.cluv.clusters.narval]:",
            ),
            'sbatch_args = [{ account = "def-me" }, { account = "rrg-me-ad" }]',
        )

    async def test_accounts_that_cant_be_listed(self) -> None:
        sacctmgr = (1, "", "sacctmgr: command not found\n")

        check = await self.check("narval", {"account": "def-me"}, sacctmgr)

        assert check == Check(Status.SKIP, "couldn't list your Slurm accounts on narval")

    @pytest.mark.parametrize(
        ("cluster_link", "access_hints"),
        [
            (
                "https://docs.alliancecan.ca/wiki/TamIA",
                ("to get access, see https://docs.alliancecan.ca/wiki/TamIA",),
            ),
            (None, ()),
        ],
    )
    async def test_no_account_on_the_cluster(
        self, cluster_link: str | None, access_hints: tuple[str, ...]
    ) -> None:
        """When `sacctmgr` lists no account, the user has no account on the cluster, like on a
        PAICE cluster without access to the allocation of a PI. Slurm refuses the jobs then."""
        config = make_cluv_config_with_clusters(tamia={"cluster_link": cluster_link})
        remote = FakeRemote("tamia", {"sacctmgr": (0, "", "")})

        check = await check_accounts(remote, config.get_cluster_config("tamia"))

        assert check == Check(
            Status.FAIL,
            "you have no Slurm account on tamia: jobs can't run there",
            (
                *access_hints,
                "to stop using tamia, remove [tool.cluv.clusters.tamia] from pyproject.toml, or "
                "run `cluv disable tamia`",
            ),
        )


class LocalRemote:
    """Stands in for a `Remote`: runs the commands on this machine."""

    hostname = "local"

    async def run(self, command: str, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.run(command, shell=True, capture_output=True, text=True)


class TestCheckFolder:
    """The checks of the folders, with the script of the checks run by bash on this machine."""

    @pytest.fixture(autouse=True)
    def home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        monkeypatch.setenv("HOME", str(tmp_path))
        return tmp_path

    @staticmethod
    async def check(path: str, must_exist: bool = False) -> Check:
        key = "data_source" if must_exist else "results_path"
        return await check_folder(
            LocalRemote(), "my folder", key, PurePosixPath(path), must_exist=must_exist
        )

    async def test_existing_folder(self, home: Path) -> None:
        """The variables of the path, like $HOME, are replaced."""
        (home / "logs").mkdir()

        assert await self.check("$HOME/logs") == Check(Status.OK, f"my folder: {home}/logs")

    async def test_folder_that_cluv_sync_creates(self, home: Path) -> None:
        check = await self.check("$HOME/logs/my_project")

        assert check == Check(
            Status.OK, f"my folder: {home}/logs/my_project (cluv sync will create it)"
        )

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can write in any folder")
    async def test_folder_that_cant_be_created(self, home: Path) -> None:
        read_only = home / "read_only"
        read_only.mkdir(mode=0o555)

        check = await self.check("$HOME/read_only/logs/my_project")

        assert check == Check(
            Status.FAIL,
            f"my folder: can't create {read_only}/logs/my_project, no write access to {read_only}",
            ("set `results_path` for local in pyproject.toml, under [tool.cluv.clusters.local]",),
        )

    async def test_file(self, home: Path) -> None:
        (home / "logs").touch()

        check = await self.check("$HOME/logs")

        assert check.status is Status.FAIL
        assert check.message == f"my folder: {home}/logs is a file, not a folder"

    async def test_undefined_variable(self) -> None:
        """A path with a variable that isn't defined on the cluster can't be used."""
        check = await self.check("$CLUV_UNDEFINED_VARIABLE/logs")

        assert check.status is Status.FAIL
        assert check.message.startswith("my folder: can't check $CLUV_UNDEFINED_VARIABLE/logs: ")
        assert "CLUV_UNDEFINED_VARIABLE" in check.message.split(": ", 2)[2]

    async def test_datasets_to_copy_must_exist(self, home: Path) -> None:
        check = await self.check("$HOME/datasets", must_exist=True)

        assert check == Check(
            Status.FAIL,
            f"my folder: {home}/datasets doesn't exist on local",
            ("fix `data_source` in pyproject.toml",),
        )
        (home / "datasets").touch()
        assert await self.check("$HOME/datasets", must_exist=True) == Check(
            Status.OK, f"my folder: {home}/datasets"
        )


@pytest.mark.parametrize(
    ("data_source", "expected_datasets_checks"),
    [
        (None, {}),
        (
            "mila:/network/datasets/x",
            {
                "mila": ("datasets to copy", "/network/datasets/x", True),
                "killarney": ("datasets folder", "$SCRATCH/data", False),
            },
        ),
        (
            "/data/x",
            {
                "mila": ("datasets folder", "$SCRATCH/data", False),
                "killarney": ("datasets folder", "$SCRATCH/data", False),
            },
        ),
    ],
)
async def test_folders_of_each_cluster(
    data_source: str | None,
    expected_datasets_checks: dict[str, tuple[str, str, bool]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without `project_dir`, the project goes in the same folder as on this machine, from $HOME.
    The datasets are only checked with a `data_source`: on the cluster of the `data_source`, the
    datasets to copy have to exist."""
    checked = []

    async def check_folder(remote, name: str, key: str, path, must_exist: bool = False) -> Check:
        checked.append((remote.hostname, name, str(path), must_exist))
        return Check(Status.OK, name)

    async def other_check(*args) -> Check:
        return Check(Status.OK, "other check")

    monkeypatch.setattr(cluster_checks, "check_folder", check_folder)
    monkeypatch.setattr(cluster_checks, "check_github", other_check)
    monkeypatch.setattr(cluster_checks, "check_accounts", other_check)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    config = CluvConfig.model_validate(
        {
            "results_path": "$SCRATCH/logs/my_project",
            "data_source": data_source,
            "datasets_path": "$SCRATCH/data",
            "clusters": {"mila": {}, "killarney": {"project_dir": "$SCRATCH/repos/my_project"}},
        }
    )

    for cluster in ["mila", "killarney"]:
        await check_on_cluster(config, cluster, tmp_path / "code" / "my_project")

    expected = [
        ("mila", "project folder", "$HOME/code/my_project", False),
        ("mila", "results folder", "$SCRATCH/logs/my_project", False),
        ("killarney", "project folder", "$SCRATCH/repos/my_project", False),
        ("killarney", "results folder", "$SCRATCH/logs/my_project", False),
    ]
    for cluster, datasets_check in expected_datasets_checks.items():
        index = max(i for i, check in enumerate(expected) if check[0] == cluster) + 1
        expected.insert(index, (cluster, *datasets_check))
    assert checked == expected


@pytest.mark.parametrize(
    ("cluster", "status_hints"),
    [
        ("rorqual", ("see the status of the Alliance clusters: https://status.alliancecan.ca/",)),
        ("mila", ()),
    ],
)
async def test_no_answer_from_a_cluster(
    cluster: str, status_hints: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When a check doesn't end in time, like a check of a folder in $SCRATCH while $SCRATCH
    doesn't answer, the checks on the cluster stop, and the cluster gets a warning. The Alliance
    clusters have a status page, unlike the Mila cluster."""
    monkeypatch.setattr(cluster_checks, "REMOTE_CHECKS_TIMEOUT", 0.1)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    results = {
        "git@github.com": GITHUB_ACCESS,
        "sacctmgr": DRAC_ACCOUNTS,
        'path="$HOME': (0, "folder\t/home/me/my_project\t/home/me\n", ""),
        'path="$SCRATCH': NO_ANSWER,
    }
    fake_clusters(monkeypatch, {cluster: results}, connected=[cluster])
    config = make_cluv_config_with_clusters(**{cluster: {}})

    checks = await check_on_cluster(config, cluster, tmp_path / "my_project")

    assert checks == [
        Check(
            Status.WARN,
            f"no answer from {cluster} after 0.1 s: GitHub access, Slurm accounts and folders not "
            "checked",
            (
                f"{cluster} may have a problem, such as a file system that doesn't answer: run "
                "`cluv init` again later",
                *status_hints,
            ),
        )
    ]


async def test_check_clusters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Each cluster has a frame with its checks. The clusters that cluv doesn't connect to (without
    an SSH entry, disabled, or the current cluster) have no checks on the cluster. The current
    cluster needs no SSH entry."""
    fake_ssh_config(monkeypatch, ["rorqual", "narval"])
    monkeypatch.setattr(cluster_checks, "current_cluster", lambda: "mila")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    disabled = {"narval": DisabledCluster(disabled_at=datetime.now(tz=timezone.utc))}
    monkeypatch.setattr(cluster_checks, "get_disabled_clusters", lambda: disabled)
    rorqual_results = {
        "git@github.com": GITHUB_ACCESS,
        "sacctmgr": DRAC_ACCOUNTS,
        'path="$HOME/my_project"': (0, "creatable\t/home/me/my_project\t/home/me\n", ""),
        'path="$SCRATCH/logs"': (0, "folder\t/scratch/me/logs\t/scratch/me\n", ""),
    }
    fake_clusters(monkeypatch, {"rorqual": rorqual_results}, connected=["rorqual"])
    config = make_cluv_config_with_clusters(
        mila={},
        rorqual={"sbatch_args": [{"account": "def-me"}, {"account": "rrg-me-ad"}]},
        narval={},
        tamia={},
    )

    with console.capture() as capture:
        reports = await check_clusters(config, tmp_path / "my_project", ask=True)

    assert reports == {
        "mila": [
            Check(Status.OK, "current cluster: no SSH connection needed"),
            Check(
                Status.SKIP,
                "GitHub access, Slurm accounts and folders not checked on this cluster",
            ),
        ],
        "rorqual": [
            Check(Status.OK, "SSH entry in ~/.ssh/config"),
            Check(Status.OK, "connected"),
            Check(Status.OK, "SSH access to GitHub"),
            Check(Status.OK, "Slurm accounts: def-me, rrg-me-ad"),
            Check(Status.OK, "project folder: /home/me/my_project (cluv sync will create it)"),
            Check(Status.OK, "results folder: /scratch/me/logs"),
        ],
        "narval": [
            Check(Status.OK, "SSH entry in ~/.ssh/config"),
            cluster_checks.check_disabled("narval", disabled["narval"]),
        ],
        "tamia": [check_ssh_entry("tamia", None)],
    }
    output = capture.get()
    assert "rorqual  ✔ 6" in output
    assert "Clusters: 1 ready · 1 with warnings · 1 not checked · 1 with problems" in output


async def test_check_clusters_without_clusters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without any cluster in the config, there is nothing to check or to show."""
    monkeypatch.setattr(cluster_checks, "get_disabled_clusters", lambda: {})

    with console.capture() as capture:
        reports = await check_clusters(make_cluv_config_with_clusters(), tmp_path, ask=True)

    assert reports == {}
    assert capture.get() == ""


@pytest.mark.integration
async def test_checks_on_a_cluster(remote: Remote) -> None:
    """The commands of the checks work on the clusters. The result of the GitHub check depends on
    the SSH keys of the user, but `sacctmgr` lists the Slurm accounts of the user, and the results
    go in $SCRATCH, which is defined on all the clusters."""
    config = make_cluv_config_with_clusters(**{remote.hostname: {}})

    checks = await check_on_cluster(config, remote.hostname, REPO_ROOT)

    # A single check is the warning about a cluster that doesn't answer in time.
    assert len(checks) == 4, checks[0].message
    _github, accounts, project, results = checks
    assert accounts.status is not Status.SKIP, accounts.message
    if accounts.message.startswith("you have no Slurm account"):
        # Without access to the cluster, the folder of the user in $SCRATCH may not exist.
        pytest.skip(accounts.message)
    assert project.status is Status.OK, project.message
    assert results.status is Status.OK, results.message
