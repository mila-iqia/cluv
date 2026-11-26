"""Unit tests for cluv/cli/init.py check functions."""

import dataclasses
import importlib
import os
import subprocess
import textwrap
import tomllib
from pathlib import Path

import pytest
import questionary

import cluv.__main__ as cluv_main
from cluv.cli.init import (
    DEFAULT_RESULTS_LINKNAME,
    DEFAULT_RESULTS_PATH,
    SCRIPTS_DIR_PATH,
    InitAnswers,
    ask_questions,
    check_cluv_config,
    check_git,
    check_home_dir,
    check_job_script,
    check_job_script_paths,
    check_symlink_to_scratch,
    default_answers,
    init,
    make_cluv_config,
)
from cluv.cli.init_utils import cluster_checks
from cluv.cli.init_utils.cluster_checks import Check, Status
from cluv.cli.init_utils.template import (
    get_cluster_name,
    get_cluster_names,
    load_cluv_config_template,
    split_sections,
)
from cluv.config import CluvConfig, load_cluv_config
from cluv.utils import console

REPO_ROOT = Path(__file__).resolve().parents[1]
CLUV_INIT_MODULE = importlib.import_module("cluv.cli.init")
JOB_SCRIPT_PATH = f"{SCRIPTS_DIR_PATH}/job.sh"
TEMPLATE = load_cluv_config_template()
TEMPLATE_CLUSTERS = get_cluster_names(TEMPLATE)
# The accounts of Mila members on each cluster, from the template.
TEMPLATE_ACCOUNTS = CLUV_INIT_MODULE._default_accounts(
    TEMPLATE, TEMPLATE_CLUSTERS, mila_access=True
)
SCRATCH_PROJECT_DIR = "$SCRATCH/repos/my_project"
HOME_PROJECT_DIR = "$HOME/code/my_project"


def parse_config(text: str) -> CluvConfig:
    """Parses a `[tool.cluv]` section. Unlike `load_cluv_config`, doesn't set local env vars."""
    return CluvConfig.model_validate(tomllib.loads(text)["tool"]["cluv"], extra="forbid")


TEMPLATE_CONFIG = parse_config(TEMPLATE)


def fake_ssh_config(
    monkeypatch: pytest.MonkeyPatch, entries: list[str], keep_alive: tuple[str, ...] = ()
) -> None:
    """Makes `ssh -G <host>` give the options of an SSH config with an entry for each host of
    `entries`, and `ServerAliveInterval` for the hosts of `keep_alive`.

    Without an entry for a host, the host name that `ssh -G` gives is the name of the host.
    """

    def get_ssh_options_for_host(host: str) -> dict[str, str]:
        return {
            "hostname": f"{host}.example.com" if host in entries else host,
            "serveraliveinterval": "120" if host in keep_alive else "0",
        }

    monkeypatch.setattr(cluster_checks, "get_ssh_options_for_host", get_ssh_options_for_host)


class TestCheckHomeDir:
    def test_not_under_home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """check_home_dir() should raise an error if the current directory is not under the user's home directory"""
        monkeypatch.setattr(
            Path, "home", lambda: str(tmp_path)
        )  # Set the home directory to tmp_path
        monkeypatch.chdir(
            tmp_path.parent
        )  # Set the current work dir to the parent of tmp_path, which is not under the "home" directory

        with pytest.raises(
            RuntimeError, match="cluv init should be run in a directory under your home directory."
        ):
            check_home_dir()


class TestGitCheck:
    def test_not_in_git_repo(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """check_git() should raise an error if the current directory is not a git repository"""
        monkeypatch.chdir(tmp_path)  # Set the working dir to tmp_path

        with pytest.raises(RuntimeError, match="Error when checking git remote: "):
            check_git()


class TestCheckCluvConfig:
    def test_add_missing_cluv_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """check_cluv_config() should add a cluv config section if the toml doesn't have it"""
        # With no cluster in ~/.ssh/config, all the clusters of the template are kept.
        fake_ssh_config(monkeypatch, [])
        p = tmp_path / "pyproject.toml"
        p.touch()

        check_cluv_config(p)
        config = load_cluv_config(p)
        expected_config = load_cluv_config(REPO_ROOT / "pyproject.toml")
        expected_results_path = f"$SCRATCH/logs/{tmp_path.name}"

        assert config.results_path == expected_results_path
        assert config.env == expected_config.env
        assert config.clusters_names == expected_config.clusters_names
        for cluster, expected_cluster_config in expected_config.clusters.items():
            if expected_cluster_config.project_dir:
                # The project goes in a folder named after the project, not after cluv (#251).
                expected_cluster_config = dataclasses.replace(
                    expected_cluster_config, project_dir=f"$SCRATCH/repos/{tmp_path.name}"
                )
            assert config.clusters[cluster] == expected_cluster_config

    def test_add_cluv_config_from_answers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """check_cluv_config(ask=True) writes the config from the answers to the questions."""
        fake_ssh_config(monkeypatch, [])
        answer_questions(
            monkeypatch,
            mila_access=False,
            clusters=["rorqual"],
            texts={"DRAC cluster rorqual?": "def-yourpi"},
        )
        p = tmp_path / "pyproject.toml"
        p.touch()

        check_cluv_config(p, ask=True)
        config = load_cluv_config(p)

        assert config.clusters_names == ["rorqual"]
        assert config.clusters["rorqual"].sbatch_args == {"account": "def-yourpi"}

    @pytest.mark.parametrize("error", [KeyboardInterrupt, EOFError])
    def test_cancel_the_questions(
        self, error: type[BaseException], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ctrl-C (KeyboardInterrupt) or Ctrl-D (EOFError) during the questions stops `cluv init`
        without writing a cluv config."""
        fake_ssh_config(monkeypatch, [])

        def cancel(*args, **kwargs):
            raise error

        monkeypatch.setattr(CLUV_INIT_MODULE, "ask_questions", cancel)
        p = tmp_path / "pyproject.toml"
        p.touch()

        with pytest.raises(SystemExit):
            check_cluv_config(p, ask=True)

        assert p.read_text() == ""

    def test_keep_existing_cluv_config(self, tmp_path: Path) -> None:
        """check_cluv_config() should not overwrite an existing cluv config"""
        p = tmp_path / "pyproject.toml"
        p.write_text(
            textwrap.dedent(
                """\
            [tool.cluv]
            clusters = {"mila" = {}}
            results_path = "results"
            """
            )
        )

        check_cluv_config(p)
        config = load_cluv_config(p)

        assert config.clusters_names == ["mila"]
        assert config.results_path == "results"


def make_config(**answers) -> tuple[str, CluvConfig]:
    """Runs make_cluv_config() with these answers. Returns the config text and the parsed config."""
    answers.setdefault("clusters", TEMPLATE_CLUSTERS)
    answers.setdefault("results_path", "$SCRATCH/logs/my_project")
    text = make_cluv_config(TEMPLATE, InitAnswers(**answers), SCRATCH_PROJECT_DIR)
    return text, parse_config(text)


def uncomment_clusters(text: str) -> str:
    """Removes one `#` from the lines of the clusters that are commented out in the config."""
    lines = []
    commented = False
    for line in text.splitlines():
        if line.startswith(("[", "# [")):
            commented = line.startswith("#")
        lines.append(line.removeprefix("#").removeprefix(" ") if commented else line)
    return "\n".join(lines)


class TestMakeCluvConfig:
    def test_template_only_sets_accounts_in_the_sbatch_args_of_clusters(self) -> None:
        """make_cluv_config() changes the accounts of a cluster by writing a new `sbatch_args` line
        with only these accounts. Any other sbatch flag in that line of the template would be
        lost."""
        for cluster, cluster_config in TEMPLATE_CONFIG.clusters.items():
            sbatch_args = cluster_config.sbatch_args
            for flags in sbatch_args if isinstance(sbatch_args, list) else [sbatch_args]:
                assert set(flags) <= {"account"}, f"Flags other than `account` on {cluster}."

    def test_template_has_a_link_under_the_header_of_each_cluster(self) -> None:
        """make_cluv_config() writes the accounts of a cluster under the header and the
        `cluster_link` line, so the template needs that line right under the header."""
        for section in split_sections(TEMPLATE):
            if get_cluster_name(section):
                assert section[1].startswith('cluster_link = "https://'), (
                    f"No link under {section[0]}."
                )

    def test_links_stay_under_the_headers(self) -> None:
        """The `cluster_link` of each cluster stays right under the header of the cluster when the
        accounts change, and when the `project_dir` of the cluster is removed."""
        accounts = {cluster: ["def-yourpi"] for cluster in TEMPLATE_CLUSTERS}
        text, config = make_config(accounts=accounts, project_dir="$SCRATCH/code/my_project")

        for cluster in TEMPLATE_CLUSTERS:
            assert f"\n[tool.cluv.clusters.{cluster}]\ncluster_link = " in text, cluster
            expected_link = TEMPLATE_CONFIG.clusters[cluster].cluster_link
            assert config.clusters[cluster].cluster_link == expected_link

    def test_unchanged_accounts_keep_the_lines_of_the_template(self) -> None:
        text, config = make_config(accounts=TEMPLATE_ACCOUNTS)

        assert config.clusters_names == TEMPLATE_CLUSTERS
        for cluster in TEMPLATE_CLUSTERS:
            expected_sbatch_args = TEMPLATE_CONFIG.clusters[cluster].sbatch_args
            assert config.clusters[cluster].sbatch_args == expected_sbatch_args
        assert "Mila doesn't have an allocation on Narval anymore." in text

    def test_changed_accounts_replace_those_of_the_template(self) -> None:
        accounts = {
            **TEMPLATE_ACCOUNTS,
            "rorqual": ["rrg-bengioy-ad", "def-yourpi"],
            "narval": [],
            "tamia": ["aip-yourpi"],
        }
        text, config = make_config(accounts=accounts)

        assert config.clusters["rorqual"].sbatch_args == [
            {"account": "rrg-bengioy-ad"},
            {"account": "def-yourpi"},
        ]
        assert config.clusters["narval"].sbatch_args == {}
        assert config.clusters["tamia"].sbatch_args == {"account": "aip-yourpi"}
        # The comment on the account of Narval goes away with that account.
        assert "allocation on Narval" not in text

    def test_no_accounts(self) -> None:
        """Without the Mila cluster, none of the accounts of Mila members stays in the config."""
        text, config = make_config(clusters=["tamia", "rorqual", "narval"])

        assert config.clusters_names == ["tamia", "rorqual", "narval"]
        assert all(cluster.sbatch_args == {} for cluster in config.clusters.values())
        assert "bengioy" not in text

    def test_clusters_that_are_not_selected_are_commented_out(self) -> None:
        """The clusters that aren't selected stay in the config, commented out, with the
        `cluster_link` of each cluster. Removing one `#` from the lines of these clusters enables
        them, with the `cluster_link`, the `env` and the `project_dir` of the template."""
        text, config = make_config(clusters=["rorqual"], accounts={"rorqual": ["def-yourpi"]})

        assert config.clusters_names == ["rorqual"]
        for cluster in TEMPLATE_CLUSTERS:
            if cluster != "rorqual":
                assert f"\n# [tool.cluv.clusters.{cluster}]\n# cluster_link = " in text, cluster
        uncommented = parse_config(uncomment_clusters(text))
        assert uncommented.clusters_names == TEMPLATE_CLUSTERS
        for cluster, template_cluster in TEMPLATE_CONFIG.clusters.items():
            assert uncommented.clusters[cluster].cluster_link == template_cluster.cluster_link
            assert uncommented.clusters[cluster].env == template_cluster.env
            expected_project_dir = SCRATCH_PROJECT_DIR if template_cluster.project_dir else None
            assert uncommented.clusters[cluster].project_dir == expected_project_dir

    @pytest.mark.parametrize(
        ("mila_access", "expected_sbatch_args"),
        [
            # The accounts of Mila members, or an example account on the clusters without any.
            (
                True,
                {
                    "mila": {},
                    "tamia": {"account": "aip-yourpi"},
                    "fir": {"account": "rrg-bengioy-ad"},
                    "narval": {"account": "def-bengioy"},
                },
            ),
            # An example account, except on the Mila cluster.
            (
                False,
                {
                    "mila": {},
                    "tamia": {"account": "aip-yourpi"},
                    "fir": {"account": "def-yourpi"},
                    "narval": {"account": "def-yourpi"},
                },
            ),
        ],
    )
    def test_accounts_of_the_clusters_that_are_not_selected(
        self, mila_access: bool, expected_sbatch_args: dict[str, dict[str, str]]
    ) -> None:
        """The clusters that aren't selected get the accounts of Mila members when the user has
        access to the Mila cluster, as in the questions. The clusters left without an account get
        an example account, except the Mila cluster, where jobs usually don't need an account."""
        text, _ = make_config(clusters=["rorqual"], mila_access=mila_access)

        config = parse_config(uncomment_clusters(text))

        sbatch_args = {
            cluster: config.clusters[cluster].sbatch_args for cluster in expected_sbatch_args
        }
        assert sbatch_args == expected_sbatch_args

    @pytest.mark.parametrize(
        "project_dir",
        ["", "$HOME/code/my_project", "${HOME}/code/my_project", "/home/me/code/my_project"],
    )
    def test_project_in_scratch_on_clusters_that_refuse_home(self, project_dir: str) -> None:
        """Unless the global `project_dir` is in $SCRATCH, the clusters that can't run jobs from
        $HOME get the project in $SCRATCH/repos (#251)."""
        _, config = make_config(project_dir=project_dir)

        for cluster in TEMPLATE_CLUSTERS:
            needs_scratch = TEMPLATE_CONFIG.clusters[cluster].project_dir is not None
            expected_project_dir = SCRATCH_PROJECT_DIR if needs_scratch else None
            assert config.clusters[cluster].project_dir == expected_project_dir

    @pytest.mark.parametrize(
        "project_dir", ["$SCRATCH/code/my_project", "${SCRATCH}/code/my_project"]
    )
    def test_no_cluster_project_dir_when_the_project_is_in_scratch(self, project_dir: str) -> None:
        """A global `project_dir` in $SCRATCH is used as is on all the clusters."""
        text, config = make_config(project_dir=project_dir)

        assert config.project_dir == project_dir
        assert all(cluster.project_dir is None for cluster in config.clusters.values())
        # The comments about the `project_dir` of these clusters are removed too.
        assert "Killarney refuses to run jobs" not in text
        assert "Keep the project on $SCRATCH instead." not in text

    def test_optional_settings(self) -> None:
        text, config = make_config()
        assert config.data_source is None
        assert config.datasets_path is None
        # The lines of the template stay commented, so they can be filled in later.
        assert "\n# data_source = " in text
        assert "\n# datasets_path = " in text

        _, config = make_config(
            data_source="mila:/network/datasets/x", datasets_path="$SCRATCH/data"
        )
        assert config.data_source == "mila:/network/datasets/x"
        assert config.datasets_path == "$SCRATCH/data"


class TestScratchProjectDir:
    def test_project_at_the_root_of_its_repo(self, tmp_path: Path) -> None:
        subprocess.run(["git", "init", "-q", "my_repo"], cwd=tmp_path, check=True)

        assert CLUV_INIT_MODULE._scratch_project_dir(tmp_path / "my_repo") == (
            "$SCRATCH/repos/my_repo"
        )

    def test_subproject(self, tmp_path: Path) -> None:
        """The path of a subproject in the repo is kept, so cluv can find the repo from it."""
        subprocess.run(["git", "init", "-q", "my_repo"], cwd=tmp_path, check=True)
        subproject = tmp_path / "my_repo" / "examples" / "my_example"
        subproject.mkdir(parents=True)

        assert CLUV_INIT_MODULE._scratch_project_dir(subproject) == (
            "$SCRATCH/repos/my_repo/examples/my_example"
        )

    def test_project_outside_of_a_git_repo(self, tmp_path: Path) -> None:
        assert CLUV_INIT_MODULE._scratch_project_dir(tmp_path) == (
            f"$SCRATCH/repos/{tmp_path.name}"
        )


class TestDefaultAnswers:
    def test_clusters_from_the_ssh_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_ssh_config(monkeypatch, ["rorqual", "mila", "github.com"])

        answers = default_answers(TEMPLATE, "my_project")

        assert answers.clusters == ["mila", "rorqual"]
        assert answers.results_path == "$SCRATCH/logs/my_project"
        # With the Mila cluster, the accounts of Mila members from the template.
        assert answers.mila_access
        assert answers.accounts == {"mila": [], "rorqual": ["rrg-bengioy-ad"]}

    def test_no_default_accounts_without_the_mila_cluster(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_ssh_config(monkeypatch, ["rorqual", "narval"])

        answers = default_answers(TEMPLATE, "my_project")

        assert not answers.mila_access
        assert answers.accounts == {"rorqual": [], "narval": []}

    def test_all_clusters_without_any_in_the_ssh_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_ssh_config(monkeypatch, [])

        answers = default_answers(TEMPLATE, "my_project")

        assert answers.clusters == TEMPLATE_CLUSTERS
        assert answers.mila_access
        assert answers.accounts == TEMPLATE_ACCOUNTS


class FakeQuestion:
    """Stands in for a questionary question: returns a fixed answer, or raises it when the answer
    is an exception (Ctrl-C raises KeyboardInterrupt, and Ctrl-D raises EOFError)."""

    def __init__(self, answer):
        self.answer = answer

    def unsafe_ask(self):
        if isinstance(self.answer, type) and issubclass(self.answer, BaseException):
            raise self.answer
        return self.answer

    async def unsafe_ask_async(self):
        return self.unsafe_ask()


def answer_questions(
    monkeypatch: pytest.MonkeyPatch,
    mila_access: bool,
    clusters: list[str],
    texts: dict[str, str],
) -> dict[str, object]:
    """Makes questionary answer `mila_access` to the yes/no question, `clusters` to the checkbox
    question, and `texts[word]` to the text questions whose message contains `word` (their default
    answer otherwise).

    Returns the questions that get asked, with their default answers. The default answer of the
    checkbox question is the list of the choices that are checked.
    """
    asked: dict[str, object] = {}

    def confirm(message: str, default: bool = True, **kwargs) -> FakeQuestion:
        asked[message] = default
        return FakeQuestion(mila_access)

    def checkbox(message: str, choices: list[questionary.Choice], **kwargs) -> FakeQuestion:
        asked[message] = [choice.title for choice in choices if choice.checked]
        return FakeQuestion(clusters)

    def text(message: str, default: str = "", **kwargs) -> FakeQuestion:
        asked[message] = default
        return FakeQuestion(next((a for word, a in texts.items() if word in message), default))

    monkeypatch.setattr(questionary, "confirm", confirm)
    monkeypatch.setattr(questionary, "checkbox", checkbox)
    monkeypatch.setattr(questionary, "text", text)
    return asked


def account_questions(asked: dict[str, object]) -> dict[str, object]:
    """The questions about the Slurm accounts among the `asked` questions, with their default
    answers. Each question is shortened to its end, such as "DRAC cluster narval?"."""
    start = "Which Slurm accounts do you use on the "
    return {
        message.removeprefix(start): default
        for message, default in asked.items()
        if message.startswith(start)
    }


class TestAskQuestions:
    defaults = InitAnswers(
        clusters=TEMPLATE_CLUSTERS,
        results_path="$SCRATCH/logs/my_project",
        mila_access=True,
        accounts=TEMPLATE_ACCOUNTS,
    )

    @pytest.mark.parametrize("mila_access", [True, False])
    def test_mila_cluster_is_selected_by_default_with_access_to_it(
        self, mila_access: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The answer to the Mila question, not its default answer, decides whether the Mila
        cluster is selected by default."""
        defaults = dataclasses.replace(
            self.defaults, clusters=["rorqual"], mila_access=not mila_access
        )
        asked = answer_questions(
            monkeypatch, mila_access=mila_access, clusters=["rorqual"], texts={}
        )

        ask_questions(TEMPLATE, defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        assert asked["Do you have access to the Mila cluster?"] == (not mila_access)
        expected_selection = ["mila", "rorqual"] if mila_access else ["rorqual"]
        assert asked["Which clusters do you want to use?"] == expected_selection

    def test_no_account_question_for_the_mila_cluster(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Jobs on the Mila cluster usually don't need an account, so no question asks for one, and
        the config has no account on the Mila cluster."""
        asked = answer_questions(
            monkeypatch, mila_access=True, clusters=["mila", "rorqual"], texts={}
        )

        answers = ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        assert list(account_questions(asked)) == ["DRAC cluster rorqual?"]
        config = parse_config(make_cluv_config(TEMPLATE, answers, SCRATCH_PROJECT_DIR))
        assert config.clusters["mila"].sbatch_args == {}

    def test_accounts_are_prefilled_with_those_of_mila_members(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With access to the Mila cluster, one question covers the clusters of the same kind that
        have the same accounts of Mila members. Narval, where these accounts differ, gets its own
        question."""
        clusters = ["mila", "tamia", "killarney", "rorqual", "fir", "narval"]
        texts = {"DRAC clusters (rorqual, fir)?": "rrg-bengioy-ad, def-yourpi"}
        asked = answer_questions(monkeypatch, mila_access=True, clusters=clusters, texts=texts)

        answers = ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        assert account_questions(asked) == {
            "PAICE clusters (tamia, killarney)?": "",
            "DRAC clusters (rorqual, fir)?": "rrg-bengioy-ad",
            "DRAC cluster narval?": "def-bengioy",
        }
        assert answers.accounts == {
            "tamia": [],
            "killarney": [],
            "rorqual": ["rrg-bengioy-ad", "def-yourpi"],
            "fir": ["rrg-bengioy-ad", "def-yourpi"],
            "narval": ["def-bengioy"],
        }

    def test_mila_accounts_are_prefilled_without_the_mila_cluster(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With access to Mila, the accounts of Mila members are suggested on the other clusters,
        even when the Mila cluster isn't selected."""
        asked = answer_questions(
            monkeypatch, mila_access=True, clusters=["rorqual", "narval"], texts={}
        )

        answers = ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        assert account_questions(asked) == {
            "DRAC cluster rorqual?": "rrg-bengioy-ad",
            "DRAC cluster narval?": "def-bengioy",
        }
        assert answers.accounts == {"rorqual": ["rrg-bengioy-ad"], "narval": ["def-bengioy"]}

    def test_accounts_are_not_prefilled_without_access_to_mila(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        texts = {
            "DRAC cluster rorqual?": "def-yourpi, rrg-yourpi-ad",
            "PAICE cluster tamia?": "aip-yourpi",
        }
        asked = answer_questions(
            monkeypatch, mila_access=False, clusters=["rorqual", "tamia"], texts=texts
        )

        answers = ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        assert account_questions(asked) == {
            "DRAC cluster rorqual?": "",
            "PAICE cluster tamia?": "",
        }
        assert answers.accounts == {
            "rorqual": ["def-yourpi", "rrg-yourpi-ad"],
            "tamia": ["aip-yourpi"],
        }

    def test_one_account_question_for_the_clusters_of_the_same_kind(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without access to the Mila cluster, one question covers the PAICE clusters, and another
        question covers the DRAC clusters. The answer to a question goes to all the clusters that
        the question covers."""
        clusters = ["tamia", "killarney", "rorqual", "fir", "narval"]
        texts = {
            "PAICE clusters (tamia, killarney)?": "aip-yourpi",
            "DRAC clusters (rorqual, fir, narval)?": "def-yourpi",
        }
        asked = answer_questions(monkeypatch, mila_access=False, clusters=clusters, texts=texts)

        answers = ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        assert list(account_questions(asked)) == [
            "PAICE clusters (tamia, killarney)?",
            "DRAC clusters (rorqual, fir, narval)?",
        ]
        assert answers.accounts == {
            "tamia": ["aip-yourpi"],
            "killarney": ["aip-yourpi"],
            "rorqual": ["def-yourpi"],
            "fir": ["def-yourpi"],
            "narval": ["def-yourpi"],
        }

    def test_datasets_path_is_only_asked_with_a_data_source(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        asked = answer_questions(monkeypatch, mila_access=True, clusters=["mila"], texts={})
        answers = ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)
        assert answers.datasets_path == ""
        assert not any("datasets go" in message for message in asked)

        texts = {"copy": "mila:/network/datasets/x", "datasets go": "$SCRATCH/data"}
        answer_questions(monkeypatch, mila_access=True, clusters=["mila"], texts=texts)
        answers = ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)
        assert answers.data_source == "mila:/network/datasets/x"
        assert answers.datasets_path == "$SCRATCH/data"

    def test_path_questions_refuse_tilde(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """cluv replaces the $VARIABLES in paths, but not `~`. So the questions about paths refuse
        `~`. The paths of the results and of the datasets are also required."""
        answer_questions(monkeypatch, mila_access=True, clusters=["mila"], texts={"copy": "/data"})
        fake_text = questionary.text
        validators = {}

        def text(message: str, **kwargs) -> FakeQuestion:
            validators[message] = kwargs.get("validate")
            return fake_text(message, **kwargs)

        monkeypatch.setattr(questionary, "text", text)

        ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        path_questions = [message for message in validators if message.startswith("Where")]
        assert len(path_questions) == 4
        required = {
            "Where should the job results go on the clusters?",
            "Where should the datasets go on the clusters?",
        }
        assert required <= set(path_questions)
        for message in path_questions:
            validate = validators[message]
            assert validate("~/data") == "Write $HOME instead of ~.", message
            assert validate("mila:~/data") == "Write $HOME instead of ~.", message
            assert validate("$HOME/data") is True, message
            expected = "This value is required." if message in required else True
            assert validate("") == expected, message

    def test_project_question_shows_where_the_project_goes_by_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without `project_dir`, the project goes in the same path relative to $HOME as on this
        machine. The question about `project_dir` shows that path as a placeholder."""
        answer_questions(monkeypatch, mila_access=True, clusters=["mila"], texts={})
        fake_text = questionary.text
        placeholders = {}

        def text(message: str, **kwargs) -> FakeQuestion:
            placeholders[message] = kwargs.get("placeholder")
            return fake_text(message, **kwargs)

        monkeypatch.setattr(questionary, "text", text)

        ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        placeholder = placeholders["Where should the project go on the clusters?"]
        assert [part for _style, part in placeholder] == [HOME_PROJECT_DIR]

    @pytest.mark.parametrize(
        ("clusters", "expected_note"),
        [
            (
                ["mila", "killarney", "rorqual", "trillium"],
                "Since killarney and trillium can't run jobs from $HOME, the project goes in "
                f"{SCRATCH_PROJECT_DIR} there, unless your answer is a path in $SCRATCH.",
            ),
            (["mila", "rorqual"], ""),
        ],
    )
    def test_note_on_the_clusters_that_refuse_home(
        self, clusters: list[str], expected_note: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Before the question about `project_dir`, a note says where the project goes on the
        selected clusters that can't run jobs from $HOME. Without these clusters, there's no note."""
        answer_questions(monkeypatch, mila_access=True, clusters=clusters, texts={})

        with console.capture() as capture:
            ask_questions(TEMPLATE, self.defaults, HOME_PROJECT_DIR, SCRATCH_PROJECT_DIR)

        # Rich wraps the long lines, so the spaces are normalized.
        assert " ".join(capture.get().split()) == expected_note


@pytest.mark.parametrize(
    ("cluster", "example"),
    [
        ("rorqual", "def-yourpi"),
        ("narval", "def-yourpi"),
        ("tamia", "aip-yourpi"),
        ("killarney", "aip-yourpi"),
    ],
)
def test_accounts_instruction_gives_an_example(cluster: str, example: str) -> None:
    """The question about the accounts shows a DRAC or PAICE account name."""
    instruction = CLUV_INIT_MODULE._accounts_instruction(cluster)

    assert f"(e.g. {example}; " in instruction


@pytest.mark.parametrize("quiet", [True, False])
def test_init_cli_passes_quiet(
    quiet: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`cluv init -q` passes quiet=True to init(), which then asks no question."""
    init_calls = []
    monkeypatch.setattr(cluv_main, "init", lambda **kwargs: init_calls.append(kwargs))
    # main() sets `console.quiet`: setting it here makes monkeypatch restore it after the test.
    monkeypatch.setattr(cluv_main.console, "quiet", False)

    cluv_main.main(["init", str(tmp_path), *(["-q"] if quiet else [])])

    assert init_calls == [{"path": tmp_path, "quiet": quiet}]


class TestSymlinkCheck:
    def test_no_symlink_if_scratch_not_set(self, tmp_path, monkeypatch) -> None:
        """check_symlink_to_scratch() should not create a symlink if the $SCRATCH env var is not set"""
        monkeypatch.delenv("SCRATCH", raising=False)

        check_symlink_to_scratch(tmp_path, DEFAULT_RESULTS_PATH, DEFAULT_RESULTS_LINKNAME)

        assert not (tmp_path / DEFAULT_RESULTS_PATH).exists()

    def test_create_symlink(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """check_symlink_to_scratch() should create a symlink from results_symlink to results_path"""
        scratch_path = tmp_path / "scratch"
        monkeypatch.setenv("SCRATCH", str(scratch_path))
        fake_project_root = tmp_path

        expected_results_symlink = fake_project_root / DEFAULT_RESULTS_LINKNAME
        expected_results_scratch_path = Path(os.path.expandvars(DEFAULT_RESULTS_PATH))

        check_symlink_to_scratch(
            project_path=fake_project_root,
            results_path=DEFAULT_RESULTS_PATH,
            results_symlink=DEFAULT_RESULTS_LINKNAME,
        )

        assert expected_results_symlink.exists()
        assert expected_results_symlink.is_symlink()
        assert expected_results_scratch_path.exists()
        assert expected_results_symlink.resolve() == expected_results_scratch_path.resolve()

    def test_keep_existing_symlink(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """check_symlink_to_scratch() should not overwrite an existing symlink not pointing to scratch"""
        scratch_path = tmp_path / "scratch"
        monkeypatch.setenv("SCRATCH", str(scratch_path))
        fake_project_root = tmp_path
        expected_results_symlink = fake_project_root / DEFAULT_RESULTS_LINKNAME
        expected_results_scratch_path = Path(os.path.expandvars(DEFAULT_RESULTS_PATH))
        expected_results_symlink.symlink_to(
            tmp_path / "some_other_folder"
        )  # Create a symlink pointing to a new location

        check_symlink_to_scratch(fake_project_root, DEFAULT_RESULTS_PATH, DEFAULT_RESULTS_LINKNAME)

        # The original symlink should be kept, and not changed to point to scratch
        assert expected_results_symlink.is_symlink()
        assert expected_results_symlink.resolve() == (tmp_path / "some_other_folder").resolve()
        assert not expected_results_scratch_path.exists()

    def test_keep_existing_results_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """check_symlink_to_scratch() should not replace an existing results directory."""
        scratch_path = tmp_path / "scratch"
        monkeypatch.setenv("SCRATCH", str(scratch_path))
        results_directory = tmp_path / DEFAULT_RESULTS_LINKNAME
        results_directory.mkdir()

        check_symlink_to_scratch(tmp_path, DEFAULT_RESULTS_PATH, DEFAULT_RESULTS_LINKNAME)

        assert results_directory.is_dir()
        assert not results_directory.is_symlink()
        assert not Path(os.path.expandvars(DEFAULT_RESULTS_PATH)).exists()


class TestJobScriptCheck:
    def test_keep_existing_job_script(self, tmp_path: Path) -> None:
        """check_job_script() should not overwrite an existing job script"""
        job_script_path = tmp_path / JOB_SCRIPT_PATH
        job_script_path.parent.mkdir(exist_ok=True)
        job_script_path.write_text("#!/bin/bash\necho 'Hello world!'")

        check_job_script(tmp_path, DEFAULT_RESULTS_PATH)

        assert job_script_path.exists()
        assert job_script_path.read_text() == "#!/bin/bash\necho 'Hello world!'"

    def test_create_missing_job_scripts_from_templates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_home = tmp_path / "home"
        project_root = fake_home / "my_project"
        project_root.mkdir(parents=True)
        monkeypatch.setattr(Path, "home", lambda: fake_home)

        check_job_script(project_root, "outputs")

        job_script = project_root / "scripts" / "job.sh"
        safe_job_script = project_root / "scripts" / "safe_job.sh"

        assert job_script.exists()
        assert safe_job_script.exists()
        # assert "#SBATCH --output=outputs/%j/slurm-%j.out" in job_script.read_text()

        safe_job_script_content = safe_job_script.read_text()
        assert 'project_name="my_project"' in safe_job_script_content
        assert 'project_root="$HOME/my_project"' in safe_job_script_content
        assert 'results_path="outputs"' in safe_job_script_content
        assert "results_dir" not in safe_job_script_content
        assert "mkdir -p $project_root_in_tmpdir/$results_path" in safe_job_script_content
        assert (
            "rsync --update --recursive $project_root/$results_path/$SLURM_JOB_ID "
            "$project_root_in_tmpdir/$results_path/"
        ) in safe_job_script_content
        assert (
            "rsync --update --recursive $project_root_in_tmpdir/$results_path/$SLURM_JOB_ID "
            "$project_root/$results_path/"
        ) in safe_job_script_content

    def test_replace_results_dir_from_legacy_template(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_home = tmp_path / "home"
        project_root = fake_home / "my_project"
        project_root.mkdir(parents=True)
        monkeypatch.setattr(Path, "home", lambda: fake_home)

        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        legacy_script = templates_dir / "legacy_job.sh"
        legacy_script.write_text(
            textwrap.dedent(
                """\
                #!/bin/bash
                #SBATCH --output=logs/%j/slurm-%j.out
                results_dir="logs"
                echo "Using $results_dir"
                """
            )
        )
        monkeypatch.setattr(CLUV_INIT_MODULE, "_get_script_templates_path", lambda: templates_dir)

        check_job_script(project_root, "outputs")

        generated_legacy_script = project_root / "scripts" / "legacy_job.sh"
        assert generated_legacy_script.exists()
        generated_legacy_script_content = generated_legacy_script.read_text()
        assert "#SBATCH --output=outputs/%j/slurm-%j.out" in generated_legacy_script_content
        assert 'results_path="outputs"' in generated_legacy_script_content
        assert "Using $results_path" in generated_legacy_script_content
        assert "results_dir" not in generated_legacy_script_content


def make_cluv_config_with_clusters(**clusters: dict[str, str]) -> CluvConfig:
    """A cluv config with these clusters, with `scripts/job.sh` as default job script."""
    return CluvConfig.model_validate(
        {"results_path": "$SCRATCH/logs", "job_script_path": JOB_SCRIPT_PATH, "clusters": clusters}
    )


class TestCheckJobScriptPaths:
    def test_job_scripts_of_the_config(self, tmp_path: Path) -> None:
        """The default job script of each cluster has to be in the project. Each job script is
        checked once."""
        (tmp_path / JOB_SCRIPT_PATH).parent.mkdir()
        (tmp_path / JOB_SCRIPT_PATH).touch()
        config = make_cluv_config_with_clusters(
            mila={}, narval={}, rorqual={"job_script_path": "scripts/rorqual_job.sh"}
        )

        with console.capture() as capture:
            check_job_script_paths(tmp_path, config)

        assert " ".join(capture.get().split()) == (
            "✅ Job script of the config found: scripts/job.sh "
            "❌ Job script of the config not found: scripts/rorqual_job.sh. "
            "Fix `job_script_path` in pyproject.toml."
        )

    def test_no_job_script_path(self, tmp_path: Path) -> None:
        config = CluvConfig.model_validate({"results_path": "logs", "clusters": {"mila": {}}})

        with console.capture() as capture:
            check_job_script_paths(tmp_path, config)

        assert capture.get() == ""


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (Status.OK, "Your cluv config is ready to go !"),
        (
            Status.FAIL,
            "Some clusters have problems: fix them (see above), then run cluv init again to "
            "check them.",
        ),
    ],
)
def test_message_after_the_checks(
    status: Status, message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`cluv init` says that the config is ready only when no cluster has a problem."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SCRATCH", raising=False)
    (tmp_path / "pyproject.toml").write_text(
        '[tool.cluv]\nresults_path = "logs"\n\n[tool.cluv.clusters.rorqual]\n'
    )
    monkeypatch.setattr(CLUV_INIT_MODULE, "check_git", lambda: None)

    async def fake_check_clusters(*args, **kwargs) -> dict[str, list[Check]]:
        return {"rorqual": [Check(status, "message")]}

    monkeypatch.setattr(CLUV_INIT_MODULE, "check_clusters", fake_check_clusters)

    with console.capture() as capture:
        init(quiet=True)

    assert message in " ".join(capture.get().split())
