from pathlib import Path

import pytest


@pytest.fixture
def fake_scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Fixture to set a fake SCRATCH environment variable if it's not already set."""
    fake_scratch = tmp_path / "scratch"
    fake_scratch.mkdir()
    monkeypatch.setenv("SCRATCH", str(fake_scratch))
    return fake_scratch
