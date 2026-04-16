def write_pyproject(tmp_path, content: str):
    p = tmp_path / "pyproject.toml"
    p.write_text(content)
    return p
