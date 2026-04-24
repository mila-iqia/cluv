def write_file(tmp_path, content: str, filename: str):
    p = tmp_path / filename
    p.write_text(content)
    return p
