from pathlib import Path

def write_file(tmp_path: Path, content: str) -> Path:
    p = tmp_path
    p.write_text(content)
    return p
