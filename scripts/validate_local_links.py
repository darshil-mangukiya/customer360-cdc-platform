from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


def validate(root: Path = ROOT) -> tuple[int, list[str]]:
    markdown = [root / "README.md", *sorted((root / "docs").rglob("*.md")), *sorted((root / "migration").rglob("*.md"))]
    checked = 0
    broken: list[str] = []
    for path in markdown:
        if not path.exists() or "project_master_report" in path.parts:
            continue
        for raw_target in LINK.findall(path.read_text(encoding="utf-8", errors="ignore")):
            target = raw_target.strip().split()[0].strip("<>")
            if not target or target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            local_target = unquote(target.split("#", 1)[0])
            if not local_target:
                continue
            checked += 1
            if not (path.parent / local_target).resolve().exists():
                broken.append(f"{path.relative_to(root)} -> {target}")
    return checked, broken


def main() -> None:
    checked, broken = validate()
    print(f"local_links_checked={checked} broken_links={len(broken)}")
    for item in broken:
        print(item)
    if broken:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
