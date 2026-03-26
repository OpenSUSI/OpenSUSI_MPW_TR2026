#!/usr/bin/env python3
# ----- ------ ----- ----- ------ ----- ----- ------ -----
# OpenSUSI jun1okamura <jun1okamura@gmail.com>
# LICENSE: Apache License Version 2.0
# ----- ------ ----- ----- ------ ----- ----- ------ -----

import argparse
import re
from pathlib import Path

import klayout.db as pya


def normalize_name(value: str) -> str:
    s = str(value or "").strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def extract_repo_name(source_repo: str) -> str:
    value = str(source_repo or "").strip()
    parts = value.split("/")
    if len(parts) >= 2 and parts[-1]:
        return parts[-1]
    return value or "unknown"


def build_top_cell_name(github_id: str, source_repo: str) -> str:
    gid = normalize_name(github_id)
    repo = normalize_name(extract_repo_name(source_repo))
    return f"tr_1um_{gid}_{repo}"[:64]


def get_single_top_cell(layout: pya.Layout, source: Path) -> pya.Cell:
    tops = list(layout.top_cells())
    if len(tops) != 1:
        names = [cell.name for cell in tops]
        raise RuntimeError(
            f"GDS must have exactly one top cell: {source}, top_cells={names}"
        )
    return tops[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rename top cell in GDSII to OpenSUSI naming rule."
    )
    parser.add_argument("--gds", type=Path, required=True)
    parser.add_argument("--github-id", required=True)
    parser.add_argument("--source-repo", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.gds.exists():
        raise FileNotFoundError(f"GDS not found: {args.gds}")

    layout = pya.Layout()
    layout.read(str(args.gds))

    top = get_single_top_cell(layout, args.gds)
    new_name = build_top_cell_name(args.github_id, args.source_repo)

    old_name = top.name
    if old_name != new_name:
        print(f"rename top cell: {old_name} -> {new_name}")
        top.name = new_name
        layout.write(str(args.gds))
    else:
        print(f"top cell already matches: {new_name}")


if __name__ == "__main__":
    main()