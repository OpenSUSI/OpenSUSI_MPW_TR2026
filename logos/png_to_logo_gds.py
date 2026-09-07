#!/usr/bin/env python3
# ----- ------ ----- ----- ------ ----- ----- ------ -----
# OpenSUSI jun1okamura <jun1okamura@gmail.com>
# LICENSE: Apache License Version 2.0
# ----- ------ ----- ----- ------ ----- ----- ------ -----
"""
Convert a logo PNG into a dot-matrix logo GDSII.

Pipeline:
  1. Load the PNG and binarize it to pure black/white (BINARIZE_THRESHOLD).
  2. Downsample the binarized image onto a regular dot grid sized to fit
     the logo bbox (BBOX_X_UM x BBOX_Y_UM), where each grid cell's value
     is the local fraction of "ink" pixels inside it.
  3. Any cell whose ink coverage is >= INK_THRESHOLD gets a single
     DOT_UM x DOT_UM square dot on GDS_LAYER/GDS_DATATYPE, laid out on a
     PITCH_UM (= DOT_UM + SPACE_UM) grid.
  4. The layout origin (0, 0) is the center of the bbox, matching the
     placement convention used elsewhere in this repo (aggregate_gds.py
     inserts the logo cell at a fixed x/y from info.yaml's
     logo.placements, so the cell's own origin must be its visual center).

Usage:
    python3 png_to_logo_gds.py --input logo.png --output logo.gds \
        [--top-cell RISE_A_LOGO] [--invert]

Requires: Pillow, numpy, klayout (klayout.db)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
from PIL import Image
import klayout.db as pya


# ----------------------------------------------------------------------
# Fixed design parameters. Edit these constants directly (not via CLI)
# to match info.yaml's logo.bbox and the target metal layer's design
# rules. Keeping them here (rather than as CLI flags) avoids a bad value
# being passed by accident at call time for what is effectively a
# per-project physical design constant.
# ----------------------------------------------------------------------
BBOX_X_UM = 320.0
BBOX_Y_UM = 160.0

DOT_UM   = 3.0
SPACE_UM = 2.0
PITCH_UM = DOT_UM + SPACE_UM  # 5.0 um center-to-center

# TODO: set to the actual M2 layer/datatype number for your PDK before
# running this for real. These placeholder values are almost certainly
# wrong for your process.
GDS_LAYER    = 20
GDS_DATATYPE = 0

# Pixel binarization cutoff (0-255, PIL "L" grayscale). Pixels darker
# than this are treated as "ink"; everything else is background.
BINARIZE_THRESHOLD = 128

# Per-cell coverage cutoff (0.0-1.0). A dot is drawn if the fraction of
# binarized "ink" pixels inside that grid cell's source-image region is
# at least this value. Tuned low (rather than 0.5) so that thin strokes
# in a wordmark/line-art logo stay legible at a coarse dot pitch instead
# of only lighting at stroke intersections.
INK_THRESHOLD = 0.15

DEFAULT_TOP_CELL = "LOGO"

DBU_UM = 0.001  # 1 database unit = 1 nm, matches layout.dbu elsewhere in this repo


def compute_grid_size() -> tuple[int, int]:
    cols = BBOX_X_UM / PITCH_UM
    rows = BBOX_Y_UM / PITCH_UM

    if abs(cols - round(cols)) > 1e-6 or abs(rows - round(rows)) > 1e-6:
        raise ValueError(
            "BBOX size must be an integer multiple of PITCH_UM "
            f"(DOT_UM+SPACE_UM={PITCH_UM}): "
            f"got {BBOX_X_UM}x{BBOX_Y_UM} -> {cols}x{rows} cells"
        )

    return int(round(cols)), int(round(rows))


def load_dot_grid(png_path: Path, cols: int, rows: int, invert: bool) -> np.ndarray:
    """
    Binarize the PNG to black/white, then downsample onto a cols x rows
    grid of per-cell ink coverage, and threshold that into an on/off dot
    grid. Returns a bool array of shape (rows, cols); True = dot on.
    Row 0 is the top of the source image; col 0 is the left edge.
    """
    im = Image.open(png_path).convert("L")

    # Step 1: 白黒化 (binarize). Darker-than-threshold pixels -> "ink"
    # (255), everything else -> background (0), so the BOX-filter resize
    # below produces a clean per-cell ink-coverage average rather than
    # picking up anti-aliasing gray levels directly.
    bw = im.point(lambda p: 255 if p < BINARIZE_THRESHOLD else 0)

    if invert:
        bw = bw.point(lambda p: 255 - p)

    # Step 2: downsample the binary image onto the dot grid; each output
    # pixel becomes the local average ink coverage (0..255) of the
    # region it was downsampled from.
    small = bw.resize((cols, rows), Image.BOX)
    coverage = np.asarray(small, dtype=np.float64) / 255.0

    return coverage >= INK_THRESHOLD


def build_gds(grid: np.ndarray, output_path: Path, top_cell_name: str) -> None:
    rows, cols = grid.shape

    layout = pya.Layout()
    layout.dbu = DBU_UM
    top = layout.create_cell(top_cell_name)
    layer_index = layout.layer(GDS_LAYER, GDS_DATATYPE)

    dbu = layout.dbu
    half_dot_dbu = int(round((DOT_UM / 2.0) / dbu))

    # Origin (0, 0) = center of the bbox. Row 0 (top of the source image)
    # maps to the top edge of the bbox (+Y), since GDS Y increases upward
    # while image rows increase downward.
    origin_x_um = -BBOX_X_UM / 2.0
    origin_y_um = BBOX_Y_UM / 2.0

    dot_count = 0

    for r in range(rows):
        cy_um = origin_y_um - (r * PITCH_UM + PITCH_UM / 2.0)
        cy_dbu = int(round(cy_um / dbu))

        for c in range(cols):
            if not grid[r, c]:
                continue

            cx_um = origin_x_um + (c * PITCH_UM + PITCH_UM / 2.0)
            cx_dbu = int(round(cx_um / dbu))

            box = pya.Box(
                cx_dbu - half_dot_dbu,
                cy_dbu - half_dot_dbu,
                cx_dbu + half_dot_dbu,
                cy_dbu + half_dot_dbu,
            )
            top.shapes(layer_index).insert(box)
            dot_count += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    layout.write(str(output_path))

    print(f"top cell   : {top_cell_name}")
    print(f"layer      : {GDS_LAYER}/{GDS_DATATYPE}")
    print(f"bbox       : {BBOX_X_UM} x {BBOX_Y_UM} um")
    print(f"grid       : {cols} x {rows} cells (pitch {PITCH_UM} um)")
    print(f"dots drawn : {dot_count} / {cols * rows}")
    print(f"output     : {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a logo PNG into a dot-matrix logo GDSII."
    )

    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Source PNG file.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output GDS file path.",
    )
    parser.add_argument(
        "--top-cell",
        default=DEFAULT_TOP_CELL,
        help=f"Top cell name written into the GDS. Default: {DEFAULT_TOP_CELL}",
    )
    parser.add_argument(
        "--invert",
        action="store_true",
        help="Invert ink/background (use if the source PNG is light-on-dark).",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.input.exists():
        print(f"ERROR: input PNG not found: {args.input}", file=sys.stderr)
        return 1

    cols, rows = compute_grid_size()
    grid = load_dot_grid(args.input, cols, rows, args.invert)
    build_gds(grid, args.output, args.top_cell)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
