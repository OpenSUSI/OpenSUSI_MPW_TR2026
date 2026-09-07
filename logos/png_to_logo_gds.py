#!/usr/bin/env python3
# ----- ------ ----- ----- ------ ----- ----- ------ -----
# OpenSUSI jun1okamura <jun1okamura@gmail.com>
# LICENSE: Apache License Version 2.0
# ----- ------ ----- ----- ------ ----- ----- ------ -----
"""
Convert a logo PNG into a dot-matrix logo GDSII.

Pipeline:
  1. Load the PNG and binarize it to pure black/white (BINARIZE_THRESHOLD).
  2. Find the tight bounding box of "ink" pixels in the binarized image
     (i.e. crop away any surrounding blank margin in the source PNG --
     the fit in the next step is sized to the actual artwork, not to
     the PNG canvas, which may include padding).
  3. Fit that content's aspect ratio to a dot grid as large as possible
     without exceeding the bbox limit (BBOX_X_UM x BBOX_Y_UM), similar
     to CSS "object-fit: contain" -- the content's aspect ratio is
     preserved, never stretched, and the grid only shrinks below the
     bbox on whichever axis the aspect ratio forces.
  4. Downsample the cropped binarized content onto that grid, where
     each cell's value is the local fraction of "ink" pixels inside it.
  5. Any cell whose ink coverage is >= INK_THRESHOLD gets a single
     DOT_UM x DOT_UM square dot on GDS_LAYER/GDS_DATATYPE, laid out on a
     PITCH_UM (= DOT_UM + SPACE_UM) grid.
  6. The layout origin (0, 0) is the center of the bbox, matching the
     placement convention used elsewhere in this repo (aggregate_gds.py
     inserts the logo cell at a fixed x/y from info.yaml's
     logo.placements, so the cell's own origin must be its visual
     center). If the fitted grid is smaller than the bbox on one axis,
     it is still centered at (0, 0) -- i.e. letterboxed within the
     declared bbox, not pinned to a corner.

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

DOT_UM = 3.0
SPACE_UM = 2.0
PITCH_UM = DOT_UM + SPACE_UM  # 5.0 um center-to-center

# M2 layer/datatype for this PDK.
GDS_LAYER = 20
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


def max_grid_size() -> tuple[int, int]:
    """Upper bound on grid cells that fit inside the bbox at PITCH_UM."""
    max_cols = int(BBOX_X_UM // PITCH_UM)
    max_rows = int(BBOX_Y_UM // PITCH_UM)

    if max_cols < 1 or max_rows < 1:
        raise ValueError(
            f"BBOX ({BBOX_X_UM}x{BBOX_Y_UM} um) is too small for "
            f"PITCH_UM={PITCH_UM}"
        )

    return max_cols, max_rows


def binarize_image(png_path: Path, invert: bool) -> Image.Image:
    """
    白黒化 (binarize) the PNG to pure black/white. Darker-than-threshold
    pixels become "ink" (255); everything else becomes background (0).
    """
    im = Image.open(png_path).convert("L")
    bw = im.point(lambda p: 255 if p < BINARIZE_THRESHOLD else 0)

    if invert:
        bw = bw.point(lambda p: 255 - p)

    return bw


def content_bbox(bw: Image.Image) -> tuple[int, int, int, int]:
    """
    Tight pixel bounding box of the "ink" (non-zero) region of a
    binarized image, i.e. the actual artwork extent with any blank
    canvas margin cropped away.
    """
    bbox = bw.getbbox()

    if bbox is None:
        raise ValueError(
            "No ink pixels found after binarization -- is the source "
            "PNG blank, or does it need --invert?"
        )

    return bbox


def fit_grid_to_aspect(content_w: int, content_h: int) -> tuple[int, int]:
    """
    Compute the largest (cols, rows) grid that preserves the content's
    aspect ratio without exceeding the bbox's max grid size in either
    dimension (an integer-grid analogue of "object-fit: contain"). This
    is sized to the content bbox, not the full PNG canvas, so any blank
    margin in the source image doesn't shrink the resulting logo.
    """
    max_cols, max_rows = max_grid_size()
    aspect = content_w / content_h  # width / height

    # Try height-constrained first (use the full row budget).
    cols = round(max_rows * aspect)
    rows = max_rows

    if cols > max_cols:
        # Height-constrained fit is too wide; fall back to width-constrained.
        cols = max_cols
        rows = round(max_cols / aspect)

    cols = max(1, min(cols, max_cols))
    rows = max(1, min(rows, max_rows))

    return cols, rows


def load_dot_grid(
    bw: Image.Image,
    bbox: tuple[int, int, int, int],
    cols: int,
    rows: int,
) -> np.ndarray:
    """
    Crop the binarized image to its content bbox, then downsample onto a
    cols x rows grid of per-cell ink coverage, and threshold that into
    an on/off dot grid. Returns a bool array of shape (rows, cols); True
    = dot on. Row 0 is the top of the content; col 0 is the left edge.
    """
    cropped = bw.crop(bbox)

    # Downsample the cropped binary content onto the dot grid; each
    # output pixel becomes the local average ink coverage (0..255) of
    # the region it was downsampled from. cols/rows were already chosen
    # (by fit_grid_to_aspect) to match the content's own aspect ratio,
    # so this is a uniform scale in both axes, not a stretch.
    small = cropped.resize((cols, rows), Image.BOX)
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

    # Actual footprint of the fitted grid (<= bbox in each axis).
    grid_w_um = cols * PITCH_UM
    grid_h_um = rows * PITCH_UM

    # Origin (0, 0) = center of the bbox. The fitted grid is centered on
    # this origin too, so if it's smaller than the bbox on one axis
    # (aspect-ratio letterboxing), the margin is split evenly on both
    # sides rather than pinned to a corner. Row 0 (top of the source
    # image) maps to the top edge of the grid (+Y), since GDS Y
    # increases upward while image rows increase downward.
    origin_x_um = -grid_w_um / 2.0
    origin_y_um = grid_h_um / 2.0

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

    max_cols, max_rows = max_grid_size()
    print(f"top cell   : {top_cell_name}")
    print(f"layer      : {GDS_LAYER}/{GDS_DATATYPE}")
    print(f"bbox limit : {BBOX_X_UM} x {BBOX_Y_UM} um ({max_cols} x {max_rows} cells max)")
    print(f"fitted grid: {cols} x {rows} cells (pitch {PITCH_UM} um) -> {grid_w_um} x {grid_h_um} um")
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

    bw = binarize_image(args.input, args.invert)
    bbox = content_bbox(bw)
    left, top, right, bottom = bbox
    content_w = right - left
    content_h = bottom - top

    cols, rows = fit_grid_to_aspect(content_w, content_h)
    grid = load_dot_grid(bw, bbox, cols, rows)
    build_gds(grid, args.output, args.top_cell)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
