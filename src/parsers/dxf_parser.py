"""
DXF Parser — extracts geometric data from DXF files using ezdxf.
Returns a DrawingData dict compatible with the QTO engine.

Layer recognition is intentionally broad: both standard AIA/CAD naming
(A-WALL, A-DOOR, A-GLAZ, A-COLS …) and the common Arabic/Gulf practice of
free-form layer names that contain keywords are supported.
"""

from __future__ import annotations

import math
import re
from typing import Any

try:
    import ezdxf
    from ezdxf.math import Vec3
    EZDXF_AVAILABLE = True
except ImportError:
    EZDXF_AVAILABLE = False


# ---------------------------------------------------------------------------
# Default dimension constants
# ---------------------------------------------------------------------------

DEFAULT_DOOR_HEIGHT: float = 2.10
DEFAULT_WINDOW_HEIGHT: float = 1.50
DEFAULT_FLOOR_HEIGHT: float = 3.00
DEFAULT_SLAB_THICKNESS: float = 0.20
DEFAULT_WALL_THICKNESS_EXT: float = 0.25
DEFAULT_WALL_THICKNESS_INT20: float = 0.20
DEFAULT_WALL_THICKNESS_INT10: float = 0.10


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _polyline_length(points: list[tuple[float, float]]) -> float:
    total = 0.0
    for i in range(len(points) - 1):
        dx = points[i + 1][0] - points[i][0]
        dy = points[i + 1][1] - points[i][1]
        total += math.hypot(dx, dy)
    return total


def _closed_polyline_perimeter(points: list[tuple[float, float]]) -> float:
    if not points:
        return 0.0
    return _polyline_length(list(points) + [points[0]])


def _polygon_area(points: list[tuple[float, float]]) -> float:
    n = len(points)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += points[i][0] * points[j][1]
        area -= points[j][0] * points[i][1]
    return abs(area) / 2.0


def _entity_to_points(entity: Any) -> list[tuple[float, float]]:
    dxftype = entity.dxftype()
    if dxftype == "LWPOLYLINE":
        return [(pt[0], pt[1]) for pt in entity.get_points()]
    if dxftype == "POLYLINE":
        return [(v.dxf.location.x, v.dxf.location.y) for v in entity.vertices]
    if dxftype == "LINE":
        s, e = entity.dxf.start, entity.dxf.end
        return [(s.x, s.y), (e.x, e.y)]
    if dxftype in ("CIRCLE", "ARC"):
        cx, cy = entity.dxf.center.x, entity.dxf.center.y
        r = entity.dxf.radius
        return [(cx - r, cy - r), (cx + r, cy - r), (cx + r, cy + r), (cx - r, cy + r)]
    return []


def _bounding_box(all_points: list[tuple[float, float]]) -> dict:
    if not all_points:
        return {"min_x": 0, "max_x": 0, "min_y": 0, "max_y": 0, "width": 0, "height": 0}
    xs = [p[0] for p in all_points]
    ys = [p[1] for p in all_points]
    return {
        "min_x": min(xs), "max_x": max(xs),
        "min_y": min(ys), "max_y": max(ys),
        "width": max(xs) - min(xs),
        "height": max(ys) - min(ys),
    }


def _scale_from_bbox(bbox: dict) -> float:
    """Guess drawing scale from bounding box; returns 1.0 if plausible metres."""
    w, h = bbox["width"], bbox["height"]
    if w == 0 or h == 0:
        return 1.0
    # If the longest dimension is >500 assume millimetres
    if max(w, h) > 500:
        return 0.001
    return 1.0


# ---------------------------------------------------------------------------
# Layer-name matchers  (case-insensitive keyword sets)
# ---------------------------------------------------------------------------

# Each tuple: (keyword_fragments_ANY_match)
# A layer matches if ANY fragment is a substring of the lower-case layer name.

_WALL_EXT_FRAGS = (
    "a-wall-ext", "a-ext-wall", "ext-wall", "extwall", "external-wall",
    "externalwall", "ext_wall", "boundary", "plot", "perimeter",
    "outer wall", "outer-wall",
)
_WALL_INT20_FRAGS = (
    "a-wall-int", "int-wall-20", "intwall20", "internal-wall-20",
    "iwall20", "wall-20", "wall20", "structural-partition",
)
_WALL_INT10_FRAGS = (
    "int-wall-10", "intwall10", "internal-wall-10", "partition-10",
    "iwall10", "wall-10", "wall10", "partition",
)
_WALL_GENERIC_FRAGS = (
    "a-wall", "wall", "parti",
)

_COLUMN_FRAGS = (
    "a-col", "a-cols", "column", "col-", "cols", "pillar", "post",
    "struct-col", "structural-col",
)
_BEAM_FRAGS = (
    "a-beam", "beam", "rcc-beam", "conc-beam", "strut",
)
_SLAB_FRAGS = (
    "a-slab", "slab", "floor-plate", "floorplate", "deck",
)
_DOOR_FRAGS = (
    "a-door", "a-dor", "door", "-dr-", "_dr_", "dr-",
)
_WINDOW_FRAGS = (
    "a-glaz", "a-win", "window", "glazing", "glaz", "-win-", "_win_",
)
_OPENING_FRAGS = _DOOR_FRAGS + _WINDOW_FRAGS + ("opening", "a-open")
_ROOM_FRAGS = (
    "a-area", "a-room", "room", "space", "area", "zone",
    "floor-area", "floorarea",
)
_FOUNDATION_FRAGS = (
    "a-fnd", "foundation", "footing", "fndn", "pad", "raft",
)
_STAIR_FRAGS = (
    "a-stair", "stair", "steps", "riser",
)
_ROOF_FRAGS = (
    "a-roof", "roof", "terrace", "top-slab",
)
_BOUNDARY_FRAGS = (
    "plot", "boundary", "site", "land",
)
_DIMENSION_FRAGS = (
    "a-dims", "dim", "dimension", "anno",
)


def _layer_has(layer_name: str, frags: tuple[str, ...]) -> bool:
    ln = layer_name.lower()
    return any(f in ln for f in frags)


def _wall_type(layer_name: str) -> str:
    """Return 'external', 'internal_20', or 'internal_10'."""
    if _layer_has(layer_name, _WALL_EXT_FRAGS):
        return "external"
    if _layer_has(layer_name, _WALL_INT10_FRAGS):
        return "internal_10"
    if _layer_has(layer_name, _WALL_INT20_FRAGS):
        return "internal_20"
    # Generic wall — classify by thickness keyword if present
    ln = layer_name.lower()
    if "10" in ln:
        return "internal_10"
    if "20" in ln or "200" in ln:
        return "internal_20"
    return "internal_20"   # safest default for unclassified walls


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

class DXFParser:
    """Parse a DXF (or converted DWG→DXF) file and return DrawingData."""

    def __init__(self, filepath: str):
        if not EZDXF_AVAILABLE:
            raise ImportError("ezdxf is not installed. Run: pip install ezdxf")
        self.filepath = filepath
        self.doc = None
        self.msp = None
        self._scale = 1.0

    # ------------------------------------------------------------------
    def parse(self) -> dict:
        self.doc = ezdxf.readfile(self.filepath)
        self.msp = self.doc.modelspace()

        # Collect all geometry points to estimate drawing scale
        all_pts: list[tuple[float, float]] = []
        for e in self.msp:
            all_pts.extend(_entity_to_points(e))
        bbox_all = _bounding_box(all_pts)
        self._scale = _scale_from_bbox(bbox_all)

        walls        = self._extract_walls()
        columns      = self._extract_columns()
        beams        = self._extract_beams()
        openings     = self._extract_openings()
        rooms        = self._extract_rooms()
        slabs        = self._extract_slabs()
        foundations  = self._extract_foundations()
        staircases   = self._extract_staircases()
        boundary     = self._extract_boundary()

        # Derived scalars
        ext_walls  = [w for w in walls if w["type"] == "external"]
        int20_walls = [w for w in walls if w["type"] == "internal_20"]
        int10_walls = [w for w in walls if w["type"] == "internal_10"]

        ext_perimeter   = sum(w["length"] for w in ext_walls)
        len_20          = sum(w["length"] for w in int20_walls)
        len_10          = sum(w["length"] for w in int10_walls)
        total_floor_area = sum(r["area"] for r in rooms)

        # Plot area from boundary polygon or bounding box fallback
        plot_area = None
        if boundary:
            plot_area = boundary[0].get("area")
        if not plot_area and total_floor_area > 0:
            plot_area = total_floor_area  # rough approximation

        return {
            "source": "dxf",
            "filepath": self.filepath,
            "walls": walls,
            "columns": columns,
            "beams": beams,
            "openings": openings,
            "rooms": rooms,
            "slabs": slabs,
            "foundations": foundations,
            "footings": foundations,        # alias used by engine
            "neck_columns": [],
            "tie_beams": [],
            "staircases": staircases,
            "bounding_box": bbox_all,
            "plot_area": plot_area,
            "gf_area": total_floor_area,
            "total_floor_area": total_floor_area,
            "external_perimeter": ext_perimeter,
            "internal_wall_length_20cm": len_20,
            "internal_wall_length_10cm": len_10,
            "total_wall_length": ext_perimeter + len_20 + len_10,
            "longest_length": bbox_all["width"]  * self._scale,
            "longest_width":  bbox_all["height"] * self._scale,
            "floor_height": DEFAULT_FLOOR_HEIGHT,
            "slab_thickness": DEFAULT_SLAB_THICKNESS,
        }

    # ------------------------------------------------------------------
    def _s(self, v: float) -> float:
        """Apply unit scale factor."""
        return v * self._scale

    def _extract_walls(self) -> list[dict]:
        walls = []
        for entity in self.msp:
            ln = entity.dxf.layer
            is_wall = _layer_has(ln, _WALL_GENERIC_FRAGS) or \
                      _layer_has(ln, _WALL_EXT_FRAGS) or \
                      _layer_has(ln, _WALL_INT20_FRAGS) or \
                      _layer_has(ln, _WALL_INT10_FRAGS)
            if not is_wall:
                continue
            pts = _entity_to_points(entity)
            if not pts:
                continue
            wtype = _wall_type(ln)
            length = self._s(_polyline_length(pts))
            # Thickness: read from LWPOLYLINE width attribute, else default
            if entity.dxftype() == "LWPOLYLINE":
                raw_thick = getattr(entity.dxf, "const_width", 0.0) or 0.0
                if raw_thick > 0:
                    thickness = self._s(raw_thick)
                else:
                    thickness = {
                        "external": DEFAULT_WALL_THICKNESS_EXT,
                        "internal_20": DEFAULT_WALL_THICKNESS_INT20,
                        "internal_10": DEFAULT_WALL_THICKNESS_INT10,
                    }[wtype]
            else:
                thickness = {
                    "external": DEFAULT_WALL_THICKNESS_EXT,
                    "internal_20": DEFAULT_WALL_THICKNESS_INT20,
                    "internal_10": DEFAULT_WALL_THICKNESS_INT10,
                }[wtype]
            walls.append({
                "layer": ln,
                "type": wtype,
                "points": [(self._s(p[0]), self._s(p[1])) for p in pts],
                "length": round(length, 3),
                "thickness": thickness,
            })
        return walls

    def _extract_columns(self) -> list[dict]:
        cols = []
        for entity in self.msp:
            if not _layer_has(entity.dxf.layer, _COLUMN_FRAGS):
                continue
            pts = _entity_to_points(entity)
            if len(pts) < 2:
                continue
            bbox = _bounding_box(pts)
            cols.append({
                "layer": entity.dxf.layer,
                "width":  round(self._s(bbox["width"]), 3),
                "length": round(self._s(bbox["height"]), 3),
                "qty": 1,
            })
        return cols

    def _extract_beams(self) -> list[dict]:
        beams = []
        for entity in self.msp:
            if not _layer_has(entity.dxf.layer, _BEAM_FRAGS):
                continue
            pts = _entity_to_points(entity)
            if not pts:
                continue
            beams.append({
                "layer": entity.dxf.layer,
                "length": round(self._s(_polyline_length(pts)), 3),
                "width": 0.30,
                "depth": 0.60,
                "count": 1,
            })
        return beams

    def _extract_openings(self) -> list[dict]:
        openings = []
        for entity in self.msp:
            ln = entity.dxf.layer
            if not _layer_has(ln, _OPENING_FRAGS):
                continue
            is_window = _layer_has(ln, _WINDOW_FRAGS)
            opening_type = "window" if is_window else "door"
            pts = _entity_to_points(entity)
            if not pts:
                continue
            bbox = _bounding_box(pts)
            w = self._s(max(bbox["width"], bbox["height"]))
            openings.append({
                "layer": ln,
                "opening_type": opening_type,
                "width":  round(w, 3),
                "height": DEFAULT_WINDOW_HEIGHT if is_window else DEFAULT_DOOR_HEIGHT,
                "count": 1,
            })
        return openings

    def _extract_rooms(self) -> list[dict]:
        rooms = []
        for entity in self.msp:
            if not _layer_has(entity.dxf.layer, _ROOM_FRAGS):
                continue
            pts = _entity_to_points(entity)
            if len(pts) < 3:
                continue
            scaled = [(self._s(p[0]), self._s(p[1])) for p in pts]
            area = _polygon_area(scaled)
            perim = _closed_polyline_perimeter(scaled)
            if area < 0.1:
                continue
            rooms.append({
                "layer": entity.dxf.layer,
                "points": scaled,
                "area": round(area, 3),
                "perimeter": round(perim, 3),
                "room_type": _guess_room_type(entity.dxf.layer),
            })
        return rooms

    def _extract_slabs(self) -> list[dict]:
        slabs = []
        for entity in self.msp:
            if not _layer_has(entity.dxf.layer, _SLAB_FRAGS):
                continue
            pts = _entity_to_points(entity)
            if len(pts) < 3:
                continue
            scaled = [(self._s(p[0]), self._s(p[1])) for p in pts]
            area = _polygon_area(scaled)
            slabs.append({
                "layer": entity.dxf.layer,
                "area": round(area, 3),
                "thickness": DEFAULT_SLAB_THICKNESS,
            })
        return slabs

    def _extract_foundations(self) -> list[dict]:
        fnd = []
        for entity in self.msp:
            if not _layer_has(entity.dxf.layer, _FOUNDATION_FRAGS):
                continue
            pts = _entity_to_points(entity)
            if len(pts) < 2:
                continue
            bbox = _bounding_box(pts)
            fnd.append({
                "layer": entity.dxf.layer,
                "width":  round(self._s(bbox["width"]), 3),
                "length": round(self._s(bbox["height"]), 3),
                "depth": 0.50,
                "count": 1,
            })
        return fnd

    def _extract_staircases(self) -> list[dict]:
        stairs = []
        for entity in self.msp:
            if not _layer_has(entity.dxf.layer, _STAIR_FRAGS):
                continue
            pts = _entity_to_points(entity)
            if not pts:
                continue
            bbox = _bounding_box(pts)
            stairs.append({
                "layer": entity.dxf.layer,
                "width":  round(self._s(min(bbox["width"], bbox["height"])), 3),
                "length": round(self._s(max(bbox["width"], bbox["height"])), 3),
            })
        return stairs

    def _extract_boundary(self) -> list[dict]:
        bounds = []
        for entity in self.msp:
            if not _layer_has(entity.dxf.layer, _BOUNDARY_FRAGS):
                continue
            pts = _entity_to_points(entity)
            if len(pts) < 3:
                continue
            scaled = [(self._s(p[0]), self._s(p[1])) for p in pts]
            area = _polygon_area(scaled)
            perim = _closed_polyline_perimeter(scaled)
            if area < 1.0:
                continue
            bounds.append({
                "layer": entity.dxf.layer,
                "area": round(area, 3),
                "perimeter": round(perim, 3),
            })
        return bounds


# ---------------------------------------------------------------------------
# Room-type guesser from layer name
# ---------------------------------------------------------------------------

_ROOM_TYPE_MAP: list[tuple[tuple[str, ...], str]] = [
    (("toilet", "wc", "restroom"),              "toilet"),
    (("bathroom", "bath", "shower"),            "bathroom"),
    (("kitchen", "ktch"),                       "kitchen"),
    (("pantry", "store"),                       "pantry"),
    (("laundry", "utility", "service"),         "laundry"),
    (("bedroom", "bed", "room", "mstr"),        "bedroom"),
    (("living", "lounge", "majlis"),            "living"),
    (("dining", "dinning"),                     "dining"),
    (("balcony", "terrace", "loggia"),          "balcony"),
]


def _guess_room_type(layer_name: str) -> str:
    ln = layer_name.lower()
    for frags, rtype in _ROOM_TYPE_MAP:
        if any(f in ln for f in frags):
            return rtype
    return "other"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_dxf(filepath: str) -> dict:
    """Parse a DXF file and return a DrawingData dict."""
    return DXFParser(filepath).parse()
