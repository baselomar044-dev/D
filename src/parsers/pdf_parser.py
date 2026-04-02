"""
PDF Parser — uses Google Gemini Vision API to extract QTO data
from scanned or vector PDF architectural drawings.
Returns a DrawingData dict compatible with the QTO engine.

Cost-discipline defaults
------------------------
* Model   : gemini-2.0-flash  (~17× cheaper than gemini-2.5-pro, same vision quality)
* DPI     : 100  (vs 150 — reduces image tokens by ~44 % while remaining legible)
* max_pages: 3   (most architectural sets have the key plan on pages 1–3)
* format  : JPEG quality 85 (vs PNG — reduces image size ~55%, same Gemini legibility)

Override any of these via the constructor or the CLI (--gemini-model, --max-pages).
"""

from __future__ import annotations

import base64
import io
import json
import os
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Cost-discipline defaults
# ---------------------------------------------------------------------------

DEFAULT_MODEL: str = "gemini-2.0-flash-lite"
DEFAULT_DPI: int = 150          # raised from 100 → sharper text for dim-string reading
DEFAULT_MAX_PAGES: int = 5      # cover GF plan + FF plan + sections + details
DEFAULT_JPEG_QUALITY: int = 90  # raised from 85 → less artefacts on fine linework

# Models allowed for cost control — never exceed flash-lite tier
ALLOWED_MODELS: set = {
    "gemini-2.0-flash-lite",
    "gemini-2.5-flash-lite",
}


# ---------------------------------------------------------------------------
# Extraction prompt — v3: covers all 45+ BOQ items, strict output rules
# ---------------------------------------------------------------------------

EXTRACTION_PROMPT = """
You are a senior Quantity Surveyor in the UAE reviewing an architectural/structural drawing set for a residential villa.
Your task is to extract EVERY measurable quantity needed to produce a complete BOQ of 45+ line items.

Study the drawing VERY carefully:
  • Read ALL dimension strings, keynotes, room tags, title block, and scale bar.
  • Measure lengths from the drawing using the scale bar if explicit dimensions are absent.
  • Identify floor levels (Ground Floor, First Floor, Roof) and record items per floor.

Return ONLY a single valid JSON object — no markdown, no code fences, no commentary.
All linear and area values in METRES / SQUARE METRES. Volumes in CUBIC METRES.

──────────────────────────────────────────────────────────────────────────────
REQUIRED JSON STRUCTURE
──────────────────────────────────────────────────────────────────────────────
{
  "project_type": "G | G+1 | G+2 | G+1 Service",
  "plot_area":    <float m² — total plot area from title block or boundary>,
  "gf_area":      <float m² — ground-floor built footprint>,
  "roof_area":    <float m² — roof slab plan area>,
  "total_floor_area": <float m² — sum of ALL floor areas>,
  "floor_height": <float m — floor-to-floor height, typically 3.0>,
  "slab_thickness": <float m — structural slab thickness, typically 0.20>,
  "exc_depth":    <float m — excavation depth below natural ground, typically 1.50>,
  "gfl":          <float m — ground floor finished level above excavation datum, typically 0.30>,
  "tb_depth":     <float m — tie beam depth, typically 0.40>,
  "pcc_thickness":<float m — PCC blinding thickness, typically 0.10>,
  "sand_fill_thickness": <float m — sand fill under SOG, typically 0.30>,
  "road_base_thickness": <float m — road base compacted thickness, typically 0.25>,
  "has_road_base": <bool — true if road base is indicated>,
  "external_perimeter": <float m — total external wall perimeter>,
  "internal_wall_length_20cm": <float m — total length of all 20 cm internal walls>,
  "internal_wall_length_10cm": <float m — total length of all 10 cm partition walls>,
  "dry_area_perimeter": <float m — total perimeter of all non-wet rooms>,
  "longest_length": <float m — longest building footprint dimension>,
  "longest_width":  <float m — shortest building footprint dimension>,
  "boundary_perimeter": <float m — perimeter of the plot boundary wall>,
  "boundary_wall_height": <float m — height of compound/boundary wall, typically 2.5>,
  "main_door_area": <float m² — area of main entrance door, typically 4.0>,
  "stair_width":   <float m — staircase clear width, typically 1.20>,
  "stair_count":   <int — number of independent staircases>,
  "thermal_block_schedule_area":  <float m² or null — from block schedule if shown>,
  "interlock_paving_area":        <float m² or null — from external works drawing>,
  "false_ceiling_area":           <float m² or null — from RCP drawing>,
  "roof_waterproofing_area":      <float m² or null — from roof plan>,
  "walls": [
    {
      "type": "external | internal_20 | internal_10",
      "length": <float m>,
      "thickness": <float m — 0.25 ext / 0.20 int20 / 0.10 int10>
    }
  ],
  "footings": [
    {"footing_type": "pad | strip | raft", "width": <m>, "length": <m>, "depth": <m>, "count": <int>}
  ],
  "neck_columns": [
    {"width": <m>, "length": <m>, "count": <int>}
  ],
  "tie_beams": [
    {"length": <m>, "width": <m>, "depth": <m>, "count": <int>}
  ],
  "solid_block_walls": [
    {"wall_length": <m>, "count": <int>}
  ],
  "slabs": [
    {"area": <float m²>, "thickness": <float m>}
  ],
  "beams": [
    {"length": <m>, "width": <m>, "depth": <m>, "count": <int>}
  ],
  "columns": [
    {"length": <m>, "width": <m>, "qty": <int>}
  ],
  "openings": [
    {
      "opening_type": "door | window",
      "width": <float m>,
      "height": <float m>,
      "count": <int>
    }
  ],
  "rooms": [
    {
      "room_type": "toilet | bathroom | kitchen | pantry | laundry | bedroom | living | dining | balcony | other",
      "area": <float m²>,
      "perimeter": <float m>
    }
  ],
  "first_floor_rooms": [
    {
      "room_type": "toilet | bathroom | kitchen | pantry | laundry | bedroom | living | dining | balcony | other",
      "area": <float m²>,
      "perimeter": <float m>
    }
  ],
  "notes": "<observations about drawing quality, missing data, or assumptions>"
}

──────────────────────────────────────────────────────────────────────────────
CRITICAL EXTRACTION RULES
──────────────────────────────────────────────────────────────────────────────
1.  SCALE: Read the scale bar or title block. If the drawing is 1:100, every
    10 mm on paper = 1 m in reality. Apply this consistently.
2.  WALLS: External boundary walls → "external". Main internal 20 cm block walls →
    "internal_20". Lightweight 10 cm dashed partitions → "internal_10".
3.  FOOTINGS: List each unique footing size group with count. Typical UAE pad
    footing: 1.2 × 1.2 × 0.5 m. Strip footings: width × depth per linear metre.
4.  OPENINGS: Count EVERY door and window. Never use the key "type" — always use
    "opening_type". Main entrance door is typically 2.0 × 2.4 m.
5.  ROOMS: List EVERY room on EVERY floor individually. Do not merge rooms.
    Estimate perimeter from area if not dimensioned: perimeter ≈ 4 × √area × 1.1.
6.  FIRST FLOOR ROOMS: Duplicate first-floor rooms in "first_floor_rooms" array
    (needed for waterproofing calculation under upper-floor wet areas).
7.  SLABS: List each slab panel separately (ground floor slab, first floor, roof).
    Include area and thickness. Roof slab area = gf_area for simple flat roofs.
8.  STAIRCASE: Identify number of stairs and their width. Count flights.
9.  BOUNDARY: Measure the plot boundary perimeter from the site plan.
10. EXTERNAL WORKS: Note any interlock paving areas shown on external works drawing.
11. NULL RULE: Use null for any value you cannot determine — never guess dimensions
    that are not visible or inferable from the drawing.
""".strip()


# ---------------------------------------------------------------------------
# Helper: convert PDF page to base64 JPEG (cost-optimised)
# ---------------------------------------------------------------------------

def _pdf_pages_to_base64(
    pdf_path: str,
    dpi: int = DEFAULT_DPI,
    max_pages: int = DEFAULT_MAX_PAGES,
    jpeg_quality: int = DEFAULT_JPEG_QUALITY,
) -> list[tuple[str, str]]:
    """Convert PDF pages to base64-encoded images.

    Returns list of (base64_string, mime_type) tuples.
    JPEG is used by default (~55 % fewer tokens vs PNG at same Gemini legibility).

    Parameters
    ----------
    pdf_path     : path to the PDF file
    dpi          : render resolution (lower = fewer image tokens = lower cost)
    max_pages    : maximum number of pages to process (0 = all)
    jpeg_quality : JPEG quality 1-95 (default 85 — good quality, small size)
    """
    try:
        from pdf2image import convert_from_path  # type: ignore
    except ImportError as exc:
        raise ImportError("pdf2image is required: pip install pdf2image") from exc

    images = convert_from_path(pdf_path, dpi=dpi)
    if max_pages and max_pages > 0:
        images = images[:max_pages]

    result = []
    for img in images:
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=jpeg_quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        result.append((b64, "image/jpeg"))
    return result


# ---------------------------------------------------------------------------
# Main parser class
# ---------------------------------------------------------------------------

class PDFParser:
    """
    Parse architectural PDF drawings via Gemini Vision and return DrawingData.

    Parameters
    ----------
    pdf_path     : path to the PDF file
    api_key      : Gemini API key
    model        : Gemini model name (default: gemini-2.0-flash)
    dpi          : render DPI for page images (default: 100)
    max_pages    : cap on number of pages sent to the API (default: 3; 0 = all)
    jpeg_quality : JPEG compression quality (default: 85; use 95 for max accuracy)
    """

    def __init__(
        self,
        pdf_path: str,
        api_key: str,
        model: str = DEFAULT_MODEL,
        dpi: int = DEFAULT_DPI,
        max_pages: int = DEFAULT_MAX_PAGES,
        jpeg_quality: int = DEFAULT_JPEG_QUALITY,
    ):
        self.pdf_path = pdf_path
        self.api_key = api_key
        if model not in ALLOWED_MODELS:
            print(f"[PDFParser] Model '{model}' not allowed. Using {DEFAULT_MODEL}.")
            model = DEFAULT_MODEL
        self.model = model
        self.dpi = dpi
        self.max_pages = max_pages
        self.jpeg_quality = jpeg_quality

    # ------------------------------------------------------------------
    def parse(self) -> dict:
        """Send PDF pages to Gemini and aggregate extracted data."""
        try:
            import google.generativeai as genai  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "google-generativeai is required: pip install google-generativeai"
            ) from exc

        genai.configure(api_key=self.api_key)
        model = genai.GenerativeModel(self.model)

        pages = _pdf_pages_to_base64(
            self.pdf_path, dpi=self.dpi,
            max_pages=self.max_pages, jpeg_quality=self.jpeg_quality,
        )
        if not pages:
            print("[PDFParser] Warning: no pages were extracted from the PDF.")
            return self._merge_pages([])

        print(
            f"[PDFParser] Processing {len(pages)} page(s) with model '{self.model}' "
            f"at {self.dpi} DPI (JPEG q{self.jpeg_quality})."
        )
        all_page_data: list[dict] = []

        for i, (page_b64, mime_type) in enumerate(pages):
            image_part = {
                "inline_data": {
                    "mime_type": mime_type,
                    "data": page_b64,
                }
            }
            for attempt in range(3):
                try:
                    response = model.generate_content([EXTRACTION_PROMPT, image_part])
                    raw_text = response.text.strip()
                    # Strip potential markdown code fences
                    if raw_text.startswith("```"):
                        raw_text = raw_text.split("```")[1]
                        if raw_text.startswith("json"):
                            raw_text = raw_text[4:]
                    page_data = json.loads(raw_text)
                    all_page_data.append(page_data)
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt < 2:
                        print(f"[PDFParser] Page {i + 1} attempt {attempt + 1} failed: {exc}, retrying...")
                        continue
                    print(f"[PDFParser] Warning: page {i + 1} failed after 3 attempts: {exc}")

        return self._merge_pages(all_page_data)

    # ------------------------------------------------------------------
    @staticmethod
    def _merge_pages(pages: list[dict]) -> dict:
        """Merge per-page extraction results into a single DrawingData dict."""
        merged: dict[str, Any] = {
            "source": "pdf",
            "walls": [],
            "columns": [],
            "beams": [],
            "openings": [],
            "rooms": [],
            "first_floor_rooms": [],
            "slabs": [],
            "foundations": [],
            "footings": [],
            "neck_columns": [],
            "tie_beams": [],
            "solid_block_walls": [],
            # Scalars — first non-null value wins
            "plot_area": None,
            "gf_area": None,
            "roof_area": None,
            "floor_height": None,
            "slab_thickness": None,
            "exc_depth": None,
            "gfl": None,
            "tb_depth": None,
            "pcc_thickness": None,
            "sand_fill_thickness": None,
            "road_base_thickness": None,
            "has_road_base": None,
            "project_type": None,
            "external_perimeter": None,
            "internal_wall_length_20cm": None,
            "internal_wall_length_10cm": None,
            "longest_length": None,
            "longest_width": None,
            "dry_area_perimeter": None,
            "total_floor_area": None,
            "boundary_perimeter": None,
            "boundary_wall_height": None,
            "main_door_area": None,
            "stair_width": None,
            "stair_count": None,
            "thermal_block_schedule_area": None,
            "interlock_paving_area": None,
            "false_ceiling_area": None,
            "roof_waterproofing_area": None,
            "notes": [],
            "bounding_box": {},
            "total_wall_length": 0.0,
        }

        list_keys = [
            "walls", "columns", "beams", "openings", "rooms", "first_floor_rooms",
            "slabs", "foundations", "footings", "neck_columns", "tie_beams",
            "solid_block_walls",
        ]
        scalar_keys = [
            "plot_area", "gf_area", "roof_area", "floor_height", "slab_thickness",
            "exc_depth", "gfl", "tb_depth", "pcc_thickness", "sand_fill_thickness",
            "road_base_thickness", "has_road_base", "project_type",
            "external_perimeter", "internal_wall_length_20cm",
            "internal_wall_length_10cm", "longest_length", "longest_width",
            "dry_area_perimeter", "total_floor_area", "boundary_perimeter",
            "boundary_wall_height", "main_door_area", "stair_width", "stair_count",
            "thermal_block_schedule_area", "interlock_paving_area",
            "false_ceiling_area", "roof_waterproofing_area",
        ]

        for page in pages:
            for key in list_keys:
                if isinstance(page.get(key), list):
                    merged[key].extend(page[key])
            for key in scalar_keys:
                if page.get(key) is not None and merged[key] is None:
                    merged[key] = page[key]
            if page.get("notes"):
                merged["notes"].append(page["notes"])

        # Derived totals
        merged["total_wall_length"] = sum(w.get("length", 0) for w in merged["walls"])

        if merged["internal_wall_length_20cm"] is None:
            merged["internal_wall_length_20cm"] = sum(
                w.get("length", 0) for w in merged["walls"]
                if w.get("type", "") == "internal_20"
            )
        if merged["internal_wall_length_10cm"] is None:
            merged["internal_wall_length_10cm"] = sum(
                w.get("length", 0) for w in merged["walls"]
                if w.get("type", "") == "internal_10"
            )
        if merged["external_perimeter"] is None:
            merged["external_perimeter"] = sum(
                w.get("length", 0) for w in merged["walls"]
                if w.get("type", "") == "external"
            )
        merged["total_floor_area"] = merged["total_floor_area"] or sum(
            r.get("area", 0) for r in merged["rooms"]
        )
        # Apply sensible defaults for missing structural parameters
        merged.setdefault("floor_height", 3.0)
        if merged["floor_height"] is None:
            merged["floor_height"] = 3.0
        merged.setdefault("slab_thickness", 0.20)
        if merged["slab_thickness"] is None:
            merged["slab_thickness"] = 0.20
        merged.setdefault("exc_depth", 1.50)
        if merged["exc_depth"] is None:
            merged["exc_depth"] = 1.50
        merged.setdefault("gfl", 0.30)
        if merged["gfl"] is None:
            merged["gfl"] = 0.30
        merged.setdefault("tb_depth", 0.40)
        if merged["tb_depth"] is None:
            merged["tb_depth"] = 0.40
        return merged


# ---------------------------------------------------------------------------
# Public convenience function
# ---------------------------------------------------------------------------

def parse_pdf(
    pdf_path: str,
    api_key: str | None = None,
    model: str = DEFAULT_MODEL,
    dpi: int = DEFAULT_DPI,
    max_pages: int = DEFAULT_MAX_PAGES,
    jpeg_quality: int = DEFAULT_JPEG_QUALITY,
) -> dict:
    """
    Parse a PDF architectural drawing via Gemini Vision.

    Parameters
    ----------
    pdf_path     : path to the PDF file
    api_key      : Gemini API key; falls back to GEMINI_API_KEY env var
    model        : Gemini model (default: gemini-2.0-flash-lite).
                   Allowed: gemini-2.0-flash-lite, gemini-2.5-flash-lite.
    dpi          : render resolution (default: 150 — sharp enough for dimension strings)
    max_pages    : maximum pages to process (default: 5; 0 = all pages)
    jpeg_quality : JPEG quality 1-95 (default: 90 — sharp linework)
    """
    key = api_key or os.environ.get("GEMINI_API_KEY", "")
    if not key:
        raise ValueError(
            "A Gemini API key is required. Pass api_key= or set GEMINI_API_KEY."
        )
    parser = PDFParser(pdf_path, key, model=model, dpi=dpi,
                       max_pages=max_pages, jpeg_quality=jpeg_quality)
    return parser.parse()
