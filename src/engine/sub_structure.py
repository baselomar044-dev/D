"""
Sub-Structure Calculator — implements the exact formulas specified in the QTO system.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Data classes for input
# ---------------------------------------------------------------------------

@dataclass
class Footing:
    width: float        # m
    length: float       # m
    depth: float        # m
    count: int = 1
    footing_type: str = "pad"


@dataclass
class NeckColumn:
    width: float        # m
    length: float       # m  (cross-section dimension; "length" per formula)
    count: int = 1


@dataclass
class TieBeam:
    length: float       # m
    width: float        # m
    depth: float        # m
    count: int = 1


@dataclass
class SolidBlockWall:
    wall_length: float  # m  (horizontal run)
    count: int = 1


# ---------------------------------------------------------------------------
# Calculator
# ---------------------------------------------------------------------------

PCC_THICKNESS = 0.10   # m  (10 cm)

# Standard rebar ratios (kg per m³ of concrete) — UAE industry norms
REBAR_KG_PER_M3 = {
    "foundation": 90,
    "neck_column": 150,
    "tie_beam": 120,
    "slab_on_grade": 75,
}


class SubStructureCalculator:
    """
    Calculates all sub-structure QTO items according to the specified formulas.
    Each method returns a dict with relevant quantities.
    """

    # ------------------------------------------------------------------
    # 1. Foundation
    # ------------------------------------------------------------------
    def calculate_foundation(self, footings: list[dict | Footing]) -> dict:
        """
        Area    = width × length × count  (per footing type)
        Volume  = width × length × depth × count
        PCC     = (length + 0.20) × (width + 0.20) × count × PCC_THICKNESS
        Bitumen = (area + perimeter × depth) × count
        """
        total_area = 0.0
        total_volume = 0.0
        total_pcc = 0.0
        total_bitumen = 0.0

        for f in footings:
            w = f["width"] if isinstance(f, dict) else f.width
            l = f["length"] if isinstance(f, dict) else f.length
            d = f["depth"] if isinstance(f, dict) else f.depth
            n = f.get("count", 1) if isinstance(f, dict) else f.count

            area_each = w * l
            volume_each = w * l * d
            pcc_each = (l + 0.20) * (w + 0.20) * PCC_THICKNESS
            perimeter = 2 * (w + l)
            bitumen_each = area_each + (perimeter * d)

            total_area += area_each * n
            total_volume += volume_each * n
            total_pcc += pcc_each * n
            total_bitumen += bitumen_each * n

        return {
            "area_m2": round(total_area, 3),
            "volume_m3": round(total_volume, 3),
            "pcc_volume_m3": round(total_pcc, 3),
            "bitumen_area_m2": round(total_bitumen, 3),
        }

    # ------------------------------------------------------------------
    # 2. Neck Columns
    # ------------------------------------------------------------------
    def calculate_neck_columns(
        self,
        columns: list[dict | NeckColumn],
        gfl: float,
        exc_depth: float,
        tb_depth: float,
        pcc_thickness: float = PCC_THICKNESS,
    ) -> dict:
        """
        Height = GFL + exc_depth - tb_depth - pcc_thickness
        Volume = width × length × height × count  (concrete cross-section × height)
        Area   = width × length × count  (plan area, for quantity schedule)
        """
        height = gfl + exc_depth - tb_depth - pcc_thickness
        total_volume = 0.0
        total_area = 0.0

        for c in columns:
            w = c["width"] if isinstance(c, dict) else c.width
            l = c["length"] if isinstance(c, dict) else c.length
            n = c.get("count", 1) if isinstance(c, dict) else c.count

            total_volume += w * l * height * n
            total_area += w * l * n

        return {
            "height_m": round(height, 3),
            "volume_m3": round(total_volume, 3),
            "plan_area_m2": round(total_area, 3),
        }

    # ------------------------------------------------------------------
    # 3. Tie Beams / Strap Beams
    # ------------------------------------------------------------------
    def calculate_tie_beams(self, beams: list[dict | TieBeam]) -> dict:
        """
        Volume  = length × width × depth
        PCC     = length × (width + 0.20) × PCC_THICKNESS
        Bitumen = length × depth × 2
        """
        total_volume = 0.0
        total_pcc = 0.0
        total_bitumen = 0.0

        for b in beams:
            l = b["length"] if isinstance(b, dict) else b.length
            w = b["width"] if isinstance(b, dict) else b.width
            d = b["depth"] if isinstance(b, dict) else b.depth
            n = b.get("count", 1) if isinstance(b, dict) else b.count

            total_volume += l * w * d * n
            total_pcc += l * (w + 0.20) * PCC_THICKNESS * n
            total_bitumen += l * d * 2 * n

        return {
            "volume_m3": round(total_volume, 3),
            "pcc_volume_m3": round(total_pcc, 3),
            "bitumen_area_m2": round(total_bitumen, 3),
        }

    # ------------------------------------------------------------------
    # 4. Solid Block Work
    # ------------------------------------------------------------------
    def calculate_solid_block_work(
        self,
        walls: list[dict | SolidBlockWall],
        gfl: float,
        exc_depth: float,
        tb_depth: float,
        pcc_thickness: float = PCC_THICKNESS,
    ) -> dict:
        """
        Height  = GFL + exc_depth - tb_depth - pcc_thickness
        Area    = wall_length × height × count
        Bitumen = area × 2
        """
        height = gfl + exc_depth - tb_depth - pcc_thickness
        total_area = 0.0

        for w in walls:
            wl = w["wall_length"] if isinstance(w, dict) else w.wall_length
            n = w.get("count", 1) if isinstance(w, dict) else w.count
            total_area += wl * height * n

        total_bitumen = total_area * 2.0

        return {
            "height_m": round(height, 3),
            "area_m2": round(total_area, 3),
            "bitumen_area_m2": round(total_bitumen, 3),
        }

    # ------------------------------------------------------------------
    # 5. Slab on Grade
    # ------------------------------------------------------------------
    def calculate_slab_on_grade(
        self, gf_area: float, thickness: float = 0.10
    ) -> dict:
        """
        Volume = GF_area × thickness
        """
        volume = gf_area * thickness
        return {
            "area_m2": round(gf_area, 3),
            "thickness_m": thickness,
            "volume_m3": round(volume, 3),
        }

    # ------------------------------------------------------------------
    # 6. Excavation
    # ------------------------------------------------------------------
    def calculate_excavation(
        self,
        longest_length: float,
        longest_width: float,
        exc_level: float,
    ) -> dict:
        """
        Area   = (2 + longest_length) × (2 + longest_width)
        Volume = area × exc_level
        """
        area = (2.0 + longest_length) * (2.0 + longest_width)
        volume = area * exc_level
        return {
            "area_m2": round(area, 3),
            "volume_m3": round(volume, 3),
            "exc_level_m": exc_level,
        }

    # ------------------------------------------------------------------
    # 7. Back Filling
    # ------------------------------------------------------------------
    def calculate_back_filling(
        self,
        exc_area: float,
        exc_level: float,
        gfsl_level: float,
        all_items_volume: float,
    ) -> dict:
        """
        Volume = (exc_area × (exc_level + gfsl_level)) - all_items_volume
        """
        gross_volume = exc_area * (exc_level + gfsl_level)
        net_volume = max(gross_volume - all_items_volume, 0.0)
        return {
            "gross_volume_m3": round(gross_volume, 3),
            "deducted_volume_m3": round(all_items_volume, 3),
            "net_volume_m3": round(net_volume, 3),
        }

    # ------------------------------------------------------------------
    # 8. Anti-Termite
    # ------------------------------------------------------------------
    def calculate_anti_termite(
        self, total_pcc_area: float, slab_on_grade_area: float
    ) -> dict:
        """
        Area = (total_pcc_area + slab_on_grade_area) × 1.15
        """
        area = (total_pcc_area + slab_on_grade_area) * 1.15
        return {"area_m2": round(area, 3)}

    # ------------------------------------------------------------------
    # 9. Polyethylene Sheet
    # ------------------------------------------------------------------
    def calculate_polyethylene_sheet(
        self, total_pcc_area: float, slab_on_grade_area: float
    ) -> dict:
        """
        Area = total_pcc_area + slab_on_grade_area
        """
        area = total_pcc_area + slab_on_grade_area
        return {"area_m2": round(area, 3)}

    # ------------------------------------------------------------------
    # 10. Road Base (if applicable)
    # ------------------------------------------------------------------
    def calculate_road_base(
        self, exc_area: float, thickness: float = 0.25
    ) -> dict:
        """
        Volume = exc_area × thickness (default 25 cm)
        """
        volume = exc_area * thickness
        return {
            "area_m2": round(exc_area, 3),
            "thickness_m": thickness,
            "volume_m3": round(volume, 3),
        }

    # ------------------------------------------------------------------
    # 11. Foundation Reinforcement Steel
    # ------------------------------------------------------------------
    def calculate_rebar_foundations(
        self, footings: list, kg_per_m3: float = REBAR_KG_PER_M3["foundation"]
    ) -> dict:
        """
        Rebar mass = foundation_concrete_volume × kg_per_m3
        """
        f_res = self.calculate_foundation(footings)
        return {"kg": round(f_res["volume_m3"] * kg_per_m3, 1)}

    # ------------------------------------------------------------------
    # 12. Neck Column Reinforcement Steel
    # ------------------------------------------------------------------
    def calculate_rebar_neck_columns(
        self, volume_m3: float, kg_per_m3: float = REBAR_KG_PER_M3["neck_column"]
    ) -> dict:
        """
        Rebar mass = neck_column_concrete_volume × kg_per_m3
        """
        return {"kg": round(volume_m3 * kg_per_m3, 1)}

    # ------------------------------------------------------------------
    # 13. Tie Beam Reinforcement Steel
    # ------------------------------------------------------------------
    def calculate_rebar_tie_beams(
        self, volume_m3: float, kg_per_m3: float = REBAR_KG_PER_M3["tie_beam"]
    ) -> dict:
        """
        Rebar mass = tie_beam_concrete_volume × kg_per_m3
        """
        return {"kg": round(volume_m3 * kg_per_m3, 1)}

    # ------------------------------------------------------------------
    # 14. Slab on Grade Reinforcement Steel
    # ------------------------------------------------------------------
    def calculate_rebar_slab_on_grade(
        self, volume_m3: float, kg_per_m3: float = REBAR_KG_PER_M3["slab_on_grade"]
    ) -> dict:
        """
        Rebar mass = slab_on_grade_concrete_volume × kg_per_m3
        """
        return {"kg": round(volume_m3 * kg_per_m3, 1)}

    # ------------------------------------------------------------------
    # 15. Foundation Formwork (side shuttering)
    # ------------------------------------------------------------------
    def calculate_formwork_foundation(
        self, footings: list
    ) -> dict:
        """
        Area = perimeter × depth × count  (4 side faces per footing)
        """
        total = 0.0
        for f in footings:
            w = f["width"] if isinstance(f, dict) else f.width
            l = f["length"] if isinstance(f, dict) else f.length
            d = f["depth"] if isinstance(f, dict) else f.depth
            n = f.get("count", 1) if isinstance(f, dict) else f.count
            total += 2 * (w + l) * d * n
        return {"area_m2": round(total, 3)}

    # ------------------------------------------------------------------
    # 16. Sand Filling Under Ground Floor Slab
    # ------------------------------------------------------------------
    def calculate_sand_filling(
        self, gf_area: float, thickness: float = 0.30
    ) -> dict:
        """
        Volume = gf_area × thickness (default 30 cm compacted sand bed)
        """
        volume = gf_area * thickness
        return {
            "area_m2": round(gf_area, 3),
            "thickness_m": thickness,
            "volume_m3": round(volume, 3),
        }
