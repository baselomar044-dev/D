"""
Super-Structure Calculator — implements the exact formulas for above-grade structural elements.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Data classes for input
# ---------------------------------------------------------------------------

@dataclass
class Slab:
    area: float         # m²
    thickness: float    # m


@dataclass
class Beam:
    length: float       # m
    width: float        # m
    depth: float        # m (overall depth including slab soffit to top)
    count: int = 1


@dataclass
class Column:
    length: float       # m (cross-section)
    width: float        # m (cross-section)
    qty: int = 1


# ---------------------------------------------------------------------------
# Calculator
# ---------------------------------------------------------------------------

class SuperStructureCalculator:
    """
    Calculates all super-structure QTO items according to the specified formulas.
    """

    # ------------------------------------------------------------------
    # 1. Slabs
    # ------------------------------------------------------------------
    def calculate_slabs(self, slabs: list[dict | Slab]) -> dict:
        """
        Volume = area × thickness  (per slab)
        """
        total_area = 0.0
        total_volume = 0.0

        for s in slabs:
            a = s["area"] if isinstance(s, dict) else s.area
            t = s["thickness"] if isinstance(s, dict) else s.thickness
            total_area += a
            total_volume += a * t

        return {
            "area_m2": round(total_area, 3),
            "volume_m3": round(total_volume, 3),
        }

    # ------------------------------------------------------------------
    # 2. Beams
    # ------------------------------------------------------------------
    def calculate_beams(
        self, beams: list[dict | Beam], slab_thickness: float
    ) -> dict:
        """
        Volume = length × width × (depth - slab_thickness)  per beam
        The portion embedded in the slab is excluded to avoid double-counting.
        """
        total_volume = 0.0

        for b in beams:
            l = b["length"] if isinstance(b, dict) else b.length
            w = b["width"] if isinstance(b, dict) else b.width
            d = b["depth"] if isinstance(b, dict) else b.depth
            n = b.get("count", 1) if isinstance(b, dict) else b.count

            net_depth = max(d - slab_thickness, 0.0)
            total_volume += l * w * net_depth * n

        return {"volume_m3": round(total_volume, 3)}

    # ------------------------------------------------------------------
    # 3. Columns
    # ------------------------------------------------------------------
    def calculate_columns(
        self, columns: list[dict | Column], floor_height: float
    ) -> dict:
        """
        Volume = length × width × floor_height × qty  (per column type)
        """
        total_volume = 0.0

        for c in columns:
            l = c["length"] if isinstance(c, dict) else c.length
            w = c["width"] if isinstance(c, dict) else c.width
            qty = c.get("qty", 1) if isinstance(c, dict) else c.qty
            total_volume += l * w * floor_height * qty

        return {"volume_m3": round(total_volume, 3)}

    # ------------------------------------------------------------------
    # 4. Dry Area Flooring
    # ------------------------------------------------------------------
    def calculate_dry_area_flooring(
        self, total_floor_area: float, wet_area: float
    ) -> dict:
        """
        Dry Area Flooring = total_floor_area - wet_area
        """
        dry = max(total_floor_area - wet_area, 0.0)
        return {"area_m2": round(dry, 3)}

    # ------------------------------------------------------------------
    # 5. Skirting
    # ------------------------------------------------------------------
    def calculate_skirting(
        self, dry_area_perimeter: float, door_widths: list[float]
    ) -> dict:
        """
        Skirting = perimeter_of_dry_areas - (40% of sum of door widths)
        """
        door_deduction = sum(door_widths) * 0.40
        skirting = max(dry_area_perimeter - door_deduction, 0.0)
        return {
            "perimeter_m": round(dry_area_perimeter, 3),
            "door_deduction_m": round(door_deduction, 3),
            "area_m": round(skirting, 3),  # skirting is linear metres (RM)
        }

    # ------------------------------------------------------------------
    # 6. Paint
    # ------------------------------------------------------------------
    def calculate_paint(
        self, skirting_length: float, floor_height: float
    ) -> dict:
        """
        Paint area = skirting_length × floor_height
        """
        area = skirting_length * floor_height
        return {"area_m2": round(area, 3)}

    # ------------------------------------------------------------------
    # 7. Dry Areas Ceiling
    # ------------------------------------------------------------------
    def calculate_dry_areas_ceiling(self, dry_area_flooring: float) -> dict:
        """
        Dry Areas Ceiling = Dry Areas Flooring  (1:1 relationship)
        """
        return {"area_m2": round(dry_area_flooring, 3)}

    # ------------------------------------------------------------------
    # 8. Slab Reinforcement Steel
    # ------------------------------------------------------------------
    def calculate_rebar_slabs(
        self, volume_m3: float, kg_per_m3: float = 100.0
    ) -> dict:
        """
        Rebar mass = slab_concrete_volume × kg_per_m3
        Default: 100 kg/m³ (suspended RC slab, UAE standard)
        """
        return {"kg": round(volume_m3 * kg_per_m3, 1)}

    # ------------------------------------------------------------------
    # 9. Beam Reinforcement Steel
    # ------------------------------------------------------------------
    def calculate_rebar_beams(
        self, volume_m3: float, kg_per_m3: float = 150.0
    ) -> dict:
        """
        Rebar mass = beam_concrete_volume × kg_per_m3
        Default: 150 kg/m³ (heavily reinforced beams)
        """
        return {"kg": round(volume_m3 * kg_per_m3, 1)}

    # ------------------------------------------------------------------
    # 10. Column Reinforcement Steel
    # ------------------------------------------------------------------
    def calculate_rebar_columns(
        self, volume_m3: float, kg_per_m3: float = 175.0
    ) -> dict:
        """
        Rebar mass = column_concrete_volume × kg_per_m3
        Default: 175 kg/m³ (RC columns with ties)
        """
        return {"kg": round(volume_m3 * kg_per_m3, 1)}

    # ------------------------------------------------------------------
    # 11. Slab Formwork / Soffit Shuttering
    # ------------------------------------------------------------------
    def calculate_formwork_slabs(self, slabs: list) -> dict:
        """
        Area = sum of slab areas  (soffit shuttering, 1:1 with slab plan)
        """
        total = sum(
            (s["area"] if isinstance(s, dict) else s.area) for s in slabs
        )
        return {"area_m2": round(total, 3)}

    # ------------------------------------------------------------------
    # 12. Beam Formwork / Shuttering
    # ------------------------------------------------------------------
    def calculate_formwork_beams(
        self, beams: list, slab_thickness: float
    ) -> dict:
        """
        Area per beam = length × (width + 2 × net_depth) × count
        net_depth = depth - slab_thickness  (portion below slab soffit)
        Includes soffit and two side faces.
        """
        total = 0.0
        for b in beams:
            l = b["length"] if isinstance(b, dict) else b.length
            w = b["width"] if isinstance(b, dict) else b.width
            d = b["depth"] if isinstance(b, dict) else b.depth
            n = b.get("count", 1) if isinstance(b, dict) else b.count
            net_depth = max(d - slab_thickness, 0.0)
            total += l * (w + 2 * net_depth) * n
        return {"area_m2": round(total, 3)}

    # ------------------------------------------------------------------
    # 13. Column Formwork / Shuttering
    # ------------------------------------------------------------------
    def calculate_formwork_columns(
        self, columns: list, floor_height: float
    ) -> dict:
        """
        Area = 2 × (width + length) × floor_height × qty  (4 faces)
        """
        total = 0.0
        for c in columns:
            l = c["length"] if isinstance(c, dict) else c.length
            w = c["width"] if isinstance(c, dict) else c.width
            qty = c.get("qty", 1) if isinstance(c, dict) else c.qty
            total += 2 * (w + l) * floor_height * qty
        return {"area_m2": round(total, 3)}

    # ------------------------------------------------------------------
    # 14. Parapet Block Work
    # ------------------------------------------------------------------
    def calculate_parapet_block(
        self, perimeter: float, height: float = 1.00
    ) -> dict:
        """
        Parapet block wall area.
        Area = perimeter × height
        """
        area = perimeter * height
        return {"area_m2": round(area, 3)}

    # ------------------------------------------------------------------
    # 15. Parapet Concrete Capping
    # ------------------------------------------------------------------
    def calculate_parapet_concrete(
        self,
        perimeter: float,
        thickness: float = 0.20,
        capping_height: float = 0.20,
    ) -> dict:
        """
        Parapet RC capping / ring beam on top of parapet wall.
        Volume = perimeter × thickness × capping_height
        """
        volume = perimeter * thickness * capping_height
        return {"volume_m3": round(volume, 3)}

    # ------------------------------------------------------------------
    # 16. Staircase
    # ------------------------------------------------------------------
    def calculate_staircase(
        self,
        floor_height: float,
        stair_width: float = 1.20,
        stair_count: int = 1,
        slab_thickness: float = 0.20,
    ) -> dict:
        """
        Concrete volume for a single stair flight + landing.

        Formula
        -------
        steps          = round(floor_height / 0.175)   ← 175 mm riser (standard UAE)
        going          = 0.25 m  (standard tread depth)
        horizontal_run = steps × going
        inclined_len   = √(horizontal_run² + floor_height²)
        waist_vol      = inclined_len × stair_width × 0.15  (150 mm waist slab)
        step_vol       = 0.5 × going × riser × stair_width × steps
        landing_vol    = stair_width × stair_width × slab_thickness
        total_vol      = (waist_vol + step_vol + landing_vol) × stair_count
        """
        steps = max(int(round(floor_height / 0.175)), 1)
        going = 0.25
        riser = floor_height / steps
        horizontal_run = steps * going
        inclined_len = math.sqrt(horizontal_run ** 2 + floor_height ** 2)

        waist_vol = inclined_len * stair_width * 0.15
        step_vol = 0.5 * going * riser * stair_width * steps
        landing_vol = stair_width * stair_width * slab_thickness
        total_vol = (waist_vol + step_vol + landing_vol) * stair_count

        return {
            "volume_m3": round(total_vol, 3),
            "steps": steps,
            "stair_count": stair_count,
        }
