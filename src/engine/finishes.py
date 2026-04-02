"""
Finishes Calculator — implements all 16+ architectural finish item formulas.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Data class for Opening schedule items
# ---------------------------------------------------------------------------

@dataclass
class Opening:
    opening_type: str   # "door" | "window"
    width: float        # m
    height: float       # m
    count: int = 1


# ---------------------------------------------------------------------------
# Calculator
# ---------------------------------------------------------------------------

class FinishesCalculator:
    """
    Calculates all architectural finish QTO items according to the
    exact formulas specified in the QTO system requirements.
    """

    # ------------------------------------------------------------------
    # 1. Wet Areas Flooring
    # ------------------------------------------------------------------
    def calculate_wet_areas_flooring(
        self,
        toilets: float = 0.0,
        bathrooms: float = 0.0,
        kitchens: float = 0.0,
        pantries: float = 0.0,
        laundry: float = 0.0,
    ) -> dict:
        """
        Wet Areas Flooring = toilets + bathrooms + kitchens + pantries + laundry
        """
        total = toilets + bathrooms + kitchens + pantries + laundry
        return {
            "area_m2": round(total, 3),
            "breakdown": {
                "toilets": round(toilets, 3),
                "bathrooms": round(bathrooms, 3),
                "kitchens": round(kitchens, 3),
                "pantries": round(pantries, 3),
                "laundry": round(laundry, 3),
            },
        }

    # ------------------------------------------------------------------
    # 2. Wall Tiles
    # ------------------------------------------------------------------
    def calculate_wall_tiles(
        self, wet_area_perimeter: float, floor_height: float
    ) -> dict:
        """
        Wall Tiles = perimeter_of_wet_areas × (floor_height - 0.50 m)
        The 0.50 m deduction accounts for the floor-tile band / threshold.
        """
        tiled_height = max(floor_height - 0.50, 0.0)
        area = wet_area_perimeter * tiled_height
        return {"area_m2": round(area, 3)}

    # ------------------------------------------------------------------
    # 3. Wet Areas Ceiling
    # ------------------------------------------------------------------
    def calculate_wet_areas_ceiling(self, wet_areas_flooring: float) -> dict:
        """
        Wet Areas Ceiling = Wet Areas Flooring  (1:1 relationship)
        """
        return {"area_m2": round(wet_areas_flooring, 3)}

    # ------------------------------------------------------------------
    # 4. Balcony Flooring
    # ------------------------------------------------------------------
    def calculate_balcony_flooring(self, balcony_area: float = 0.0) -> dict:
        """
        Balcony Flooring = balcony_area (if exists, else 0)
        """
        return {"area_m2": round(balcony_area, 3)}

    # ------------------------------------------------------------------
    # 5. Marble Threshold (RM)
    # ------------------------------------------------------------------
    def calculate_marble_threshold(self, door_widths: list[float]) -> dict:
        """
        Marble Threshold (RM) = sum of widths of all doors
        """
        total = sum(door_widths)
        return {"length_rm": round(total, 3)}

    # ------------------------------------------------------------------
    # 6. Block 20 External
    # ------------------------------------------------------------------
    def calculate_block_20_external(
        self,
        external_villa_length: float,
        floor_height: float,
        windows_area: float,
        main_door_area: float,
    ) -> dict:
        """
        Block 20 External = (external_villa_length × floor_height)
                            - (windows_areas + main_door_area)
        external_villa_length here means the total external wall perimeter.
        """
        gross_area = external_villa_length * floor_height
        deductions = windows_area + main_door_area
        net_area = max(gross_area - deductions, 0.0)
        return {
            "gross_area_m2": round(gross_area, 3),
            "deductions_m2": round(deductions, 3),
            "net_area_m2": round(net_area, 3),
        }

    # ------------------------------------------------------------------
    # 7. Block 20 Internal
    # ------------------------------------------------------------------
    def calculate_block_20_internal(
        self,
        total_length_20cm: float,
        floor_height: float,
        door_areas_sum: float,
    ) -> dict:
        """
        Block 20 Internal = (20cm internal walls × floor_height) - door openings in 20cm walls.
        Caller must pass the proportional share of internal door areas for 20cm walls.
        """
        gross_area = total_length_20cm * floor_height
        net_area = max(gross_area - door_areas_sum, 0.0)
        return {
            "gross_area_m2": round(gross_area, 3),
            "deduction_m2": round(door_areas_sum, 3),
            "net_area_m2": round(net_area, 3),
        }

    # ------------------------------------------------------------------
    # 8. Block 10 Internal
    # ------------------------------------------------------------------
    def calculate_block_10_internal(
        self,
        total_length_10cm: float,
        floor_height: float,
        door_areas_sum: float,
    ) -> dict:
        """
        Block 10 Internal = (10cm internal walls × floor_height) - door openings in 10cm walls.
        Caller must pass the proportional share of internal door areas for 10cm walls.
        """
        gross_area = total_length_10cm * floor_height
        net_area = max(gross_area - door_areas_sum, 0.0)
        return {
            "gross_area_m2": round(gross_area, 3),
            "deduction_m2": round(door_areas_sum, 3),
            "net_area_m2": round(net_area, 3),
        }

    # ------------------------------------------------------------------
    # 9. Internal Plaster
    # ------------------------------------------------------------------
    def calculate_internal_plaster(
        self,
        internal_walls_area: float,
        external_walls_area: float,
        doors_area: float,
        windows_area: float,
    ) -> dict:
        """
        Internal Plaster = ((internal_walls_area × 2) + external_walls_area)
                           - (doors_area × 2 + windows_area)
        The ×2 for internal walls accounts for plastering both faces.
        The ×2 for doors deducts both sides of the opening.
        """
        gross = (internal_walls_area * 2.0) + external_walls_area
        deductions = (doors_area * 2.0) + windows_area
        net = max(gross - deductions, 0.0)
        return {
            "gross_area_m2": round(gross, 3),
            "deductions_m2": round(deductions, 3),
            "net_area_m2": round(net, 3),
        }

    # ------------------------------------------------------------------
    # 10. External Villa Walls Finish
    # ------------------------------------------------------------------
    def calculate_external_villa_walls_finish(
        self,
        external_perimeter: float,
        floor_count: int = 2,
        floor_height: float = 3.0,
        parapet_height: float = 1.5,
    ) -> dict:
        """
        External Villa Walls Finish = external_perimeter × (2 floors height + 1.5 m)
        The formula uses 2 standard floors + a parapet / top band of 1.5 m.
        """
        total_height = (floor_count * floor_height) + parapet_height
        area = external_perimeter * total_height
        return {
            "total_height_m": round(total_height, 3),
            "area_m2": round(area, 3),
        }

    # ------------------------------------------------------------------
    # 11. Waterproofing
    # ------------------------------------------------------------------
    def calculate_waterproofing(
        self, first_floor_wet_area: float
    ) -> dict:
        """
        Waterproofing = all 1st floor wet areas (toilets, bathrooms, kitchen, laundry)
        Balcony waterproofing is handled separately via calculate_balcony_waterproofing.
        """
        return {"area_m2": round(first_floor_wet_area, 3)}

    # ------------------------------------------------------------------
    # 11b. Balcony Waterproofing
    # ------------------------------------------------------------------
    def calculate_balcony_waterproofing(self, balcony_area: float = 0.0) -> dict:
        """
        Balcony Waterproofing = balcony_area (liquid membrane on balcony deck)
        """
        return {"area_m2": round(balcony_area, 3)}

    # ------------------------------------------------------------------
    # 12. Combo Roof System
    # ------------------------------------------------------------------
    def calculate_combo_roof_system(self, roof_slab_area: float) -> dict:
        """
        Combo Roof System = roof_slab_area × 1.2  (20% uplift for upturns/overlaps)
        """
        area = roof_slab_area * 1.2
        return {"area_m2": round(area, 3)}

    # ------------------------------------------------------------------
    # 13. Openings (Doors & Windows)
    # ------------------------------------------------------------------
    def calculate_openings(self, openings: list[dict | Opening]) -> dict:
        """
        Each opening: area = width × height × count
        Returns total door area, total window area, and per-opening breakdown.
        """
        total_door_area = 0.0
        total_window_area = 0.0
        details = []

        for o in openings:
            otype = o.get("opening_type", o.get("type", "")) if isinstance(o, dict) else o.opening_type
            w = o["width"] if isinstance(o, dict) else o.width
            h = o["height"] if isinstance(o, dict) else o.height
            n = o.get("count", 1) if isinstance(o, dict) else o.count

            area = w * h * n
            details.append({
                "type": otype,
                "width": w,
                "height": h,
                "count": n,
                "area_m2": round(area, 3),
            })

            if otype.lower() == "door":
                total_door_area += area
            else:
                total_window_area += area

        return {
            "total_door_area_m2": round(total_door_area, 3),
            "total_window_area_m2": round(total_window_area, 3),
            "total_area_m2": round(total_door_area + total_window_area, 3),
            "details": details,
        }

    # ------------------------------------------------------------------
    # 14. Thermal Block External
    # ------------------------------------------------------------------
    def calculate_thermal_block_external(
        self,
        schedule_area: float | None = None,
        plot_area: float | None = None,
        project_type: str = "G+1",
        averages: dict | None = None,
    ) -> dict:
        """
        Use schedule area if provided; otherwise scale from average data.
        """
        if schedule_area is not None:
            return {"area_m2": round(schedule_area, 3), "source": "schedule"}

        avg_area = self._lookup_average(
            "thermal_block_external", project_type, averages
        )
        if avg_area and plot_area:
            avg_plot = self._lookup_avg_plot(project_type, averages)
            scaled = avg_area * (plot_area / avg_plot) if avg_plot else avg_area
            return {"area_m2": round(scaled, 3), "source": "scaled_average"}

        return {"area_m2": 0.0, "source": "unavailable"}

    # ------------------------------------------------------------------
    # 15. Interlock Paving
    # ------------------------------------------------------------------
    def calculate_interlock_paving(
        self,
        schedule_area: float | None = None,
        plot_area: float | None = None,
        project_type: str = "G+1",
        averages: dict | None = None,
    ) -> dict:
        """
        Use schedule area if provided; otherwise scale from average data.
        """
        if schedule_area is not None:
            return {"area_m2": round(schedule_area, 3), "source": "schedule"}

        avg_area = self._lookup_average(
            "interlock_paving", project_type, averages
        )
        if avg_area and plot_area:
            avg_plot = self._lookup_avg_plot(project_type, averages)
            scaled = avg_area * (plot_area / avg_plot) if avg_plot else avg_area
            return {"area_m2": round(scaled, 3), "source": "scaled_average"}

        return {"area_m2": 0.0, "source": "unavailable"}

    # ------------------------------------------------------------------
    # 16. False Ceiling
    # ------------------------------------------------------------------
    def calculate_false_ceiling(
        self,
        schedule_area: float | None = None,
        plot_area: float | None = None,
        project_type: str = "G+1",
        averages: dict | None = None,
    ) -> dict:
        """
        Use schedule area if provided; otherwise scale from average data.
        """
        if schedule_area is not None:
            return {"area_m2": round(schedule_area, 3), "source": "schedule"}

        avg_area = self._lookup_average(
            "false_ceiling", project_type, averages
        )
        if avg_area and plot_area:
            avg_plot = self._lookup_avg_plot(project_type, averages)
            scaled = avg_area * (plot_area / avg_plot) if avg_plot else avg_area
            return {"area_m2": round(scaled, 3), "source": "scaled_average"}

        return {"area_m2": 0.0, "source": "unavailable"}

    # ------------------------------------------------------------------
    # 17. Roof Waterproofing
    # ------------------------------------------------------------------
    def calculate_roof_waterproofing(
        self,
        schedule_area: float | None = None,
        plot_area: float | None = None,
        project_type: str = "G+1",
        averages: dict | None = None,
    ) -> dict:
        """
        Use schedule area if provided; otherwise scale from average data.
        """
        if schedule_area is not None:
            return {"area_m2": round(schedule_area, 3), "source": "schedule"}

        avg_area = self._lookup_average(
            "roof_waterproofing", project_type, averages
        )
        if avg_area and plot_area:
            avg_plot = self._lookup_avg_plot(project_type, averages)
            scaled = avg_area * (plot_area / avg_plot) if avg_plot else avg_area
            return {"area_m2": round(scaled, 3), "source": "scaled_average"}

        return {"area_m2": 0.0, "source": "unavailable"}

    # ------------------------------------------------------------------
    # 18. External Paint
    # ------------------------------------------------------------------
    def calculate_external_paint(self, external_walls_area: float) -> dict:
        """
        External paint applied over rendered external walls.
        Area = external_villa_walls_finish_area  (same surface)
        """
        return {"area_m2": round(external_walls_area, 3)}

    # ------------------------------------------------------------------
    # 19. Roof Screed (for drainage falls)
    # ------------------------------------------------------------------
    def calculate_roof_screed(self, roof_area: float) -> dict:
        """
        Sand-cement screed on roof slab to form drainage falls.
        Area = roof_area  (measured on plan)
        """
        return {"area_m2": round(roof_area, 3)}

    # ------------------------------------------------------------------
    # 20. Damp Proof Course (DPC)
    # ------------------------------------------------------------------
    def calculate_dpc(
        self,
        external_perimeter: float,
        internal_wall_length_20cm: float = 0.0,
        internal_wall_length_10cm: float = 0.0,
    ) -> dict:
        """
        Hessian-based DPC applied at base of all walls at ground floor level.
        Length (RM) = external_perimeter + all internal wall lengths
        """
        total = external_perimeter + internal_wall_length_20cm + internal_wall_length_10cm
        return {"length_rm": round(total, 3)}

    # ------------------------------------------------------------------
    # 21. Kitchen / Pantry Countertop
    # ------------------------------------------------------------------
    def calculate_kitchen_countertop(self, rooms: list) -> dict:
        """
        Granite / stone countertop in running metres.
        Standard allowance: kitchen = 3.5 RM, pantry = 2.5 RM each.
        """
        total = 0.0
        for room in rooms:
            rt = (
                room.get("room_type", "").lower()
                if isinstance(room, dict)
                else room.room_type.lower()
            )
            if rt == "kitchen":
                total += 3.5
            elif rt == "pantry":
                total += 2.5
        return {"length_rm": round(total, 3)}

    # ------------------------------------------------------------------
    # 22. Boundary Wall — Block Work
    # ------------------------------------------------------------------
    def calculate_boundary_wall(
        self,
        boundary_perimeter: float,
        boundary_wall_height: float = 2.5,
    ) -> dict:
        """
        Plot boundary wall.
        Area = boundary_perimeter × wall_height (default 2.5 m privacy wall)
        """
        area = boundary_perimeter * boundary_wall_height
        return {"area_m2": round(area, 3)}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _lookup_average(
        item_key: str, project_type: str, averages: dict | None
    ) -> float | None:
        if averages is None:
            return None
        pt_data = averages.get(project_type, {})
        item = pt_data.get("items", {}).get(item_key, {})
        return item.get("value") if item else None

    @staticmethod
    def _lookup_avg_plot(project_type: str, averages: dict | None) -> float | None:
        if averages is None:
            return None
        meta = averages.get(project_type, {}).get("meta", {})
        return meta.get("avg_plot_area")
