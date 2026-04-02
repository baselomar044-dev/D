"""
QTO Engine — orchestrates all calculators from a DrawingData input dict
and returns a complete BOQ with 50+ items.
"""

from __future__ import annotations

import json
import math
import os
from typing import Any

from src.engine.sub_structure import SubStructureCalculator
from src.engine.super_structure import SuperStructureCalculator
from src.engine.finishes import FinishesCalculator
from src.validation.validator import QTOValidator


# ---------------------------------------------------------------------------
# Config loader helpers
# ---------------------------------------------------------------------------

def _load_json(relative_path: str) -> dict:
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    full = os.path.join(base, relative_path)
    with open(full, encoding="utf-8") as fh:
        return json.load(fh)


def _load_averages() -> dict:
    return _load_json("config/averages.json")


def _load_rates() -> dict:
    return _load_json("config/rates.json")


# ---------------------------------------------------------------------------
# BOQ line item builder
# ---------------------------------------------------------------------------

def _item(
    item_no: str,
    description: str,
    unit: str,
    quantity: float,
    category: str,
    rate_key: str | None = None,
    rates: dict | None = None,
    confidence_note: str = "",
) -> dict:
    rates = rates or {}
    rate = 0.0
    if rate_key:
        for section in rates.values():
            if isinstance(section, dict) and rate_key in section:
                rate = section[rate_key]
                break
    amount = round(quantity * rate, 2)
    return {
        "item_no": item_no,
        "description": description,
        "unit": unit,
        "quantity": round(quantity, 3),
        "rate": rate,
        "amount": amount,
        "category": category,
        "confidence_note": confidence_note,
    }


# ---------------------------------------------------------------------------
# Main Engine
# ---------------------------------------------------------------------------

class QTOEngine:
    """
    Orchestrates sub-structure, super-structure and finishes calculators
    from a normalised DrawingData dictionary.
    """

    def __init__(self):
        self.sub_calc = SubStructureCalculator()
        self.sup_calc = SuperStructureCalculator()
        self.fin_calc = FinishesCalculator()
        self._averages = _load_averages()
        self._rates = _load_rates()
        self._validator = QTOValidator()

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_input(data: dict) -> dict:
        """
        Accept multiple input formats and normalise into the internal schema.
        Handles the sample_input.json format (rooms dict, walls dict, levels dict)
        as well as the DrawingData format from parsers.
        """
        d = dict(data)  # shallow copy so we don't mutate the original

        # --- Levels ---
        levels = d.pop("levels", None)
        if isinstance(levels, dict):
            d.setdefault("exc_depth", levels.get("excavation_depth", 1.5))
            d.setdefault("gfsl_level", levels.get("gfsl_level", 0.30))
            d.setdefault("gfl", levels.get("gfsl_level", 0.30))
            d.setdefault("tb_depth", levels.get("tb_depth", 0.40))
            d.setdefault("pcc_thickness", levels.get("pcc_thickness", 0.10))
            if "staircase_steps" in levels:
                d.setdefault("staircase_steps", levels["staircase_steps"])

        # --- Floors → floor_height, gf_floor_area, ff_floor_area ---
        floors = d.get("floors", [])
        if floors:
            d.setdefault("floor_height", floors[0].get("floor_height", 3.0))
            d.setdefault("slab_thickness", floors[0].get("slab_thickness", 0.20))
            if len(floors) >= 1:
                d.setdefault("gf_floor_area", floors[0].get("area", 0))
            if len(floors) >= 2:
                d.setdefault("ff_floor_area", floors[1].get("area", 0))
                d.setdefault("total_floor_area", floors[0].get("area", 0) + floors[1].get("area", 0))

        # --- Walls: convert {external_perimeter, internal_wall_length_20cm, ...} → list ---
        walls_raw = d.get("walls")
        if isinstance(walls_raw, dict):
            wall_list = []
            ext_p = walls_raw.get("external_perimeter", 0)
            int20 = walls_raw.get("internal_wall_length_20cm", 0)
            int10 = walls_raw.get("internal_wall_length_10cm", 0)
            if ext_p:
                wall_list.append({"type": "external", "length": ext_p})
            if int20:
                wall_list.append({"type": "internal_20", "length": int20})
            if int10:
                wall_list.append({"type": "internal_10", "length": int10})
            d["walls"] = wall_list
            d.setdefault("external_perimeter", ext_p)
            d.setdefault("internal_wall_length_20cm", int20)
            d.setdefault("internal_wall_length_10cm", int10)

        # --- Rooms: convert {ground_floor:[], first_floor:[]} → flat lists ---
        rooms_raw = d.get("rooms")
        if isinstance(rooms_raw, dict):
            gf_rooms = []
            for rm in rooms_raw.get("ground_floor", []):
                gf_rooms.append({
                    "name": rm.get("name", ""),
                    "area": rm.get("area", 0),
                    "perimeter": rm.get("perimeter", 0),
                    "room_type": rm.get("type", "dry"),
                })
            d["gf_rooms"] = gf_rooms
            # 'rooms' must be a flat list of all rooms (GF + FF) for whole-building queries
            d["rooms"] = gf_rooms  # will be extended below with ff_rooms

            ff_rooms = []
            for rm in rooms_raw.get("first_floor", []):
                ff_rooms.append({
                    "name": rm.get("name", ""),
                    "area": rm.get("area", 0),
                    "perimeter": rm.get("perimeter", 0),
                    "room_type": rm.get("type", "dry"),
                })
            d["first_floor_rooms"] = ff_rooms
            d["rooms"] = gf_rooms + ff_rooms  # flat list of all rooms

            # Compute dry perimeters
            wet_types = {"toilet", "bathroom", "kitchen", "pantry", "laundry", "wet"}
            gf_dry_peri = sum(rm["perimeter"] for rm in gf_rooms if rm["room_type"] not in wet_types)
            ff_dry_peri = sum(rm["perimeter"] for rm in ff_rooms if rm["room_type"] not in wet_types)
            d.setdefault("gf_dry_area_perimeter", gf_dry_peri)
            d.setdefault("ff_dry_area_perimeter", ff_dry_peri)
            d.setdefault("dry_area_perimeter", gf_dry_peri + ff_dry_peri)

        return d

    # ------------------------------------------------------------------
    def run(self, data: dict) -> list[dict]:
        """
        Entry point.  `data` must conform to the DrawingData / sample_input schema.
        Returns a list of BOQ line-item dicts.
        """
        data = self._normalize_input(data)
        boq: list[dict] = []
        project_type = data.get("project_type", "G+1")
        plot_area = data.get("plot_area", 153.0)
        floor_height = data.get("floor_height", 3.0)

        # Project meta
        gfl = data.get("gfl", 0.30)          # Ground floor level (m above excavation datum)
        exc_depth = data.get("exc_depth", 1.50)
        tb_depth = data.get("tb_depth", 0.40)
        pcc_thickness = data.get("pcc_thickness", 0.10)
        gfsl_level = data.get("gfsl_level", 0.30)
        slab_thickness = data.get("slab_thickness", 0.20)

        # ------------------------------------------------------------------
        # SUB-STRUCTURE
        # ------------------------------------------------------------------
        boq.extend(self._calc_sub_structure(
            data, gfl, exc_depth, tb_depth, pcc_thickness, gfsl_level
        ))

        # ------------------------------------------------------------------
        # SUPER-STRUCTURE
        # ------------------------------------------------------------------
        boq.extend(self._calc_super_structure(
            data, floor_height, slab_thickness, project_type
        ))

        # ------------------------------------------------------------------
        # FINISHES
        # ------------------------------------------------------------------
        boq.extend(self._calc_finishes(
            data, floor_height, project_type, plot_area
        ))

        # ------------------------------------------------------------------
        # CONFIDENCE FILTER — items below 90% get smart statistical estimation
        # rather than silent exclusion, so the BOQ is always complete.
        # ------------------------------------------------------------------
        exclusion_threshold = self._validator._thresholds.get("exclusion_threshold", 90.0)
        validated: list[dict] = []
        for item in boq:
            vr = self._validator.validate_item(
                item_name=item["description"],
                calculated_qty=item["quantity"],
                unit=item["unit"],
                project_type=project_type,
                plot_area=plot_area,
            )
            item["confidence"] = vr.confidence
            item["flag"] = vr.flag
            item["estimated"] = False

            if vr.confidence >= exclusion_threshold:
                validated.append(item)
            elif vr.scaled_average is not None and vr.scaled_average > 0:
                # Smart estimation: swap in the scaled historical average so the
                # BOQ remains complete; record the original calculated quantity.
                item["original_qty"] = item["quantity"]
                item["quantity"] = round(vr.scaled_average, 3)
                item["amount"] = round(item["quantity"] * item["rate"], 2)
                item["flag"] = "ESTIMATED"
                item["estimated"] = True
                item["confidence_note"] = (
                    f"Qty estimated from {project_type} historical average "
                    f"(calculated: {item['original_qty']:.3f} {item['unit']}, "
                    f"confidence {vr.confidence:.1f}%)"
                )
                validated.append(item)
            # else: no historical reference at all and confidence < 90% → truly
            # unknown quantity; exclude rather than invent a number.

        return validated

    # ==================================================================
    # SUB-STRUCTURE helpers
    # ==================================================================
    def _calc_sub_structure(
        self, data: dict, gfl: float, exc_depth: float,
        tb_depth: float, pcc_thickness: float, gfsl_level: float
    ) -> list[dict]:
        items: list[dict] = []
        r = self._rates

        footings = data.get("footings", [])
        neck_cols = data.get("neck_columns", [])
        tie_beams = data.get("tie_beams", [])
        solid_walls = data.get("solid_block_walls", [])
        gf_area = data.get("gf_area", data.get("plot_area", 153.0))
        longest_length = data.get("longest_length", 0.0)
        longest_width = data.get("longest_width", 0.0)
        has_road_base = data.get("has_road_base", False)
        road_base_thickness = data.get("road_base_thickness", 0.25)

        # Pre-calculate excavation so road_base can follow immediately
        plot_area = data.get("plot_area", 153.0)
        if longest_length == 0.0 or longest_width == 0.0:
            side = math.sqrt(plot_area)
            longest_length = longest_length or side * 1.2
            longest_width = longest_width or side * 0.9

        exc_res = self.sub_calc.calculate_excavation(longest_length, longest_width, exc_depth)
        exc_area = exc_res["area_m2"]

        # ---- ORDER matches user spec exactly ----
        # 1. Excavation
        items.append(_item("A.1", "Excavation", "m3",
                           exc_res["volume_m3"], "Sub-Structure",
                           "excavation", r))

        # 2. Road Base (optional — only present when has_road_base=True)
        if has_road_base:
            rb_res = self.sub_calc.calculate_road_base(exc_area, road_base_thickness)
            items.append(_item("A.2", "Road Base (Compacted)", "m3",
                               rb_res["volume_m3"], "Sub-Structure",
                               "road_base", r))

        # Collect all bitumen areas to combine into a single item later
        total_bitumen = 0.0
        f_pcc_area = 0.0
        tb_pcc_area = 0.0

        # Pre-calculate all foundation/column/beam values (not appended yet)
        f_pcc_vol = f_conc_vol = 0.0
        f_bitumen = 0.0
        if footings:
            f_res = self.sub_calc.calculate_foundation(footings)
            f_pcc_vol = f_res["pcc_volume_m3"]
            f_conc_vol = f_res["volume_m3"]
            f_bitumen = f_res["bitumen_area_m2"]
            total_bitumen += f_bitumen
            f_pcc_area = sum(
                (ff.get("length", 0) + 0.20) * (ff.get("width", 0) + 0.20) * ff.get("count", 1)
                for ff in footings
            )

        tb_pcc_vol = tb_conc_vol = 0.0
        tb_bitumen = 0.0
        if tie_beams:
            tb_res = self.sub_calc.calculate_tie_beams(tie_beams)
            tb_pcc_vol = tb_res["pcc_volume_m3"]
            tb_conc_vol = tb_res["volume_m3"]
            tb_bitumen = tb_res["bitumen_area_m2"]
            total_bitumen += tb_bitumen
            tb_pcc_area = sum(
                b.get("length", 0) * (b.get("width", 0) + 0.20) * b.get("count", 1)
                for b in tie_beams
            )

        nc_conc_vol = 0.0
        if neck_cols:
            nc_res = self.sub_calc.calculate_neck_columns(
                neck_cols, gfl, exc_depth, tb_depth, pcc_thickness
            )
            nc_conc_vol = nc_res["volume_m3"]

        sbw_area = sbw_bitumen = 0.0
        if solid_walls:
            sbw_res = self.sub_calc.calculate_solid_block_work(
                solid_walls, gfl, exc_depth, tb_depth, pcc_thickness
            )
            sbw_area = sbw_res["area_m2"]
            sbw_bitumen = sbw_res["bitumen_area_m2"]
            total_bitumen += sbw_bitumen

        sog_res = self.sub_calc.calculate_slab_on_grade(gf_area)
        sog_area = sog_res["area_m2"]

        # 3. pcc_footings
        if footings:
            items.append(_item("A.3", "PCC — Footings", "m3",
                               f_pcc_vol, "Sub-Structure", "foundation_pcc", r))

        # 4. pcc_tb
        if tie_beams:
            items.append(_item("A.4", "PCC — Tie Beams", "m3",
                               tb_pcc_vol, "Sub-Structure", "tie_beam_pcc", r))

        # 5. footing_concrete
        if footings:
            items.append(_item("A.5", "Footing Concrete (Grade C30)", "m3",
                               f_conc_vol, "Sub-Structure", "foundation_concrete", r))

        # 6. neck_column
        if neck_cols:
            items.append(_item("A.6", "Neck Column Concrete (Grade C30)", "m3",
                               nc_conc_vol, "Sub-Structure", "neck_column_concrete", r))

        # 7. tb_concrete
        if tie_beams:
            items.append(_item("A.7", "Tie Beam Concrete (Grade C30)", "m3",
                               tb_conc_vol, "Sub-Structure", "tie_beam_concrete", r))

        # 8. bitumen (combined)
        items.append(_item("A.8", "Bitumen Waterproofing", "m2",
                           total_bitumen, "Sub-Structure", "bitumen", r))

        # 9. blockwall_solid_sub
        if solid_walls:
            items.append(_item("A.9", "Solid Block Wall (Below Grade)", "m2",
                               sbw_area, "Sub-Structure", "solid_block_work", r))

        # 10. slab_on_grade
        items.append(_item("A.10", "Slab on Grade Concrete (Grade C30)", "m3",
                           sog_res["volume_m3"], "Sub-Structure",
                           "slab_on_grade_concrete", r))

        # 11. polythene
        total_pcc_area = f_pcc_area + tb_pcc_area
        ps_res = self.sub_calc.calculate_polyethylene_sheet(total_pcc_area, sog_area)
        items.append(_item("A.11", "Polythene Sheet (1000 gauge)", "m2",
                           ps_res["area_m2"], "Sub-Structure",
                           "polyethylene_sheet", r))

        # 12. anti_termite
        at_res = self.sub_calc.calculate_anti_termite(total_pcc_area, sog_area)
        items.append(_item("A.12", "Anti-Termite Treatment", "m2",
                           at_res["area_m2"], "Sub-Structure",
                           "anti_termite", r))

        # 13. backfill — deduct all concrete/masonry placed in the pit
        _EXCLUDE_BACKFILL = {"A.1"}
        all_vols = sum(
            it["quantity"] for it in items
            if it["unit"] == "m3"
            and it["category"] == "Sub-Structure"
            and it["item_no"] not in _EXCLUDE_BACKFILL
        )
        bf_res = self.sub_calc.calculate_back_filling(
            exc_area, exc_depth, gfsl_level, all_vols
        )
        items.append(_item("A.13", "Back Fill", "m3",
                           bf_res["net_volume_m3"], "Sub-Structure",
                           "back_filling", r))

        return items

    # ==================================================================
    # SUPER-STRUCTURE helpers
    # ==================================================================
    def _calc_super_structure(
        self, data: dict, floor_height: float, slab_thickness: float,
        project_type: str = "G+1"
    ) -> list[dict]:
        items: list[dict] = []
        r = self._rates

        # Per-floor structural inputs (with legacy fallbacks)
        gf_cols = data.get("gf_columns") or data.get("columns", [])
        ff_cols = data.get("ff_columns") or data.get("columns", [])

        slabs_list = data.get("slabs", [])
        ff_slabs = data.get("ff_slabs") or (slabs_list[:1] if slabs_list else [])
        roof_slabs = data.get("roof_slabs") or (slabs_list[1:] if len(slabs_list) > 1 else slabs_list)

        beams_list = data.get("beams", [])
        ff_beams = data.get("ff_beams") or beams_list
        roof_beams = data.get("roof_beams") or beams_list

        # Parapet parameters
        parapet_perimeter = data.get("parapet_perimeter",
                                     data.get("external_perimeter", 0.0))
        parapet_height = data.get("parapet_height", 1.0)
        parapet_thickness = data.get("parapet_thickness", 0.20)
        parapet_capping_h = data.get("parapet_capping_height", 0.20)

        # --- GF Columns ---
        if gf_cols:
            res = self.sup_calc.calculate_columns(gf_cols, floor_height)
            items.append(_item("B.1", "Columns — Ground Floor (Grade C30)", "m3",
                               res["volume_m3"], "Super-Structure", "gf_columns", r))

        # --- FF Columns ---
        if ff_cols and project_type != "G":
            res = self.sup_calc.calculate_columns(ff_cols, floor_height)
            items.append(_item("B.2", "Columns — First Floor (Grade C30)", "m3",
                               res["volume_m3"], "Super-Structure", "ff_columns", r))

        # --- FF Beams (before FF Slab per spec) ---
        if ff_beams and project_type != "G":
            res = self.sup_calc.calculate_beams(ff_beams, slab_thickness)
            items.append(_item("B.3", "Beams — First Floor (Grade C30)", "m3",
                               res["volume_m3"], "Super-Structure", "ff_beams", r))

        # --- FF Slab ---
        if ff_slabs and project_type != "G":
            res = self.sup_calc.calculate_slabs(ff_slabs)
            items.append(_item("B.4", "Slab — First Floor (Grade C30)", "m3",
                               res["volume_m3"], "Super-Structure", "ff_slab", r))

        # --- Roof Beams (before Roof Slab per spec) ---
        if roof_beams:
            res = self.sup_calc.calculate_beams(roof_beams, slab_thickness)
            items.append(_item("B.5", "Beams — Roof (Grade C30)", "m3",
                               res["volume_m3"], "Super-Structure", "roof_beams", r))

        # --- Roof Slab ---
        if roof_slabs:
            res = self.sup_calc.calculate_slabs(roof_slabs)
            items.append(_item("B.6", "Slab — Roof (Grade C30)", "m3",
                               res["volume_m3"], "Super-Structure", "roof_slab", r))

        # --- Parapet Concrete Capping (before Block per spec) ---
        if parapet_perimeter > 0:
            pc_res = self.sup_calc.calculate_parapet_concrete(
                parapet_perimeter, parapet_thickness, parapet_capping_h
            )
            items.append(_item("B.7", "Parapet — Concrete Capping (Grade C25)", "m3",
                               pc_res["volume_m3"], "Super-Structure",
                               "parapet_concrete", r))

            # --- Parapet Block Work ---
            pb_res = self.sup_calc.calculate_parapet_block(
                parapet_perimeter, parapet_height
            )
            items.append(_item("B.8", "Parapet — Block Work", "m2",
                               pb_res["area_m2"], "Super-Structure", "parapet_block", r))

        # --- Staircase Concrete ---
        stair_width = data.get("stair_width", 1.20)
        stair_count = data.get("stair_count", 0)
        if stair_count > 0:
            st_res = self.sup_calc.calculate_staircase(
                floor_height, stair_width, stair_count, slab_thickness
            )
            items.append(_item("B.9", "Staircase Concrete (Grade C30)", "m3",
                               st_res["volume_m3"], "Super-Structure",
                               "staircase_concrete", r))

        return items

    # ==================================================================
    # FINISHES helpers
    # ==================================================================
    def _calc_finishes(
        self, data: dict, floor_height: float, project_type: str, plot_area: float
    ) -> list[dict]:
        items: list[dict] = []
        r = self._rates
        fc = self.fin_calc

        openings_raw = data.get("openings", [])
        # Normalise openings: accept flat list OR {doors:[], windows:[]} dict
        if isinstance(openings_raw, dict):
            openings = []
            for d in openings_raw.get("doors", []):
                openings.append({**d, "opening_type": "door"})
            for w in openings_raw.get("windows", []):
                openings.append({**w, "opening_type": "window"})
        else:
            openings = list(openings_raw)

        # Pre-compute openings totals
        op_res = fc.calculate_openings(openings)
        windows_area = op_res["total_window_area_m2"]
        doors_area = op_res["total_door_area_m2"]
        main_door_area = data.get("main_door_area", 4.0)
        door_widths_all = [
            op.get("width", 0.9)
            for op in openings
            if op.get("opening_type", op.get("type", "")) == "door"
            for _ in range(op.get("count", 1))
        ]

        # ----------------------------------------------------------------
        # Per-floor finishes helper
        # ----------------------------------------------------------------
        def _floor_items(
            prefix: str,       # "C" for GF, "D" for FF
            floor_label: str,  # "Ground Floor" or "First Floor"
            rooms: list[dict],
            walls: list[dict],
            floor_area: float,
            dry_peri: float,
            floor_main_door_area: float = 0.0,
        ) -> list[dict]:
            result: list[dict] = []
            fl = floor_label

            # Derive wall lengths from walls list
            ext_walls = [w for w in walls if w.get("type", "") == "external"]
            int_walls_20 = [w for w in walls if w.get("type", "") == "internal_20"]
            int_walls_10 = [w for w in walls if w.get("type", "") == "internal_10"]
            ext_perim = sum(w.get("length", 0) for w in ext_walls) or data.get("external_perimeter", 0.0)
            len_20 = sum(w.get("length", 0) for w in int_walls_20) or data.get("internal_wall_length_20cm", 0.0)
            len_10 = sum(w.get("length", 0) for w in int_walls_10) or data.get("internal_wall_length_10cm", 0.0)

            # Wet / dry room breakdown
            wet_types = {"toilet", "bathroom", "kitchen", "pantry", "laundry"}
            wet_area = sum(rm.get("area", 0) for rm in rooms if rm.get("room_type", "").lower() in wet_types)
            wet_peri = sum(rm.get("perimeter", 0) for rm in rooms if rm.get("room_type", "").lower() in wet_types)
            dry_area = max(floor_area - wet_area, 0.0)

            suffix = "_gf" if prefix == "C" else "_ff"

            # Block 20 External
            b20e = fc.calculate_block_20_external(ext_perim, floor_height, windows_area, floor_main_door_area)
            result.append(_item(f"{prefix}.1", f"Block 20cm — External Walls ({fl})", "m2",
                                b20e["net_area_m2"], "Finishes", f"block_20_ext{suffix}", r))

            # Proportional door split for internal blocks
            internal_doors = max(doors_area - main_door_area, 0.0)
            total_int_len = len_20 + len_10
            if total_int_len > 0:
                doors_in_20 = internal_doors * (len_20 / total_int_len)
                doors_in_10 = internal_doors * (len_10 / total_int_len)
            else:
                doors_in_20 = doors_in_10 = 0.0

            # Block 20 Internal
            b20i = fc.calculate_block_20_internal(len_20, floor_height, doors_in_20)
            result.append(_item(f"{prefix}.2", f"Block 20cm — Internal Walls ({fl})", "m2",
                                b20i["net_area_m2"], "Finishes", f"block_20_int{suffix}", r))

            # Block 10 Internal
            b10i = fc.calculate_block_10_internal(len_10, floor_height, doors_in_10)
            result.append(_item(f"{prefix}.3", f"Block 10cm — Internal Walls ({fl})", "m2",
                                b10i["net_area_m2"], "Finishes", f"block_10_int{suffix}", r))

            # Internal Plaster
            int_wall_area = (len_20 + len_10) * floor_height
            ext_wall_area = ext_perim * floor_height
            ip = fc.calculate_internal_plaster(int_wall_area, ext_wall_area, doors_area, windows_area)
            result.append(_item(f"{prefix}.4", f"Internal Plaster ({fl})", "m2",
                                ip["net_area_m2"], "Finishes", f"plaster_int{suffix}", r))

            # Flooring Dry
            daf = self.sup_calc.calculate_dry_area_flooring(floor_area, wet_area)
            result.append(_item(f"{prefix}.5", f"Flooring — Dry Areas ({fl})", "m2",
                                daf["area_m2"], "Finishes", f"flooring_dry{suffix}", r))

            # Flooring Wet
            result.append(_item(f"{prefix}.6", f"Flooring — Wet Areas ({fl})", "m2",
                                round(wet_area, 3), "Finishes", f"flooring_wet{suffix}", r))

            # Skirting
            sk = self.sup_calc.calculate_skirting(dry_peri, door_widths_all)
            result.append(_item(f"{prefix}.7", f"Skirting ({fl})", "RM",
                                sk["area_m"], "Finishes", f"skirting{suffix}", r))

            # Paint
            pt = self.sup_calc.calculate_paint(sk["area_m"], floor_height)
            result.append(_item(f"{prefix}.8", f"Paint — Internal Walls ({fl})", "m2",
                                pt["area_m2"], "Finishes", f"paint{suffix}", r))

            # Ceiling Dry
            dac = self.sup_calc.calculate_dry_areas_ceiling(daf["area_m2"])
            result.append(_item(f"{prefix}.9", f"Ceiling — Dry Areas ({fl})", "m2",
                                dac["area_m2"], "Finishes", f"ceiling_dry{suffix}", r))

            # Ceiling Wet
            wac = fc.calculate_wet_areas_ceiling(wet_area)
            result.append(_item(f"{prefix}.10", f"Ceiling — Wet Areas ({fl})", "m2",
                                wac["area_m2"], "Finishes", f"ceiling_wet{suffix}", r))

            # Wall Tiles (wet areas)
            wt = fc.calculate_wall_tiles(wet_peri, floor_height)
            result.append(_item(f"{prefix}.11", f"Wall Tiles ({fl})", "m2",
                                wt["area_m2"], "Finishes", f"tiles_wall{suffix}", r))

            return result

        # GF rooms / walls
        gf_rooms = data.get("gf_rooms") or [
            rm for rm in data.get("rooms", [])
            if rm not in data.get("first_floor_rooms", [])
        ] or data.get("rooms", [])
        gf_walls = data.get("gf_walls") or data.get("walls", [])
        gf_floor_area = data.get("gf_floor_area", data.get("total_floor_area", data.get("plot_area", 153.0)))
        gf_dry_peri = data.get("gf_dry_area_perimeter", data.get("dry_area_perimeter", 0.0))

        items.extend(_floor_items("C", "Ground Floor", gf_rooms, gf_walls, gf_floor_area, gf_dry_peri, main_door_area))

        # FF rooms / walls (only for multi-storey projects)
        if project_type != "G":
            ff_rooms = data.get("first_floor_rooms", [])
            ff_walls = data.get("ff_walls") or data.get("walls", [])
            ff_floor_area = data.get("ff_floor_area", data.get("total_floor_area", data.get("plot_area", 153.0)))
            ff_dry_peri = data.get("ff_dry_area_perimeter", data.get("dry_area_perimeter", 0.0))
            items.extend(_floor_items("D", "First Floor", ff_rooms, ff_walls, ff_floor_area, ff_dry_peri, 0.0))

        # ----------------------------------------------------------------
        # External / whole-building finish items
        # ----------------------------------------------------------------

        # External Villa Walls Finish
        ext_perimeter = data.get("external_perimeter", 0.0)
        evwf = fc.calculate_external_villa_walls_finish(ext_perimeter)
        items.append(_item("E.1", "External Villa Walls Finish", "m2",
                           evwf["area_m2"], "Finishes", "finish_ext", r))

        # External Paint
        ep = fc.calculate_external_paint(evwf["area_m2"])
        items.append(_item("E.2", "External Paint", "m2",
                           ep["area_m2"], "Finishes", "external_paint", r))

        # Marble Threshold
        mt = fc.calculate_marble_threshold(door_widths_all)
        items.append(_item("E.3", "Marble Threshold", "RM",
                           mt["length_rm"], "Finishes", "marble_threshold", r))

        # Balcony Flooring
        balcony_area = sum(
            rm.get("area", 0) for rm in data.get("rooms", [])
            if rm.get("room_type", "").lower() == "balcony"
        )
        bf = fc.calculate_balcony_flooring(balcony_area)
        items.append(_item("E.4", "Balcony Flooring", "m2",
                           bf["area_m2"], "Finishes", "balcony_flooring", r))

        # Balcony Waterproofing
        balcony_wp = fc.calculate_balcony_waterproofing(balcony_area)
        items.append(_item("E.5", "Balcony Waterproofing", "m2",
                           balcony_wp["area_m2"], "Finishes", "balcony_wp", r))

        # Waterproofing (1st floor wet areas only)
        first_floor_wet = sum(
            rm.get("area", 0) for rm in data.get("first_floor_rooms", [])
            if rm.get("room_type", "").lower() in
            {"toilet", "bathroom", "kitchen", "pantry", "laundry"}
        ) or sum(
            rm.get("area", 0) for rm in data.get("rooms", [])
            if rm.get("room_type", "").lower() in
            {"toilet", "bathroom", "kitchen", "pantry", "laundry"}
        ) * 0.5
        wp = fc.calculate_waterproofing(first_floor_wet)
        items.append(_item("E.6", "Waterproofing \u2014 1st Floor Wet Areas", "m2",
                           wp["area_m2"], "Finishes", "waterproofing", r))

        # Roof Waterproofing
        avg = self._averages
        rw = fc.calculate_roof_waterproofing(
            data.get("roof_waterproofing_area"),
            plot_area, project_type, avg
        )
        items.append(_item("E.7", "Roof Waterproofing", "m2",
                           rw["area_m2"], "Finishes", "roof_waterproofing", r,
                           confidence_note=f"source={rw['source']}"))

        # Combo Roof System
        roof_area = data.get("roof_area", data.get("plot_area", 153.0))
        crs = fc.calculate_combo_roof_system(roof_area)
        items.append(_item("E.8", "Combo Roof System", "m2",
                           crs["area_m2"], "Finishes", "combo_roof", r))

        # Doors Schedule
        items.append(_item("E.9", "Doors (Schedule)", "m2",
                           op_res["total_door_area_m2"], "Finishes", "doors_schedule", r))

        # Windows Schedule
        items.append(_item("E.10", "Windows (Schedule)", "m2",
                           op_res["total_window_area_m2"], "Finishes", "windows_schedule", r))

        return items
