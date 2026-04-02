"""
QTO Validator — compares calculated quantities against historical averages,
flags items that deviate beyond the allowed tolerance, and computes
a per-item and overall confidence score.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_json(relative_path: str) -> dict:
    base = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    full = os.path.join(base, relative_path)
    with open(full, encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Mapping from BOQ description substrings → averages.json item keys
# Order matters: more specific patterns must come before broad ones.
# ---------------------------------------------------------------------------

_DESC_TO_KEY: dict[str, str] = {
    # Sub-structure — ordered specific → general
    "pcc — footings":               "foundation_pcc",
    "pcc — tie beams":              "tie_beam_pcc",
    "footing concrete":             "foundation_concrete",
    "neck column concrete":         "neck_column_concrete",
    "tie beam concrete":            "tie_beam_concrete",
    "bitumen waterproofing":        "foundation_bitumen",
    "solid block wall":             "solid_block_work",
    "slab on grade":                "slab_on_grade_concrete",
    "excavation":                   "excavation",
    "back fill":                    "back_filling",
    "anti-termite":                 "anti_termite",
    "polythene sheet":              "polyethylene_sheet",
    "road base":                    "road_base",
    # Super-structure — specific before generic
    "staircase concrete":           "staircase_concrete",
    "parapet — concrete":           "parapet_concrete",
    "parapet — block":              "parapet_block",
    "columns —":                    "column_concrete",
    "beams —":                      "beam_concrete",
    "slab —":                       "slab_concrete",
    # Finishes — per-floor items (substring match works for GF/FF variants)
    "block 20cm — external":        "thermal_block_external",
    "block 20cm — internal":        "block_20_internal",
    "block 10cm":                   "block_10_internal",
    "internal plaster":             "internal_plaster",
    "flooring — dry":               "dry_area_flooring",
    "flooring — wet":               "wet_area_flooring",
    "skirting":                     "skirting",
    "paint — internal":             "internal_paint",
    "ceiling — dry":                "ceiling_spray_plaster",
    "ceiling — wet":                "ceiling_wet",
    "wall tiles":                   "wall_tiles",
    # External / whole-building
    "external villa walls finish":  "external_plaster",
    "external paint":               "external_paint",
    "marble threshold":             "marble_threshold",
    "balcony flooring":             "balcony_flooring",
    "balcony waterproofing":        "balcony_waterproofing",
    "waterproofing — 1st floor":    "wet_area_waterproofing",
    "combo roof system":            "roof_waterproofing",
    "roof waterproofing":           "roof_waterproofing",
    # Scaled-from-average items (for when added later)
    "thermal block":                "thermal_block_external",
    "false ceiling":                "false_ceiling",
    "interlock paving":             "interlock_paving",
}


def _description_to_key(description: str) -> str | None:
    dl = description.lower()
    for substr, key in _DESC_TO_KEY.items():
        if substr in dl:
            return key
    return None


# ---------------------------------------------------------------------------
# Confidence cap by sample count — prevents 100% confidence on sparse data
# ---------------------------------------------------------------------------

def _n_confidence_cap(n: int) -> float:
    """Return the maximum achievable confidence for a reference with n samples."""
    if n == 0:
        return 70.0    # estimated / no real measurement
    if n == 1:
        return 80.0    # single data point
    if n < 5:
        return 90.0    # very few projects
    if n < 10:
        return 95.0    # moderate sample
    return 100.0       # well-sampled (n ≥ 10)


# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    item_name: str
    calculated_qty: float
    unit: str
    average_qty: float | None
    scaled_average: float | None
    deviation_pct: float | None       # None if no reference available
    confidence: float                 # 0–100
    flag: str                         # "GREEN" | "YELLOW" | "RED"
    requires_manual_review: bool
    note: str = ""


@dataclass
class RatioValidationResult:
    ratio_name: str
    expected_ratio: float
    actual_ratio: float
    deviation_pct: float
    flag: str
    note: str = ""


@dataclass
class ValidationReport:
    project_type: str
    plot_area: float
    item_results: list[ValidationResult] = field(default_factory=list)
    ratio_results: list[RatioValidationResult] = field(default_factory=list)
    overall_confidence: float = 0.0
    is_draft: bool = True
    summary: str = ""


# ---------------------------------------------------------------------------
# Validator class
# ---------------------------------------------------------------------------

class QTOValidator:
    """
    Validates BOQ results against historical average data.
    """

    def __init__(self):
        self._averages = _load_json("config/averages.json")
        self._thresholds = _load_json("config/thresholds.json")

    @property
    def overall_confidence_threshold(self) -> float:
        """The minimum overall confidence required for a FINAL (non-draft) output."""
        return float(self._thresholds.get("overall_confidence_threshold", 75.0))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_item(
        self,
        item_name: str,
        calculated_qty: float,
        unit: str,
        project_type: str,
        plot_area: float,
    ) -> ValidationResult:
        """
        Compare a single BOQ item against the scaled historical average.

        Confidence logic (three layers, applied in order):
        1. Deviation from scaled mean → linear decay 100%→0% over 2×tolerance.
        2. Range boost: if the quantity falls within the historical [min, max]
           band (scaled to the project plot area), confidence is floored at 85%.
           This reflects that a value within observed real-project bounds is
           reasonable even if it deviates from the mean.
        3. Sample-count cap: prevents 100% confidence when the reference is
           backed by very few projects (n=0 estimated → cap 70%, n=1 → 80%, etc.).
        """
        tol = self._thresholds["range_tolerance"]
        conf_threshold = self._thresholds["confidence_threshold"]

        avg_qty, avg_plot, n, min_qty, max_qty = self._get_average(item_name, project_type)

        if avg_qty is None:
            return ValidationResult(
                item_name=item_name,
                calculated_qty=calculated_qty,
                unit=unit,
                average_qty=None,
                scaled_average=None,
                deviation_pct=None,
                confidence=100.0,
                flag="GREEN",
                requires_manual_review=False,
                note="No reference average available",
            )

        # Scale all reference values by the plot-area ratio
        ratio = (plot_area / avg_plot) if (avg_plot and avg_plot > 0) else 1.0
        scaled = avg_qty * ratio
        scaled_min = (min_qty * ratio) if min_qty is not None else None
        scaled_max = (max_qty * ratio) if max_qty is not None else None

        deviation = abs(calculated_qty - scaled) / scaled if scaled > 0 else 0.0
        deviation_pct = deviation * 100.0

        # Layer 1: deviation-based confidence
        raw_conf = max(0.0, 1.0 - (deviation / (2.0 * tol))) * 100.0

        # Layer 2: in-range boost — value is within observed historical bounds
        if scaled_min is not None and scaled_max is not None and n > 0:
            if scaled_min <= calculated_qty <= scaled_max:
                raw_conf = max(raw_conf, 90.0)

        # Layer 3: sample-count cap
        confidence = round(min(raw_conf, _n_confidence_cap(n)), 1)

        flag = self._flag(confidence)
        requires_review = confidence < conf_threshold

        note = ""
        if requires_review:
            note = (
                f"Deviation {deviation_pct:.1f}% from scaled average "
                f"{scaled:.1f} {unit} (n={n}). REQUIRES MANUAL REVIEW."
            )
        elif n < 5:
            note = f"Reference based on only {n} project(s) — treat with caution."

        return ValidationResult(
            item_name=item_name,
            calculated_qty=calculated_qty,
            unit=unit,
            average_qty=avg_qty,
            scaled_average=round(scaled, 3),
            deviation_pct=round(deviation_pct, 2),
            confidence=confidence,
            flag=flag,
            requires_manual_review=requires_review,
            note=note,
        )

    # ------------------------------------------------------------------
    def validate_ratios(
        self, boq_results: list[dict], project_type: str
    ) -> list[RatioValidationResult]:
        """
        Check ratio relationships between items (e.g. external plaster ≈ 1.54×
        thermal block for G+1).
        """
        ratio_configs = self._thresholds.get("ratio_checks", {}).get(project_type, {})
        results: list[RatioValidationResult] = []

        # Build a lookup of key → quantity from the BOQ
        # Per-floor items (GF + FF) are SUMMED to whole-building totals
        key_to_qty: dict[str, float] = {}
        for item in boq_results:
            key = _description_to_key(item.get("description", ""))
            if key:
                key_to_qty[key] = key_to_qty.get(key, 0.0) + item.get("quantity", 0.0)

        for ratio_name, expected_ratio in ratio_configs.items():
            parts = ratio_name.split("_to_")
            if len(parts) != 2:
                continue
            numerator_key, denominator_key = parts
            num_qty = key_to_qty.get(numerator_key, 0.0)
            den_qty = key_to_qty.get(denominator_key, 0.0)

            if den_qty == 0.0:
                results.append(RatioValidationResult(
                    ratio_name=ratio_name,
                    expected_ratio=expected_ratio,
                    actual_ratio=0.0,
                    deviation_pct=100.0,
                    flag="RED",
                    note=f"Denominator '{denominator_key}' is zero or not found.",
                ))
                continue

            actual_ratio = num_qty / den_qty
            deviation_pct = abs(actual_ratio - expected_ratio) / expected_ratio * 100.0
            tol_pct = self._thresholds["range_tolerance"] * 100.0
            flag = "GREEN" if deviation_pct <= tol_pct else (
                "YELLOW" if deviation_pct <= tol_pct * 1.5 else "RED"
            )

            results.append(RatioValidationResult(
                ratio_name=ratio_name,
                expected_ratio=expected_ratio,
                actual_ratio=round(actual_ratio, 3),
                deviation_pct=round(deviation_pct, 2),
                flag=flag,
                note=(
                    f"Expected {expected_ratio:.2f}, got {actual_ratio:.2f} "
                    f"(dev {deviation_pct:.1f}%)"
                ),
            ))

        return results

    # ------------------------------------------------------------------
    def validate_all(
        self,
        boq_results: list[dict],
        project_type: str,
        plot_area: float,
    ) -> ValidationReport:
        """
        Run full validation on a BOQ result list.
        Returns a ValidationReport with all item results, ratio results,
        and overall confidence.
        """
        report = ValidationReport(
            project_type=project_type,
            plot_area=plot_area,
        )

        for item in boq_results:
            vr = self.validate_item(
                item_name=item.get("description", ""),
                calculated_qty=item.get("quantity", 0.0),
                unit=item.get("unit", ""),
                project_type=project_type,
                plot_area=plot_area,
            )
            report.item_results.append(vr)

        report.ratio_results = self.validate_ratios(boq_results, project_type)

        # Weighted overall confidence (items with reference data carry full weight)
        with_ref = [r for r in report.item_results if r.average_qty is not None]
        if with_ref:
            overall = sum(r.confidence for r in with_ref) / len(with_ref)
        else:
            overall = 100.0

        report.overall_confidence = round(overall, 2)
        overall_threshold = self._thresholds["overall_confidence_threshold"]
        report.is_draft = report.overall_confidence < overall_threshold

        status = "DRAFT" if report.is_draft else "FINAL"
        report.summary = (
            f"Overall BOQ confidence: {report.overall_confidence:.1f}% — {status}. "
            f"Items validated: {len(report.item_results)}. "
            f"Items requiring manual review: "
            f"{sum(1 for r in report.item_results if r.requires_manual_review)}."
        )

        return report

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_average(
        self, item_name: str, project_type: str
    ) -> tuple[float | None, float | None, int, float | None, float | None]:
        """Return (avg_qty, avg_plot_area, n, min_qty, max_qty) for the item."""
        key = _description_to_key(item_name)
        if key is None:
            return None, None, 0, None, None

        pt_data = self._averages.get(project_type, {})
        item_data = pt_data.get("items", {}).get(key, {})
        avg_qty = item_data.get("value")
        avg_plot = pt_data.get("meta", {}).get("avg_plot_area")
        n = item_data.get("n", 0)
        min_qty = item_data.get("min")
        max_qty = item_data.get("max")
        return avg_qty, avg_plot, n, min_qty, max_qty

    def _flag(self, confidence: float) -> str:
        ct = self._thresholds["color_thresholds"]
        if confidence >= ct["green"]:
            return "GREEN"
        if confidence >= ct["yellow"]:
            return "YELLOW"
        return "RED"
