"""
QTO System — Command-line interface.

Accepts PDF and DWG/DXF architectural drawings only.

Usage
-----
python src/main.py --input drawing.dxf --type G+1 --output boq.xlsx --plot-area 153
python src/main.py --input drawing.dwg --type G+1 --output boq.xlsx --plot-area 153
python src/main.py --input plan.pdf   --type G+1 --output boq.xlsx --api-key YOUR_KEY
python src/main.py --input plan.pdf   --type G+1 --output boq.xlsx \\
                   --api-key YOUR_KEY --gemini-model gemini-2.5-pro --max-pages 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path


# Allow running as `python src/main.py` from the repo root
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


from src.engine.qto_engine import QTOEngine
from src.validation.validator import QTOValidator
from src.output.excel_generator import ExcelGenerator


# ---------------------------------------------------------------------------
# Sample data loader
# ---------------------------------------------------------------------------

def _load_sample() -> dict:
    sample_path = os.path.join(_ROOT, "samples", "sample_input.json")
    if not os.path.exists(sample_path):
        sys.exit(f"[ERROR] Sample file not found: {sample_path}")
    with open(sample_path, encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# File input router
# ---------------------------------------------------------------------------

def _parse_file(input_path: str, api_key: str | None, gemini_model: str, max_pages: int) -> dict:
    ext = Path(input_path).suffix.lower()
    if ext in {".dxf", ".dwg"}:
        try:
            from src.parsers.dxf_parser import parse_dxf
            return parse_dxf(input_path)
        except ImportError as exc:
            sys.exit(f"[ERROR] ezdxf not installed. Run: pip install ezdxf\n{exc}")
        except Exception as exc:  # noqa: BLE001
            if ext == ".dwg":
                sys.exit(
                    f"[ERROR] Could not open DWG file: {exc}\n"
                    "Tip: DWG files must first be converted to DXF.\n"
                    "Use the free ODA File Converter "
                    "(https://www.opendesign.com) then re-run with the .dxf file."
                )
            sys.exit(f"[ERROR] Failed to parse DXF file: {exc}")
    elif ext == ".pdf":
        if not api_key:
            api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            sys.exit(
                "[ERROR] A Gemini API key is required for PDF input. "
                "Use --api-key or set GEMINI_API_KEY."
            )
        try:
            from src.parsers.pdf_parser import parse_pdf
            return parse_pdf(input_path, api_key, model=gemini_model, max_pages=max_pages)
        except ImportError as exc:
            sys.exit(
                f"[ERROR] Required PDF libraries not installed.\n"
                f"Run: pip install google-generativeai pdf2image Pillow\n{exc}"
            )
    elif ext == ".json":
        # JSON is supported for development / sample runs only
        with open(input_path, encoding="utf-8") as fh:
            return json.load(fh)
    else:
        sys.exit(
            f"[ERROR] Unsupported file type '{ext}'.\n"
            "Accepted formats: .pdf  .dxf  .dwg"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="qto",
        description="QTO Automation System — generate a BOQ from architectural drawings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    input_group = p.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input", "-i",
        metavar="FILE",
        help="Path to a PDF or DXF/DWG architectural drawing.",
    )
    input_group.add_argument(
        "--sample",
        action="store_true",
        help="Use the built-in sample data (for development and testing).",
    )
    p.add_argument(
        "--type", "-t",
        dest="project_type",
        default="G+1",
        choices=["G", "G+1", "G+2", "G+1 Service"],
        help="Project type for validation averages (default: G+1).",
    )
    p.add_argument(
        "--output", "-o",
        metavar="FILE",
        default="boq_output.xlsx",
        help="Output Excel file path (default: boq_output.xlsx).",
    )
    p.add_argument(
        "--plot-area",
        type=float,
        metavar="M2",
        help="Plot area in m² (overrides value in input data if provided).",
    )
    p.add_argument(
        "--api-key",
        metavar="KEY",
        help="Google Gemini API key (required for PDF input).",
    )
    p.add_argument(
        "--gemini-model",
        metavar="MODEL",
        default="gemini-2.0-flash-lite",
        help=(
            "Gemini model for PDF extraction "
            "(default: gemini-2.0-flash-lite). "
            "Allowed: gemini-2.0-flash-lite, gemini-2.5-flash-lite."
        ),
    )
    p.add_argument(
        "--max-pages",
        type=int,
        metavar="N",
        default=3,
        help=(
            "Maximum PDF pages sent to Gemini (default: 3). "
            "Set to 0 to process all pages."
        ),
    )
    p.add_argument(
        "--project-name",
        metavar="NAME",
        default="Villa Project",
        help="Project name for the Excel header.",
    )
    p.add_argument(
        "--project-ref",
        metavar="REF",
        default="QTO-001",
        help="Project reference for the Excel header.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    print("[QTO] Loading input data …")

    if args.sample:
        data = _load_sample()
        print("[QTO] Using sample data from samples/sample_input.json")
    else:
        data = _parse_file(args.input, args.api_key, args.gemini_model, args.max_pages)
        print(f"[QTO] Parsed input file: {args.input}")

    # Allow CLI overrides of key project parameters
    if args.plot_area:
        data["plot_area"] = args.plot_area
    if args.project_type:
        data["project_type"] = args.project_type

    plot_area = data.get("plot_area", 153.0)
    project_type = data.get("project_type", "G+1")

    print(f"[QTO] Project type: {project_type}  |  Plot area: {plot_area} m²")

    # --- Run engine ---
    print("[QTO] Running QTO engine …")
    engine = QTOEngine()
    boq = engine.run(data)
    print(f"[QTO] Generated {len(boq)} BOQ line items.")

    # --- Validate ---
    print("[QTO] Running validation …")
    validator = QTOValidator()
    report = validator.validate_all(boq, project_type, plot_area)
    print(f"[QTO] {report.summary}")

    # --- Generate Excel ---
    print(f"[QTO] Generating Excel output: {args.output}")
    project_info = {
        "name": args.project_name,
        "ref": args.project_ref,
        "type": project_type,
        "plot_area": plot_area,
        "date": datetime.today().strftime("%Y-%m-%d"),
    }

    generator = ExcelGenerator()
    output_path = generator.generate(boq, report, args.output, project_info)
    print(f"[QTO] ✓ BOQ saved to: {output_path}")

    threshold = validator.overall_confidence_threshold
    if report.is_draft:
        print(
            f"[QTO] ⚠  Overall confidence {report.overall_confidence:.1f}% is below "
            f"{threshold:.0f}%. Output is marked DRAFT — manual review required."
        )
    else:
        print(
            f"[QTO] ✓ Overall confidence {report.overall_confidence:.1f}% — BOQ is FINAL."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
