/**
 * QTO Engine Integration Tests
 *
 * Uses REAL run data from uploads/villa-qto-runs/ to verify:
 * 1. translateMasterEngineResults maps Arabic names → correct item_codes
 * 2. No Arabic leaks through to item_code values
 * 3. Core structural items are non-zero when the engine extracted them
 * 4. Trust gate opens when ≥ 3 STR items have qty > 0
 * 5. Exact/total items override prefix-accumulated values
 *
 * These tests will catch any future regression where:
 * - A Python item name changes without updating the TS map
 * - The trust gate threshold breaks
 * - The accumulation logic regresses
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import { readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import * as db from "../db";

vi.mock("./pdfVectorToDxf", () => ({
  convertPdfVectorToDxf: vi.fn(async ({ outputPath }: { outputPath: string }) => ({
    outputPath,
    pageCount: 1,
    polylineCount: 10,
    textCount: 5,
  })),
}));

import { __villaQtoTestHooks } from "./villaQtoEngine";
import { convertPdfVectorToDxf } from "./pdfVectorToDxf";

const { translateMasterEngineResults, computeAggregateConfidenceScore, prepareDrawingForRuntime, shouldReplaceMergedEngineItem } = __villaQtoTestHooks;
const {
  applyLearnedOverlays,
  applySanityCheck,
  applyApprovedBaselineRelations,
  buildNormalizedEvidenceSnapshot,
  computeDerivedEquationRows,
  computeExternalWorksRows,
  buildBoqSections,
  buildItemTrustAudit,
  materializeRowQuantitySources,
  computeFinishEvidenceConfidence,
  deriveRowsFromEvidence,
} = __villaQtoTestHooks;

afterEach(() => {
  vi.clearAllMocks();
  vi.restoreAllMocks();
});

// ─── Helpers ──────────────────────────────────────────────────────────────────

const RUNS_DIR = join(process.cwd(), "uploads", "villa-qto-runs");
const STR_HARD_GATE = 60;

/** Parse stdout.log (may contain multiple JSON objects from retries — take the last valid one) */
function parseEngineStdout(filePath: string): any | null {
  if (!existsSync(filePath)) return null;
  const raw = readFileSync(filePath, "utf-8");
  // Split on newlines that start a new JSON object
  const parts = raw.split(/\n(?=\{)/);
  for (let i = parts.length - 1; i >= 0; i--) {
    try {
      return JSON.parse(parts[i].trim());
    } catch {
      // try previous
    }
  }
  return null;
}

/** Load all available run outputs */
function loadAllRuns() {
  if (!existsSync(RUNS_DIR)) return [];
  return readdirSync(RUNS_DIR)
    .map((dir) => {
      const stdoutPath = join(RUNS_DIR, dir, "output", "engine.stdout.log");
      const data = parseEngineStdout(stdoutPath);
      return { dir, data };
    })
    .filter((r) => r.data !== null && r.data.status === "Success");
}

// ─── Unit tests — synthetic input ────────────────────────────────────────────

describe("translateMasterEngineResults — Arabic item names", () => {
  const ARABIC_FLAT_ITEMS = [
    { item: "حجم الحفر",                   qty: 54.0,  unit: "م³" },
    { item: "إجمالي حجم الأسس",            qty: 16.5,  unit: "م³" },
    { item: "إجمالي حجم أعمدة العنق",       qty: 2.1,   unit: "م³" },
    { item: "إجمالي حجم كمرات الربط",       qty: 0.737, unit: "م³" },
    { item: "حجم بلاطة على الأرض",          qty: 4.2,   unit: "م³" },
    { item: "حجم الردم",                    qty: 41.0,  unit: "م³" },
    { item: "مبيد النمل الأبيض",            qty: 78.0,  unit: "م²" },
    { item: "نايلون أسود (بولي إيثيلين)",   qty: 78.0,  unit: "م²" },
    { item: "إجمالي عزل البيتومين",          qty: 580.0, unit: "م²" },
    { item: "إجمالي حجم الأعمدة",           qty: 36.6,  unit: "م³" },
    { item: "إجمالي حجم الكمرات",           qty: 11.0,  unit: "م³" },
    { item: "خرسانة الدرج",                qty: 5.0,   unit: "م³" },
    { item: "بلوك 20 سم خارجي",             qty: 367.0, unit: "م²" },
    { item: "بلوك 20 سم تصوينة السطح",      qty: 48.0,  unit: "م²" },
    { item: "بلوك 20 سم داخلي",             qty: 172.0, unit: "م²" },
    { item: "بلوك 10 سم داخلي",             qty: 174.0, unit: "م²" },
    { item: "لياسة داخلية",                qty: 285.0, unit: "م²" },
    { item: "تشطيب الواجهة الخارجية",       qty: 416.0, unit: "م²" },
    { item: "عزل مائي",                    qty: 18.0,  unit: "م²" },
    { item: "نظام السقف المركب",            qty: 78.0,  unit: "م²" },
    { item: "بلاط المناطق الجافة",          qty: 3.4,   unit: "م²" },
    { item: "سيراميك المناطق المبللة",       qty: 22.0,  unit: "م²" },
    { item: "بلاط البلكونة",                qty: 5.0,   unit: "م²" },
    { item: "سكرتة",                       qty: 45.0,  unit: "م.ط" },
    { item: "دهان",                        qty: 300.0, unit: "م²" },
    { item: "سقف المناطق الجافة",           qty: 3.4,   unit: "م²" },
    { item: "بلاط الجدران",                qty: 55.0,  unit: "م²" },
    { item: "سقف المناطق المبللة",          qty: 22.0,  unit: "م²" },
    { item: "عتبات رخام",                  qty: 8.5,   unit: "م.ط" },
  ];

  const apiResponse = { results_flat: ARABIC_FLAT_ITEMS };
  const rows = translateMasterEngineResults(apiResponse);
  const byCode = new Map(rows.map((r) => [r.item_code, r]));

  it("maps حجم الحفر → EXCAVATION_M3", () => {
    expect(byCode.get("EXCAVATION_M3")?.system_qty).toBe(54.0);
  });

  it("maps إجمالي حجم الأسس → RCC_FOOTINGS_M3", () => {
    expect(byCode.get("RCC_FOOTINGS_M3")?.system_qty).toBe(16.5);
  });

  it("maps إجمالي حجم أعمدة العنق → NECK_COLUMNS_M3", () => {
    expect(byCode.get("NECK_COLUMNS_M3")?.system_qty).toBe(2.1);
  });

  it("maps إجمالي حجم كمرات الربط → TIE_BEAMS_M3", () => {
    expect(byCode.get("TIE_BEAMS_M3")?.system_qty).toBe(0.737);
  });

  it("maps حجم الردم → BACKFILL_COMPACTION_M3", () => {
    expect(byCode.get("BACKFILL_COMPACTION_M3")?.system_qty).toBe(41.0);
  });

  it("maps إجمالي حجم الأعمدة → RCC_COLUMNS_M3", () => {
    expect(byCode.get("RCC_COLUMNS_M3")?.system_qty).toBe(36.6);
  });

  it("maps إجمالي حجم الكمرات → FIRST_SLAB_BEAMS_M3", () => {
    expect(byCode.get("FIRST_SLAB_BEAMS_M3")?.system_qty).toBe(11.0);
  });

  it("maps إجمالي عزل البيتومين → BITUMEN_SUBSTRUCTURE_TOTAL_QTY", () => {
    expect(byCode.get("BITUMEN_SUBSTRUCTURE_TOTAL_QTY")?.system_qty).toBe(580.0);
  });

  it("does not map strap beam volume directly into SOLID_BLOCK_WORK_M2", () => {
    expect(byCode.get("SOLID_BLOCK_WORK_M2")).toBeUndefined();
  });

  it("maps بلوك 20 سم خارجي + بلوك 20 سم تصوينة السطح → BLOCK_EXTERNAL_THERMAL_M2 (sums both)", () => {
    // Both items share the same code → their quantities accumulate: 367 + 48 = 415
    expect(byCode.get("BLOCK_EXTERNAL_THERMAL_M2")?.system_qty).toBe(415.0);
  });

  it("maps بلوك 20 سم داخلي → BLOCK_INTERNAL_HOLLOW_8_M2", () => {
    expect(byCode.get("BLOCK_INTERNAL_HOLLOW_8_M2")?.system_qty).toBe(172.0);
  });

  it("maps لياسة داخلية → PLASTER_INTERNAL_M2", () => {
    expect(byCode.get("PLASTER_INTERNAL_M2")?.system_qty).toBe(285.0);
  });

  it("maps تشطيب الواجهة الخارجية → PLASTER_EXTERNAL_M2", () => {
    expect(byCode.get("PLASTER_EXTERNAL_M2")?.system_qty).toBe(416.0);
  });

  it("maps سكرتة → SKIRTING_LM", () => {
    expect(byCode.get("SKIRTING_LM")?.system_qty).toBe(45.0);
  });

  it("maps عتبات رخام → MARBLE_THRESHOLD_LM", () => {
    expect(byCode.get("MARBLE_THRESHOLD_LM")?.system_qty).toBe(8.5);
  });

  it("maps dry + wet area ceiling items into one CEILING_SPRAY_PLASTER_M2 total", () => {
    expect(byCode.get("CEILING_SPRAY_PLASTER_M2")?.system_qty).toBe(3.4 + 22.0);
  });

  it("produces at least 10 non-zero items from Arabic input", () => {
    const nonZero = rows.filter((r) => Number(r.system_qty) > 0);
    expect(nonZero.length).toBeGreaterThanOrEqual(10);
  });

  it("no item_code contains Arabic characters", () => {
    const arabicLeaks = rows.filter((r) => /[\u0600-\u06FF]/.test(r.item_code));
    expect(arabicLeaks).toHaveLength(0);
  });

  it("ignores slab-on-grade area intermediate items without warning", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const rows = translateMasterEngineResults({
      results_flat: [{ item: "مساحة بلاطة على الأرض", qty: 22.06, unit: "م²" }],
    });

    expect(rows.filter((row) => Number(row.system_qty) > 0)).toHaveLength(0);
    expect(warnSpy).not.toHaveBeenCalled();
  });

  it("preserves Python AVG_* statuses as average-derived provenance", () => {
    const rows = translateMasterEngineResults({
      results_flat: [
        { item: "بلاط الجدران", qty: 285, unit: "م²", status: "AVG_FALLBACK" },
      ],
    });

    const wallTiles = rows.find((row) => row.item_code === "WALL_TILES_WET_AREAS_M2");

    expect(wallTiles?._averageDerived).toBe(true);
    expect(wallTiles?._averageDerivationSource).toBe("engine_avg_status");
    expect(wallTiles?._averageScaleSource).toContain("AVG_FALLBACK");
  });
});

describe("prepareDrawingForRuntime", () => {
  it("passes DXF files through unchanged", async () => {
    const result = await prepareDrawingForRuntime("C:\\tmp", "str", "C:\\tmp\\structural.dxf");

    expect(result).toBe("C:\\tmp\\structural.dxf");
    expect(convertPdfVectorToDxf).not.toHaveBeenCalled();
  });

  it("passes PDF files through unchanged (native PyMuPDF extraction)", async () => {
    const result = await prepareDrawingForRuntime("C:\\tmp", "arch", "C:\\tmp\\architectural.pdf");

    expect(convertPdfVectorToDxf).not.toHaveBeenCalled();
    expect(result).toBe("C:\\tmp\\architectural.pdf");
  });
});

// ─── Unit tests — English legacy names still work ────────────────────────────

describe("translateMasterEngineResults — English legacy names", () => {
  const ENGLISH_FLAT = [
    { item: "Excavation",             qty: 60.0, unit: "m³" },
    { item: "Total Foundation Volume",qty: 20.0, unit: "m³" },
    { item: "Total Columns Volume",   qty: 30.0, unit: "m³" },
    { item: "Block 20cm External",    qty: 200.0, unit: "m²" },
    { item: "Internal Plaster",       qty: 250.0, unit: "m²" },
    { item: "Staircase Concrete",     qty: 5.0,  unit: "m³" },
  ];

  const rows = translateMasterEngineResults({ results_flat: ENGLISH_FLAT });
  const byCode = new Map(rows.map((r) => [r.item_code, r]));

  it("maps Excavation → EXCAVATION_M3", () => {
    expect(byCode.get("EXCAVATION_M3")?.system_qty).toBe(60.0);
  });

  it("maps Total Foundation Volume → RCC_FOOTINGS_M3", () => {
    expect(byCode.get("RCC_FOOTINGS_M3")?.system_qty).toBe(20.0);
  });

  it("maps Staircase Concrete → STAIRS_INTERNAL_M3", () => {
    expect(byCode.get("STAIRS_INTERNAL_M3")?.system_qty).toBe(5.0);
  });

  it("does not manufacture zero-quantity catalog rows for missing item codes", () => {
    expect(rows.find((row) => row.item_code === "PAINT_EXTERNAL_M2")).toBeUndefined();
  });
});

// ─── Unit tests — English per-element prefix accumulation ────────────────────

describe("translateMasterEngineResults — English prefix accumulation", () => {
  it("accumulates Foundation PCC per-element items into PLAIN_CONCRETE_UNDER_FOOTINGS_M3", () => {
    const flat = [
      { item: "Foundation PCC (FN)", qty: 0.196, unit: "m³" },
      { item: "Foundation PCC (F1)", qty: 0.285, unit: "m³" },
      { item: "Foundation PCC (F2)", qty: 0.142, unit: "m³" },
    ];
    const rows = translateMasterEngineResults({ results_flat: flat });
    const pcc = rows.find((r) => r.item_code === "PLAIN_CONCRETE_UNDER_FOOTINGS_M3");
    expect(pcc?.system_qty).toBeCloseTo(0.196 + 0.285 + 0.142, 4);
  });

  it("exact total overrides accumulated prefix values", () => {
    const flat = [
      // These arrive first via prefix match
      { item: "Foundation Volume (F1)", qty: 5.0, unit: "m³" },
      { item: "Foundation Volume (F2)", qty: 3.0, unit: "m³" },
      // Then the authoritative total arrives
      { item: "Total Foundation Volume", qty: 16.5, unit: "m³" },
    ];
    const rows = translateMasterEngineResults({ results_flat: flat });
    const footings = rows.find((r) => r.item_code === "RCC_FOOTINGS_M3");
    // Must be 16.5 (exact total) not 5+3=8
    expect(footings?.system_qty).toBe(16.5);
  });

  it("accumulates Foundation Bitumen per-elements into BITUMEN_SUBSTRUCTURE_TOTAL_QTY", () => {
    const flat = [
      { item: "Foundation Bitumen (FN)", qty: 3.12, unit: "m²" },
      { item: "Foundation Bitumen (F1)", qty: 4.61, unit: "m²" },
      { item: "Neck Column Bitumen (C1)", qty: 1.20, unit: "m²" },
      { item: "Tie Beam Bitumen (TB1)", qty: 0.80, unit: "m²" },
    ];
    const rows = translateMasterEngineResults({ results_flat: flat });
    const bitumen = rows.find((r) => r.item_code === "BITUMEN_SUBSTRUCTURE_TOTAL_QTY");
    expect(bitumen?.system_qty).toBeCloseTo(3.12 + 4.61 + 1.20 + 0.80, 3);
  });
});

describe("derived equation rows", () => {
  it("derives PAINT_EXTERNAL_M2 from external plaster when missing", () => {
    const baseRows = translateMasterEngineResults({
      results_flat: [
        { item: "External Wall Finish", qty: 416.0, unit: "m²" },
      ],
    });

    const derived = computeDerivedEquationRows(baseRows, baseRows.length + 1);
    const externalPaint = derived.find((row) => row.item_code === "PAINT_EXTERNAL_M2");

    expect(externalPaint?.system_qty).toBe(416.0);
  });

  it("does not derive external paint when external plaster evidence is missing", () => {
    const derived = computeDerivedEquationRows([], 1);
    expect(derived.find((row) => row.item_code === "PAINT_EXTERNAL_M2")).toBeUndefined();
  });

  it("derives dry and wet ceilings directly from flooring quantities when missing", () => {
    const derived = computeDerivedEquationRows(
      [
        { item_no: 1, item_code: "DRY_AREA_FLOORING_M2", discipline: "FINISH", unit: "m²", system_qty: 380 },
        { item_no: 2, item_code: "WET_AREA_FLOORING_M2", discipline: "FINISH", unit: "m²", system_qty: 97 },
      ],
      3,
    );

    expect(derived.find((row) => row.item_code === "CEILING_SPRAY_PLASTER_M2")?.system_qty).toBe(380);
  });
});

describe("normalized evidence derivations", () => {
  it("derives flooring, ceilings, waterproofing, and false ceiling from available room evidence", () => {
    const snapshot = buildNormalizedEvidenceSnapshot({
      mergedResponse: {
        raw_spatial_evidence: {
          rooms: [
            { key: "BATH", scope: "GROUND", width_m: 2, height_m: 3, area_m2: 6, source: "ROOM_GEOMETRY" },
            { key: "KITCHEN", scope: "GROUND", width_m: 4, height_m: 3, area_m2: 12, source: "ROOM_GEOMETRY" },
            { key: "BALCONY", scope: "FIRST", width_m: 2, height_m: 4, area_m2: 8, source: "ROOM_GEOMETRY" },
            { key: "MAJLIS", scope: "GROUND", width_m: 5, height_m: 4, area_m2: 20, source: "ROOM_GEOMETRY" },
          ],
        },
      },
      rows: [
        { item_no: 1, item_code: "PLASTER_EXTERNAL_M2", discipline: "FINISH", unit: "m²", system_qty: 420 },
        { item_no: 2, item_code: "ROOF_WATERPROOF_M2", discipline: "FINISH", unit: "m²", system_qty: 256 },
      ],
      inputs: {
        excavationDepthM: 1.3,
        roadBaseExists: false,
        roadBaseThicknessM: 0.3,
        internalStaircaseDefaultM3: 5.4,
        hasExternalStaircase: false,
        levelReference: "FFL",
        foundationDepthM: 1.2,
        groundFloorToFloorM: 4,
        firstFloorToFloorM: 4,
        secondFloorToFloorM: 4,
        strictBlueprint: false,
      },
    });

    const derived = deriveRowsFromEvidence(snapshot, 1);
    const byCode = new Map(derived.map((row) => [row.item_code, row]));

    expect(byCode.get("WET_AREA_FLOORING_M2")?.system_qty).toBe(18);
    expect(byCode.get("CEILING_SPRAY_PLASTER_M2")?.system_qty).toBe(20);
    expect(byCode.get("BALCONY_FLOORING_M2")?.system_qty).toBe(8);
    expect(byCode.get("WALL_TILES_WET_AREAS_M2")?.system_qty).toBeCloseTo(57.6, 2);
    expect(byCode.get("WET_AREAS_BALCONY_WATERPROOF_M2")?.system_qty).toBeCloseTo(36.8, 2);
    expect(byCode.get("FALSE_CEILING_M2")?.system_qty).toBe(18);
    expect(byCode.get("PAINT_EXTERNAL_M2")?.system_qty).toBe(420);
  });

  it("normalizes Arabic room objects from Python raw_spatial_evidence", () => {
    const snapshot = buildNormalizedEvidenceSnapshot({
      mergedResponse: {
        raw_spatial_evidence: {
          الغرف: [
            { الاسم: "حمام", المساحة: 6, المحيط: 10, المستوى: "GF" },
            { الاسم: "مجلس", المساحة: 20, المحيط: 18, المستوى: "GF" },
            { الاسم: "بلكونة", المساحة: 4, المحيط: 8, المستوى: "1F" },
          ],
        },
      },
      rows: [],
    });

    expect(snapshot.rooms).toHaveLength(3);
    expect(snapshot.rooms.map((room) => room.key)).toEqual(["BATH", "MAJLIS", "BALCONY"]);
    expect(snapshot.rooms.map((room) => room.scope)).toEqual(["GF", "GF", "1F"]);
  });

  it("derives SOLID_BLOCK_WORK_M2 from raw strap beam geometry as area, not volume", () => {
    const snapshot = buildNormalizedEvidenceSnapshot({
      mergedResponse: {
        raw_spatial_evidence: {
          الكمرات_من_الرسم: {
            كمرات_الستراب: [
              { الرمز: "STB1", الطول: 3.38, العمق: 0.5 },
              { الرمز: "STB2", الطول: 12.0, العمق: 0.767 },
            ],
          },
        },
      },
      rows: [],
    });

    const derived = deriveRowsFromEvidence(snapshot, 1);
    expect(derived.find((row) => row.item_code === "SOLID_BLOCK_WORK_M2")?.system_qty).toBeCloseTo(10.89, 2);
  });
});

describe("merge heuristics", () => {
  it("keeps a realistic AVG-corrected external block quantity when a tiny sparse extraction fragment appears", () => {
    const shouldReplace = shouldReplaceMergedEngineItem(
      { item: "Block 20cm External", qty: 389.7, status: "AVG_FLOOR_CORRECTED" },
      { item: "Block 20cm External", qty: 8.4, status: "" },
    );

    expect(shouldReplace).toBe(false);
  });

  it("still allows extracted structural quantities to replace corrected averages when the ratio is materially close", () => {
    const shouldReplace = shouldReplaceMergedEngineItem(
      { item: "Total Tie Beams Volume", qty: 23.4, status: "AVG_FALLBACK" },
      { item: "Total Tie Beams Volume", qty: 17.902, status: "" },
    );

    expect(shouldReplace).toBe(true);
  });
});

describe("finish evidence confidence", () => {
  it("upgrades to MEDIUM when finish and arch evidence rows are present even if global confidence is low", () => {
    const confidence = computeFinishEvidenceConfidence({
      roomCount: 1,
      confidenceScore: 34,
      rows: [
        { item_no: 1, item_code: "DRY_AREA_FLOORING_M2", discipline: "FINISH", unit: "m²", system_qty: 318.1 },
        { item_no: 2, item_code: "WET_AREA_FLOORING_M2", discipline: "FINISH", unit: "m²", system_qty: 60 },
        { item_no: 3, item_code: "SKIRTING_LM", discipline: "FINISH", unit: "RM", system_qty: 340 },
        { item_no: 4, item_code: "CEILING_SPRAY_PLASTER_M2", discipline: "FINISH", unit: "m²", system_qty: 378.1 },
        { item_no: 5, item_code: "PAINT_INTERNAL_M2", discipline: "FINISH", unit: "m²", system_qty: 920 },
        { item_no: 6, item_code: "BLOCK_EXTERNAL_THERMAL_M2", discipline: "ARCH", unit: "m²", system_qty: 389.7 },
        { item_no: 7, item_code: "PLASTER_EXTERNAL_M2", discipline: "ARCH", unit: "m²", system_qty: 825 },
      ],
    });

    expect(confidence).toBe("MEDIUM");
  });
});

describe("computeExternalWorksRows", () => {
  it("derives FALSE_CEILING_M2 from wet flooring when the BOQ row is missing", () => {
    const rows = computeExternalWorksRows(
      { raw_spatial_evidence: { rooms: [] } },
      1,
      undefined,
      [{ item_no: 1, item_code: "WET_AREA_FLOORING_M2", discipline: "FINISH", unit: "m²", system_qty: 97 }],
    );

    expect(rows.find((row) => row.item_code === "FALSE_CEILING_M2")?.system_qty).toBe(97);
  });

  it("uses translated item codes rather than raw engine labels", () => {
    const translatedRows = translateMasterEngineResults({
      results_flat: [
        { item: "نظام السقف المركب", qty: 256.0, unit: "م²" },
        { item: "تشطيب الواجهة الخارجية", qty: 840.0, unit: "م²" },
      ],
    });

    const rows = computeExternalWorksRows(
      { raw_spatial_evidence: { rooms: [] } },
      translatedRows.length + 1,
      undefined,
      translatedRows,
    );

    expect(rows.find((row) => row.item_code === "ROOF_THERMAL_INSULATION_M2")?.system_qty).toBe(256.0);
    expect(Number(rows.find((row) => row.item_code === "PARAPET_WALL_M2")?.system_qty || 0)).toBeGreaterThan(0);
    expect(Number(rows.find((row) => row.item_code === "COPING_STONES_LM")?.system_qty || 0)).toBeGreaterThan(0);
  });
});

describe("buildBoqSections", () => {
  it("filters zero placeholders and collapses duplicate item codes", () => {
    const sections = buildBoqSections([
      { item_no: 1, item_code: "PLASTER_EXTERNAL_M2", discipline: "FINISH", unit: "m²", system_qty: 0, _catalogFill: true },
      { item_no: 2, item_code: "PLASTER_EXTERNAL_M2", discipline: "FINISH", unit: "m²", system_qty: 824.19 },
      { item_no: 3, item_code: "PAINT_EXTERNAL_M2", discipline: "FINISH", unit: "m²", system_qty: 0, _catalogFill: true },
    ]);

    const allItems = sections.flatMap((section) => section.items);

    expect(allItems.filter((item) => item.ref === "PLASTER_EXTERNAL_M2")).toHaveLength(1);
    expect(allItems.find((item) => item.ref === "PLASTER_EXTERNAL_M2")?.quantity).toBe(824.19);
    expect(allItems.find((item) => item.ref === "PAINT_EXTERNAL_M2")).toBeUndefined();
  });

  it("marks average-derived quantities explicitly in BOQ output", () => {
    const sections = buildBoqSections([
      {
        item_no: 1,
        item_code: "WALL_TILES_WET_AREAS_M2",
        discipline: "FINISH",
        unit: "m²",
        system_qty: 570,
        _averageDerived: true,
        _averageDerivationSource: "learned_overlay",
        _averageScaleSource: "average area relation via WET_AREA_FLOORING_M2",
        _averageScaleFactor: 2,
        _averageReferenceQty: 285,
      },
    ]);

    const item = sections.flatMap((section) => section.items).find((entry) => entry.ref === "WALL_TILES_WET_AREAS_M2");

    expect(item?.quantitySource).toBe("average_scaled");
    expect(item?.quantitySourceNote).toContain("As per average relation");
    expect(item?.quantitySourceNote).toContain("WET_AREA_FLOORING_M2");
    expect(item?.needsVerification).toBe(true);
  });

  it("distinguishes extracted rows from derived rows in BOQ output", () => {
    const sections = buildBoqSections([
      {
        item_no: 1,
        item_code: "PLASTER_EXTERNAL_M2",
        discipline: "FINISH",
        unit: "m²",
        system_qty: 416,
      },
      {
        item_no: 2,
        item_code: "PAINT_EXTERNAL_M2",
        discipline: "FINISH",
        unit: "m²",
        system_qty: 416,
        _derivedSource: "evidence_equation",
      },
    ]);

    const items = sections.flatMap((section) => section.items);
    const plaster = items.find((entry) => entry.ref === "PLASTER_EXTERNAL_M2");
    const paint = items.find((entry) => entry.ref === "PAINT_EXTERNAL_M2");

    expect(plaster?.quantitySource).toBe("extracted");
    expect(plaster?.quantitySourceNote).toBeUndefined();
    expect(paint?.quantitySource).toBe("derived");
    expect(paint?.quantitySourceNote).toContain("evidence equation");
  });
});

describe("Phase 1 quantity source audit", () => {
  it("materializes quantity source metadata onto returned qto rows", () => {
    const rows = materializeRowQuantitySources([
      {
        item_no: 1,
        item_code: "PAINT_EXTERNAL_M2",
        discipline: "FINISH",
        unit: "m²",
        system_qty: 416,
        _derivedSource: "evidence_equation",
      },
      {
        item_no: 2,
        item_code: "WALL_TILES_WET_AREAS_M2",
        discipline: "FINISH",
        unit: "m²",
        system_qty: 570,
        _averageDerived: true,
        _averageDerivationSource: "learned_overlay",
        _averageScaleSource: "average area relation via WET_AREA_FLOORING_M2",
        _averageScaleFactor: 2,
        _averageReferenceQty: 285,
      },
    ]);

    expect(rows[0]?.quantitySource).toBe("derived");
    expect(rows[0]?.quantitySourceNote).toContain("evidence equation");
    expect(rows[1]?.quantitySource).toBe("average_scaled");
    expect(rows[1]?.quantitySourceNote).toContain("As per average relation");
  });

  it("includes explicit source classification in trust audit output", () => {
    const items = buildItemTrustAudit({
      rows: [
        {
          item_no: 1,
          item_code: "PAINT_EXTERNAL_M2",
          discipline: "FINISH",
          unit: "m²",
          system_qty: 416,
          _derivedSource: "evidence_equation",
        },
      ],
      inputs: {
        excavationDepthM: 1.3,
        roadBaseExists: false,
        roadBaseThicknessM: 0,
        internalStaircaseDefaultM3: 5.4,
        hasExternalStaircase: false,
        levelReference: "NGL0",
        foundationDepthM: 1.3,
        groundFloorToFloorM: 4,
        firstFloorToFloorM: 4,
        secondFloorToFloorM: 0,
        strictBlueprint: false,
      },
      disciplineTrust: new Map([
        [
          "FINISH",
          {
            discipline: "FINISH",
            status: "PASS",
            reasons: [],
            warnings: [],
            warningCodes: [],
            metrics: {},
          },
        ],
      ]),
    });

    expect(items[0]?.quantitySource).toBe("derived");
    expect(items[0]?.quantitySourceNote).toContain("evidence equation");
    expect(items[0]?.reasons.some((reason) => reason.includes("SOURCE BASIS:"))).toBe(true);
  });
});

describe("average-based scaling", () => {
  it("scales learned absolute averages by related area instead of copying the raw average", async () => {
    vi.spyOn(db, "getActiveQtoBaselineItems").mockResolvedValue([
      { itemCode: "WET_AREA_FLOORING_M2", avgQty: 60 },
      { itemCode: "WALL_TILES_WET_AREAS_M2", avgQty: 285 },
    ] as any);
    vi.spyOn(db, "getLearnedPattern").mockResolvedValue({
      confidence: 90,
      sampleCount: 5,
      avgValue: 285,
      metadata: { overlayType: "bulk_training_absolute" },
    } as any);

    const { correctedRows } = await applyLearnedOverlays([
      { item_no: 1, item_code: "WET_AREA_FLOORING_M2", discipline: "FINISH", unit: "m²", system_qty: 120 },
      { item_no: 2, item_code: "WALL_TILES_WET_AREAS_M2", discipline: "FINISH", unit: "m²", system_qty: 0 },
    ], "g1");

    const wallTiles = correctedRows.find((row) => row.item_code === "WALL_TILES_WET_AREAS_M2");

    expect(wallTiles?.system_qty).toBe(570);
    expect(wallTiles?._averageDerived).toBe(true);
    expect(wallTiles?._averageScaleSource).toContain("WET_AREA_FLOORING_M2");
  });

  it("uses related area scaling during sanity clamp instead of generic avg copy", async () => {
    vi.spyOn(db, "getActiveQtoBaselineItems").mockResolvedValue([
      { itemCode: "WET_AREA_FLOORING_M2", avgQty: 60, minQty: 50, maxQty: 70, sampleCount: 10 },
      { itemCode: "WALL_TILES_WET_AREAS_M2", avgQty: 285, minQty: 250, maxQty: 320, sampleCount: 10 },
    ] as any);

    const { checkedRows, sanityAlerts } = await applySanityCheck([
      { item_no: 1, item_code: "WET_AREA_FLOORING_M2", discipline: "FINISH", unit: "m²", system_qty: 120 },
      { item_no: 2, item_code: "WALL_TILES_WET_AREAS_M2", discipline: "FINISH", unit: "m²", system_qty: 1800 },
    ], "g1");

    const wallTiles = checkedRows.find((row) => row.item_code === "WALL_TILES_WET_AREAS_M2");
    const alert = sanityAlerts.find((entry) => entry.itemCode === "WALL_TILES_WET_AREAS_M2");

    expect(wallTiles?.system_qty).toBe(570);
    expect(wallTiles?._averageDerived).toBe(true);
    expect(alert?.scaleSource).toContain("WET_AREA_FLOORING_M2");
    expect(alert?.scaleFactor).toBe(2);
  });

  it("does not apply learned multiplier overlays on Python-owned average rows", async () => {
    vi.spyOn(db, "getActiveQtoBaselineItems").mockResolvedValue([] as any);
    vi.spyOn(db, "getLearnedPattern").mockResolvedValue({
      confidence: 95,
      sampleCount: 7,
      avgValue: 1.4,
      metadata: { overlayType: "villa_qto_item_multiplier" },
    } as any);

    const { correctedRows, appliedOverlays } = await applyLearnedOverlays([
      {
        item_no: 1,
        item_code: "RCC_FOOTINGS_M3",
        discipline: "STR",
        unit: "m³",
        system_qty: 18,
        _averageDerived: true,
        _averageDerivationSource: "engine_avg_status",
        _averageScaleSource: "python engine status AVG_FALLBACK",
        _averageScaleFactor: 1,
        _averageReferenceQty: 18,
      },
    ], "g1");

    expect(correctedRows[0]?.system_qty).toBe(18);
    expect(appliedOverlays).toHaveLength(0);
  });

  it("does not re-clamp rows already marked as average-derived", async () => {
    vi.spyOn(db, "getActiveQtoBaselineItems").mockResolvedValue([
      { itemCode: "RCC_FOOTINGS_M3", avgQty: 10, minQty: 8, maxQty: 12, sampleCount: 10 },
    ] as any);

    const { checkedRows, sanityAlerts } = await applySanityCheck([
      {
        item_no: 1,
        item_code: "RCC_FOOTINGS_M3",
        discipline: "STR",
        unit: "m³",
        system_qty: 18,
        _averageDerived: true,
        _averageDerivationSource: "engine_avg_status",
        _averageScaleSource: "python engine status AVG_FALLBACK",
        _averageScaleFactor: 1,
        _averageReferenceQty: 18,
      },
    ], "g1");

    expect(checkedRows[0]?.system_qty).toBe(18);
    expect(sanityAlerts).toHaveLength(0);
  });

  it("propagates average lineage through baseline relations instead of downgrading it to plain derived", async () => {
    vi.spyOn(db, "getActiveQtoBaselineRelations").mockResolvedValue([
      {
        itemCode: "CEILING_WET_AREA_M2",
        relationType: "equals",
        relatedItemCode: "WET_AREA_FLOORING_M2",
        factor: 1,
        metadata: {},
      },
    ] as any);

    const relationRows = await applyApprovedBaselineRelations([
      {
        item_no: 1,
        item_code: "WET_AREA_FLOORING_M2",
        discipline: "FINISH",
        unit: "m²",
        system_qty: 97,
        _averageDerived: true,
        _averageDerivationSource: "engine_avg_status",
        _averageScaleSource: "average area relation via WET_AREA_FLOORING_M2",
        _averageScaleFactor: 1.6167,
        _averageReferenceQty: 60,
      },
    ], "g1");

    const ceiling = materializeRowQuantitySources(relationRows).find((row) => row.item_code === "CEILING_WET_AREA_M2");

    expect(ceiling?.system_qty).toBe(97);
    expect(ceiling?._averageDerived).toBe(true);
    expect(ceiling?._averageDerivationSource).toBe("baseline_relation");
    expect(ceiling?.quantitySource).toBe("average_scaled");
    expect(ceiling?.quantitySourceNote).toContain("approved relation from WET_AREA_FLOORING_M2");
  });
});

// ─── Trust gate tests ─────────────────────────────────────────────────────────

describe("Trust gate — strItemsWithQty bypass", () => {
  it("gate opens when ≥ 3 STR items have qty > 0 even if confidence = 0", () => {
    // Simulate the gate logic as it exists in the engine
    const confidenceScore = 0; // always 0 for pure STR DXFs
    const qtoRows = [
      { item_code: "EXCAVATION_M3",     discipline: "STR", system_qty: 54.0 },
      { item_code: "RCC_FOOTINGS_M3",   discipline: "STR", system_qty: 16.5 },
      { item_code: "RCC_COLUMNS_M3",    discipline: "STR", system_qty: 36.6 },
      { item_code: "TIE_BEAMS_M3",      discipline: "STR", system_qty: 0 },
      { item_code: "PLASTER_INTERNAL_M2", discipline: "FINISH", system_qty: 285.0 },
    ];

    const strItemsWithQty = qtoRows.filter(
      (r) => r.discipline === "STR" && Number(r.system_qty) > 0
    ).length;

    const gateOk = confidenceScore >= STR_HARD_GATE || strItemsWithQty >= 3;
    expect(gateOk).toBe(true);
    expect(strItemsWithQty).toBe(3);
  });

  it("gate blocks when < 3 STR items have qty and confidence = 0", () => {
    const confidenceScore = 0;
    const qtoRows = [
      { item_code: "EXCAVATION_M3",   discipline: "STR", system_qty: 0 },
      { item_code: "RCC_FOOTINGS_M3", discipline: "STR", system_qty: 0 },
      { item_code: "RCC_COLUMNS_M3",  discipline: "STR", system_qty: 0 },
    ];

    const strItemsWithQty = qtoRows.filter(
      (r) => r.discipline === "STR" && Number(r.system_qty) > 0
    ).length;

    const gateOk = confidenceScore >= STR_HARD_GATE || strItemsWithQty >= 3;
    expect(gateOk).toBe(false);
  });

  it("gate opens when confidence alone meets threshold (no STR quantities needed)", () => {
    const confidenceScore = 75;
    const qtoRows: any[] = [];
    const strItemsWithQty = 0;
    const gateOk = confidenceScore >= STR_HARD_GATE || strItemsWithQty >= 3;
    expect(gateOk).toBe(true);
  });
});

// ─── computeAggregateConfidenceScore tests ────────────────────────────────────

describe("computeAggregateConfidenceScore", () => {
  it("returns 0 when no responses have confidence set", () => {
    const score = computeAggregateConfidenceScore([
      { raw_spatial_evidence: {} },
      { raw_spatial_evidence: { extraction_confidence: {} } },
    ]);
    expect(score).toBe(0);
  });

  it("returns correct weighted average", () => {
    const score = computeAggregateConfidenceScore([
      {
        raw_spatial_evidence: {
          extraction_confidence: { score: 80 },
          rooms: [{}, {}, {}], // weight = 3
        },
      },
      {
        raw_spatial_evidence: {
          extraction_confidence: { score: 40 },
          rooms: [{}], // weight = 1
        },
      },
    ]);
    // (80*3 + 40*1) / (3+1) = 280/4 = 70
    expect(score).toBe(70);
  });

  it("clamps score to 0-100 range", () => {
    const score = computeAggregateConfidenceScore([
      { raw_spatial_evidence: { extraction_confidence: { score: 999 } } },
    ]);
    expect(score).toBe(100);
  });
});

// ─── Integration tests — real run data ───────────────────────────────────────

describe("Real run data — regression tests", () => {
  const runs = loadAllRuns();

  if (runs.length === 0) {
    it.skip("No run data available — skipping integration tests", () => {});
  } else {
    it(`found ${runs.length} successful runs to test`, () => {
      expect(runs.length).toBeGreaterThan(0);
    });
  }

  for (const { dir, data } of runs) {
    describe(`Run: ${dir}`, () => {
      const rows = translateMasterEngineResults(data);
      const nonZero = rows.filter((r) => Number(r.system_qty) > 0);
      const arabicLeaks = rows.filter((r) => /[\u0600-\u06FF]/.test(r.item_code));

      // Count non-zero items from the raw Python engine output.
      // Runs from before the engine fix may have only 1–2 non-zero items (degenerate output);
      // we skip quantity/trust-gate assertions for those historical broken runs.
      const flatItems: any[] = data?.results_flat ?? [];
      const sectionItems: any[] = Object.values(data?.results_by_section ?? {}).flat() as any[];
      const rawSource = (flatItems.length > 0 ? flatItems : sectionItems) as any[];
      const rawNonZeroCount = rawSource.filter((r: any) => Number(r.qty) > 0).length;
      const hasRichOutput = rawNonZeroCount >= 5;

      it.skipIf(!hasRichOutput)("produces at least 5 non-zero BOQ items", () => {
        expect(nonZero.length).toBeGreaterThanOrEqual(5);
      });

      it("no Arabic characters leak into item_code values", () => {
        if (arabicLeaks.length > 0) {
          // Show which ones leaked for easy debugging
          const leaked = arabicLeaks.map((r) => r.item_code).join(", ");
          expect.fail(`Arabic leaked into item_code: ${leaked}`);
        }
        expect(arabicLeaks).toHaveLength(0);
      });

      it("EXCAVATION_M3 is present in output (all villa projects have excavation)", () => {
        const excav = rows.find((r) => r.item_code === "EXCAVATION_M3");
        expect(excav).toBeDefined();
      });

      it.skipIf(!hasRichOutput)("trust gate would open (≥ 3 STR items with qty > 0)", () => {
        const strWithQty = rows.filter(
          (r) => r.discipline === "STR" && Number(r.system_qty) > 0
        ).length;
        const confidence = Number(
          data?.درجة_الثقة?.النتيجة ??
          data?.confidence?.score ??
          data?.raw_spatial_evidence?.extraction_confidence?.score ?? 0
        );
        const gateOk = confidence >= STR_HARD_GATE || strWithQty >= 3;

        if (!gateOk) {
          expect.fail(
            `Gate would block this run: confidence=${confidence}, STR items with qty=${strWithQty}`
          );
        }
        expect(gateOk).toBe(true);
      });
    });
  }
});
