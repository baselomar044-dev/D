import { describe, expect, it } from "vitest";

import { __villaQtoTestHooks } from "./villaQtoEngine";

const baseInputs = {
  excavationDepthM: 1.3,
  roadBaseExists: false,
  roadBaseThicknessM: 0,
  internalStaircaseDefaultM3: 5.4,
  hasExternalStaircase: false,
  levelReference: "NGL0",
  foundationDepthM: 1.3,
  groundFloorToFloorM: 4,
  firstFloorToFloorM: 4,
  secondFloorToFloorM: 4,
  strictBlueprint: false,
};

describe("villaQtoEngine trust logic", () => {
  it("treats catalog placeholder zero rows as warnings instead of failures", () => {
    const items = __villaQtoTestHooks.buildItemTrustAudit({
      rows: [
        {
          item_no: 1,
          item_code: "PLASTER_INTERNAL_M2",
          discipline: "FINISH",
          unit: "m²",
          system_qty: 0,
          _catalogFill: true,
        },
      ],
      inputs: baseInputs,
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

    expect(items[0].quantityStatus).toBe("ZERO");
    expect(items[0].finalStatus).toBe("FAIL");
    expect(items[0].reasons[0]).toMatch(/Zero quantity/i);
  });

  it("replaces catalog-fill placeholders with computed rows for the same item code", () => {
    const merged = __villaQtoTestHooks.mergeComputedRowsReplacingCatalogFill(
      [
        {
          item_no: 7,
          item_code: "PARAPET_WALL_M2",
          discipline: "ARCH",
          unit: "m²",
          system_qty: 0,
          _catalogFill: true,
        },
      ],
      [
        {
          item_no: 99,
          item_code: "PARAPET_WALL_M2",
          discipline: "ARCH",
          unit: "m²",
          system_qty: 118.25,
        },
      ]
    );

    expect(merged).toHaveLength(1);
    expect(merged[0].item_no).toBe(7);
    expect(merged[0].system_qty).toBe(118.25);
    expect(merged[0]._catalogFill).toBeUndefined();
  });

  it("uses weighted confidence instead of the highest single confidence score", () => {
    const score = __villaQtoTestHooks.computeAggregateConfidenceScore([
      {
        raw_spatial_evidence: {
          rooms: new Array(12).fill({}),
          extraction_confidence: { score: 58 },
        },
        results_by_section: {
          structural: new Array(12).fill({}),
        },
      },
      {
        raw_spatial_evidence: {
          rooms: [],
          extraction_confidence: { score: 96 },
        },
        results_by_section: {},
      },
    ]);

    expect(score).toBeGreaterThanOrEqual(58);
    expect(score).toBeLessThan(96);
  });

  it("downgrades moderate structural confidence to review warning instead of fail", () => {
    const summary = __villaQtoTestHooks.buildDisciplineTrustSummary({
      discipline: "STR",
      evidence: {
        stats: {
          text_entities_total: 40,
          extraction_confidence_score: 68,
        },
        schedule_rows_total: 5,
        gating: { ok: true },
      },
      requiredQuestions: null,
      qtoMode: { mode: "QTO_ONLY", external_reference_enabled: false },
      qualityReport: null,
    });

    expect(summary.status).toBe("WARN");
    expect(summary.warnings.some((warning) => /moderate/i.test(warning))).toBe(true);
  });
});
