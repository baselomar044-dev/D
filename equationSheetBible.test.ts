import { describe, it, expect } from "vitest";
import {
  loadEquationSheetBible,
  getEquation,
  getEquationsByDiscipline,
  getEquationsBySection,
} from "./equationSheetBible";

describe("equationSheetBible", () => {
  const bible = loadEquationSheetBible();

  it("loads bible with metadata", () => {
    expect(bible.version).toBe("1.5.0");
    expect(bible.fileName).toContain("BIBLE");
    expect(bible.equations.length).toBeGreaterThan(30);
  });

  it("contains all 5 sections", () => {
    const sections = new Set(bible.equations.map((e) => e.section));
    expect(sections.has(1)).toBe(true); // Substructure
    expect(sections.has(2)).toBe(true); // Superstructure
    expect(sections.has(3)).toBe(true); // Blockwork
    expect(sections.has(4)).toBe(true); // Finishing
    expect(sections.has(5)).toBe(true); // External Works
  });

  it("has at least 40 total equations (33 core + 7 external)", () => {
    expect(bible.equations.length).toBeGreaterThanOrEqual(40);
  });

  // ── Section 5: External Works items ──
  describe("External Works (Section 5)", () => {
    const externalCodes = [
      "PARAPET_WALL_M2",
      "COPING_STONES_LM",
      "ROOF_THERMAL_INSULATION_M2",
      "INTERLOCK_PAVING_M2",
      "KERB_STONES_LM",
      "BOUNDARY_WALL_LM",
      "FALSE_CEILING_M2",
    ];

    it.each(externalCodes)("contains %s", (code) => {
      const eq = getEquation(code);
      expect(eq).toBeDefined();
      expect(eq!.section).toBe(5);
    });

    it("PARAPET_WALL_M2 is ARCH discipline", () => {
      expect(getEquation("PARAPET_WALL_M2")!.discipline).toBe("ARCH");
    });

    it("BOUNDARY_WALL_LM is ARCH discipline", () => {
      expect(getEquation("BOUNDARY_WALL_LM")!.discipline).toBe("ARCH");
    });

    it("INTERLOCK_PAVING_M2 is FINISH discipline", () => {
      expect(getEquation("INTERLOCK_PAVING_M2")!.discipline).toBe("FINISH");
    });
  });

  // ── Staircase auto-calculation formula ──
  it("STAIRS has auto-calculation notes with G+2 rule", () => {
    const stairs = getEquation("STAIRS_INTERNAL_M3")!;
    expect(stairs.notes).toContain("G+2");
    expect(stairs.notes).toContain("32 steps");
  });

  // ── Arabic descriptions ──
  it("every equation has an Arabic description", () => {
    for (const eq of bible.equations) {
      expect(eq.descriptionAr).toBeTruthy();
    }
  });

  // ── Disciplines ──
  it("getEquationsByDiscipline returns correct counts", () => {
    const str = getEquationsByDiscipline("STR");
    const arch = getEquationsByDiscipline("ARCH");
    const finish = getEquationsByDiscipline("FINISH");

    expect(str.length).toBeGreaterThanOrEqual(17);   // 10 sub + 7 super
    expect(arch.length).toBeGreaterThanOrEqual(6);    // 4 block + parapet + boundary
    expect(finish.length).toBeGreaterThanOrEqual(17); // 12 core + 5 external
  });

  // ── Lookups ──
  it("getEquation returns undefined for unknown code", () => {
    expect(getEquation("NONEXISTENT_CODE")).toBeUndefined();
  });

  it("getEquationsBySection returns all section 1 items", () => {
    const sub = getEquationsBySection(1);
    expect(sub.length).toBe(10);
    expect(sub.every((e) => e.sectionName === "Substructure")).toBe(true);
  });

  // ── Unique codes ──
  it("all equation codes are unique", () => {
    const codes = bible.equations.map((e) => e.code);
    expect(new Set(codes).size).toBe(codes.length);
  });
});
