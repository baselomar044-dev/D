export type MergeQuantitySource = "extracted" | "derived" | "average_scaled" | "catalog_fill";

export interface MergeableQtoRow {
  item_no: number;
  section?: string;
  item_code: string;
  discipline: string;
  unit: string;
  system_qty: number | string;
  quantitySource?: MergeQuantitySource;
  quantitySourceNote?: string;
  _catalogFill?: boolean;
  _averageDerived?: boolean;
  _averageDerivationSource?: "engine_avg_status" | "learned_overlay" | "sanity_clamp" | "baseline_relation";
  _averageScaleSource?: string;
  _averageScaleFactor?: number;
  _averageReferenceQty?: number;
  _originalSystemQty?: number | string;
  _derivedSource?: "evidence_equation" | "baseline_relation";
}

export interface MergeableEngineItem {
  item?: string;
  code?: string;
  description?: string;
  qty?: number | string;
  unit?: string;
  status?: unknown;
}

interface EngineMergeOptions<T extends MergeableEngineItem> {
  getGroupKey: (item: T) => string;
  getStatusRank: (status: unknown) => number;
}

const UNIT_ALIASES: Record<string, string> = {
  "rm": "lm",
  "r.m": "lm",
  "rmt": "lm",
  "running meter": "lm",
  "running metre": "lm",
  "m": "lm",
  "m2": "m²",
  "m^2": "m²",
  "sqm": "m²",
  "sq.m": "m²",
  "m3": "m³",
  "m^3": "m³",
  "cum": "m³",
  "cu.m": "m³",
};

const SPARSE_PROTECTED_CODES = new Set([
  "BLOCK_EXTERNAL_THERMAL_M2",
  "BLOCK_INTERNAL_HOLLOW_8_M2",
  "BLOCK_INTERNAL_HOLLOW_6_M2",
  "PLASTER_INTERNAL_M2",
  "PLASTER_EXTERNAL_M2",
  "PAINT_EXTERNAL_M2",
  "DRY_AREA_FLOORING_M2",
  "WET_AREA_FLOORING_M2",
  "BALCONY_FLOORING_M2",
  "WALL_TILES_WET_AREAS_M2",
  "CEILING_SPRAY_PLASTER_M2",
  "SKIRTING_LM",
  "WET_AREAS_BALCONY_WATERPROOF_M2",
]);

function toNumber(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function normalizeUnit(unit: unknown): string {
  const raw = String(unit ?? "").trim().toLowerCase();
  if (!raw) return "";
  return UNIT_ALIASES[raw] ?? raw;
}

function sourceRank(row: MergeableQtoRow): number {
  if (row._derivedSource === "evidence_equation") return 480;
  if (!row._averageDerived && !row._catalogFill && !row._derivedSource) return 440;
  if (row._derivedSource === "baseline_relation") return 320;
  if (row._averageDerived && row._averageDerivationSource === "learned_overlay") return 280;
  if (row._averageDerived && row._averageDerivationSource === "sanity_clamp") return 260;
  if (row._averageDerived && row._averageDerivationSource === "baseline_relation") return 240;
  if (row._averageDerived && row._averageDerivationSource === "engine_avg_status") return 180;
  if (row._catalogFill) return 60;
  return 220;
}

function metadataRichness(row: MergeableQtoRow): number {
  let score = 0;
  if (row.section) score += 10;
  if (row.quantitySource) score += 10;
  if (row.quantitySourceNote) score += 8;
  if (row._derivedSource) score += 8;
  if (row._averageDerived) score += 6;
  if (row._averageScaleSource) score += 6;
  if (row._averageScaleFactor) score += 4;
  if (row._averageReferenceQty) score += 4;
  return score;
}

function relativeSimilarity(a: number, b: number): number {
  const hi = Math.max(Math.abs(a), Math.abs(b));
  if (hi === 0) return 1;
  return Math.min(Math.abs(a), Math.abs(b)) / hi;
}

function isSparseReplacementBlocked(existing: MergeableQtoRow, candidate: MergeableQtoRow): boolean {
  const code = String(candidate.item_code || existing.item_code || "").trim().toUpperCase();
  if (!SPARSE_PROTECTED_CODES.has(code)) return false;

  const existingQty = toNumber(existing.system_qty);
  const candidateQty = toNumber(candidate.system_qty);
  if (existingQty < 50 || candidateQty <= 0) return false;

  const existingRank = sourceRank(existing);
  const candidateRank = sourceRank(candidate);
  return existingRank >= 180 && candidateRank >= 440 && candidateQty < existingQty * 0.35;
}

function reconcileUnit(existing: MergeableQtoRow, candidate: MergeableQtoRow): string {
  const existingUnit = normalizeUnit(existing.unit);
  const candidateUnit = normalizeUnit(candidate.unit);
  if (!existingUnit) return candidate.unit;
  if (!candidateUnit) return existing.unit;
  if (existingUnit === candidateUnit) {
    return existing.unit || candidate.unit;
  }
  return sourceRank(candidate) > sourceRank(existing) ? candidate.unit : existing.unit;
}

function chooseBetterRow(existing: MergeableQtoRow, candidate: MergeableQtoRow): MergeableQtoRow {
  const existingQty = toNumber(existing.system_qty);
  const candidateQty = toNumber(candidate.system_qty);
  const existingRank = sourceRank(existing);
  const candidateRank = sourceRank(candidate);
  const similarity = relativeSimilarity(existingQty, candidateQty);

  if (existingQty <= 0 && candidateQty > 0) return candidate;
  if (candidateQty <= 0 && existingQty > 0) return existing;

  if (isSparseReplacementBlocked(existing, candidate)) {
    return existing;
  }

  const unitMismatch = normalizeUnit(existing.unit) && normalizeUnit(candidate.unit) && normalizeUnit(existing.unit) !== normalizeUnit(candidate.unit);
  if (unitMismatch && existingQty > 0 && candidateQty > 0) {
    return candidateRank > existingRank + 120 ? candidate : existing;
  }

  if (candidateRank > existingRank) {
    if (!existing._catalogFill && existingRank >= 440 && similarity < 0.72 && candidateRank < existingRank + 80) {
      return existing;
    }
    return candidate;
  }

  if (existingRank > candidateRank) {
    if (existingQty > 0) return existing;
    return candidateQty > 0 ? candidate : existing;
  }

  if (similarity >= 0.92) {
    return metadataRichness(candidate) >= metadataRichness(existing) ? candidate : existing;
  }

  if (!existing._catalogFill && candidate._catalogFill) return existing;
  if (existing._catalogFill && !candidate._catalogFill) return candidate;

  if (existingRank >= 440 && candidateRank >= 440 && similarity < 0.6) {
    return existingQty >= candidateQty ? existing : candidate;
  }

  if (candidateQty !== existingQty) {
    return candidateQty > existingQty ? candidate : existing;
  }

  return metadataRichness(candidate) >= metadataRichness(existing) ? candidate : existing;
}

export function mergeComputedRowsEvidenceFirst<T extends MergeableQtoRow>(baseRows: T[], computedRows: T[]): T[] {
  const mergedRows = [...baseRows];
  for (const computedRow of computedRows) {
    const existingIdx = mergedRows.findIndex(
      (row) => row.item_code === computedRow.item_code
    );

    if (existingIdx >= 0) {
      const chosen = chooseBetterRow(mergedRows[existingIdx], computedRow) as T;
      mergedRows[existingIdx] = {
        ...chosen,
        item_no: mergedRows[existingIdx].item_no,
        unit: reconcileUnit(mergedRows[existingIdx], chosen),
      } as T;
      continue;
    }

    mergedRows.push(computedRow);
  }

  return mergedRows;
}

function isLikelyExplicitTotal(item: MergeableEngineItem): boolean {
  const label = String(item.item || item.code || item.description || "").trim();
  if (!label) return false;
  return !/[\(\)\[\]{}:#]/.test(label);
}

function metadataRichnessEngine(item: MergeableEngineItem): number {
  let score = 0;
  if (item.item) score += 10;
  if (item.description) score += 8;
  if (item.code) score += 6;
  if (String(item.status ?? "").trim()) score += 6;
  if (String(item.unit ?? "").trim()) score += 4;
  return score;
}

function chooseBetterEngineItem<T extends MergeableEngineItem>(existing: T, candidate: T, options: EngineMergeOptions<T>): T {
  const existingQty = toNumber(existing.qty);
  const candidateQty = toNumber(candidate.qty);
  const existingRank = options.getStatusRank(existing.status);
  const candidateRank = options.getStatusRank(candidate.status);
  const similarity = relativeSimilarity(existingQty, candidateQty);
  const existingExplicit = isLikelyExplicitTotal(existing);
  const candidateExplicit = isLikelyExplicitTotal(candidate);

  if (existingQty <= 0 && candidateQty > 0) return candidate;
  if (candidateQty <= 0 && existingQty > 0) return existing;

  if (candidateExplicit !== existingExplicit) {
    if (candidateExplicit && candidateRank >= existingRank - 60) return candidate;
    if (existingExplicit && existingRank >= candidateRank - 60) return existing;
  }

  if (candidateRank > existingRank) {
    if (similarity < 0.55 && existingQty > candidateQty && candidateRank < existingRank + 120) {
      return existing;
    }
    return candidate;
  }

  if (existingRank > candidateRank) {
    if (similarity < 0.55 && candidateQty > existingQty && existingRank < candidateRank + 120) {
      return candidate;
    }
    return existing;
  }

  if (similarity >= 0.92) {
    return metadataRichnessEngine(candidate) >= metadataRichnessEngine(existing) ? candidate : existing;
  }

  if (existingExplicit && !candidateExplicit) return existing;
  if (candidateExplicit && !existingExplicit) return candidate;

  if (candidateQty !== existingQty) {
    return candidateQty > existingQty ? candidate : existing;
  }

  return metadataRichnessEngine(candidate) >= metadataRichnessEngine(existing) ? candidate : existing;
}

export function mergeEngineSectionItemsByEvidence<T extends MergeableEngineItem>(
  items: T[],
  options: EngineMergeOptions<T>,
): T[] {
  const merged = new Map<string, T>();

  for (const item of items) {
    const key = options.getGroupKey(item).trim();
    if (!key) continue;

    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, item);
      continue;
    }

    merged.set(key, chooseBetterEngineItem(existing, item, options));
  }

  return Array.from(merged.values());
}
