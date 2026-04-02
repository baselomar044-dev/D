import fs from "fs/promises";
import path from "path";

import { afterAll, describe, expect, it } from "vitest";

const workspaceRoot = path.resolve(import.meta.dirname, "..", "..");
const fixtureRoot = path.join(
	workspaceRoot,
	"uploads",
	"villa-qto-runs",
	"k-240003-1773010364239",
	"source"
);

const drawingPath = (fileName: string) => path.join(fixtureRoot, fileName);
const runRoots: string[] = [];

/** Check whether the required PDF fixtures actually exist on this machine */
async function fixturesExist(): Promise<boolean> {
	try {
		await Promise.all([
			"str_STR_DETAILS.pdf",
			"arch_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf",
			"finish_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf",
		].map((f) => fs.access(drawingPath(f))));
		return true;
	} catch {
		return false;
	}
}

afterAll(async () => {
	await Promise.all(
		runRoots.map((runRoot) =>
			fs.rm(runRoot, { recursive: true, force: true })
		)
	);
});

describe("villa QTO heavy trusted runtime", () => {
	it(
		"blocks the heavy PDF path when structural rows still depend on summary-only evidence",
		async () => {
			if (!(await fixturesExist())) {
				console.warn("[villaQtoTrustedHeavy] PDF fixtures not found — skipping heavy runtime test");
				return;
			}
			process.env.JWT_SECRET ??= "qto-runtime-test-secret";
			const { runVillaQtoProject } = await import("./villaQtoEngine");

			const result = await runVillaQtoProject({
				projectId: 930011,
				projectName: "trusted-heavy-regression",
				projectType: "g1",
				config: {
					engine: "villa_qto_v1",
					requestedProjectType: "g1",
					drawings: [
						{
							role: "str",
							discipline: "str",
							originalName: "str_STR_DETAILS.pdf",
							localPath: drawingPath("str_STR_DETAILS.pdf"),
						},
						{
							role: "arch",
							discipline: "arch",
							originalName: "arch_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf",
							localPath: drawingPath("arch_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf"),
						},
						{
							role: "finish",
							discipline: "finish",
							originalName: "finish_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf",
							localPath: drawingPath("finish_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf"),
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
						strictBlueprint: false,
					},
				},
			});

			runRoots.push(result.runMeta.runRoot);

			expect(result.trustReport.releaseDecision.gate).toBe("BLOCKED");
			expect(result.trustReport.summary.failedItems).toBeGreaterThan(0);

			const arch = result.trustReport.disciplines.find((row) => row.discipline === "ARCH");
			const finish = result.trustReport.disciplines.find((row) => row.discipline === "FINISH");
			const stairItem = result.trustReport.items.find((row) => row.itemCode === "STAIRS_INTERNAL_M3");
			const totalBitumenItem = result.trustReport.items.find((row) => row.itemCode === "BITUMEN_SUBSTRUCTURE_TOTAL_QTY");
			const footingItem = result.trustReport.items.find((row) => row.itemCode === "RCC_FOOTINGS_M3");
			const excavationItem = result.trustReport.items.find((row) => row.itemCode === "EXCAVATION_M3");
			const internalBlock8 = result.trustReport.items.find((row) => row.itemCode === "BLOCK_INTERNAL_HOLLOW_8_M2");
			const internalBlock6 = result.trustReport.items.find((row) => row.itemCode === "BLOCK_INTERNAL_HOLLOW_6_M2");

			expect(arch?.status).toBe("WARN");
			expect(finish?.status).toBe("PASS");
			expect(arch?.metrics.wallPairs).toBeGreaterThan(0);
			expect(result.trustReport.summary.totalItems).toBeGreaterThanOrEqual(30);
			expect(result.qtoRows.some((row: { item_code: string }) => row.item_code === "ROAD_BASE_M3")).toBe(false);
			expect(result.qtoRows.some((row: { item_code: string }) => row.item_code === "BITUMEN_FOUNDATIONS_QTY")).toBe(false);
			expect(result.qtoRows.some((row: { item_code: string }) => row.item_code === "BITUMEN_SOLID_BLOCK_QTY")).toBe(false);
			expect(result.qtoRows.some((row: { item_code: string }) => row.item_code === "BITUMEN_NECK_COLUMNS_QTY")).toBe(false);
			expect(stairItem?.quantity).toBe(5.4);
			expect(totalBitumenItem?.quantity ?? 0).toBeGreaterThan(0);
			expect(footingItem?.finalStatus).toBe("FAIL");
			expect(excavationItem?.finalStatus).toBe("FAIL");
			expect(footingItem?.reasons.some((reason) => /summary text/i.test(reason))).toBe(true);
			expect(Number(internalBlock8?.quantity ?? 0)).toBeGreaterThan(0);
			expect(Number(internalBlock6?.quantity ?? 0)).toBeGreaterThan(0);
			expect(internalBlock8?.finalStatus).toBe("PASS");
			expect(internalBlock6?.finalStatus).toBe("PASS");
		},
		240000
	);

	it("requires explicit road base confirmation before trusted execution", async () => {
		if (!(await fixturesExist())) {
			console.warn("[villaQtoTrustedHeavy] PDF fixtures not found — skipping road-base confirmation test");
			return;
		}
		process.env.JWT_SECRET ??= "qto-runtime-test-secret";
		const { runVillaQtoProject } = await import("./villaQtoEngine");

		await expect(
			runVillaQtoProject({
				projectId: 930012,
				projectName: "roadbase-confirmation-required",
				projectType: "g1",
				config: {
					engine: "villa_qto_v1",
					requestedProjectType: "g1",
					drawings: [
						{
							role: "str",
							discipline: "str",
							originalName: "str_STR_DETAILS.pdf",
							localPath: drawingPath("str_STR_DETAILS.pdf"),
						},
						{
							role: "arch",
							discipline: "arch",
							originalName: "arch_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf",
							localPath: drawingPath("arch_THE_MODERN_LUXURY_VILLA_2-2-2026_ARCH.pdf"),
						},
					],
					inputs: {
						excavationDepthM: 1.3,
						internalStaircaseDefaultM3: 5.4,
						hasExternalStaircase: false,
						levelReference: "NGL0",
						foundationDepthM: 1.3,
						groundFloorToFloorM: 4,
						firstFloorToFloorM: 4,
						strictBlueprint: false,
					},
				},
			})
		).rejects.toThrow(/road base requires explicit user confirmation/i);
	});
});