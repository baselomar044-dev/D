import fs from "fs/promises";
import path from "path";

import { getDocument, OPS, VerbosityLevel } from "pdfjs-dist/legacy/build/pdf.mjs";

type Matrix = [number, number, number, number, number, number];

interface Point {
  x: number;
  y: number;
}

interface Polyline {
  points: Point[];
  closed: boolean;
}

interface DxfTextEntity {
  text: string;
  x: number;
  y: number;
  height: number;
}

interface ConversionProfile {
  maxPolylines: number;
  maxPointsPerPolyline: number;
  maxTexts: number;
  minPolylineLength: number;
  minSegmentLength: number;
  pointPrecision: number;
}

interface PdfOperatorList {
  fnArray: number[];
  argsArray: unknown[];
}

function normalizeNumericPayload(value: unknown): number[] {
  const numbers: number[] = [];
  const pending: unknown[] = [value];

  while (pending.length > 0) {
    const current = pending.pop();

    if (typeof current === "number") {
      numbers.push(current);
      continue;
    }

    if (ArrayBuffer.isView(current)) {
      const view = current as unknown as ArrayLike<number>;
      for (let index = 0; index < view.length; index += 1) {
        numbers.push(view[index]);
      }
      continue;
    }

    if (Array.isArray(current)) {
      for (let index = current.length - 1; index >= 0; index -= 1) {
        pending.push(current[index]);
      }
    }
  }

  return numbers;
}

const IDENTITY_MATRIX: Matrix = [1, 0, 0, 1, 0, 0];
const CURVE_SEGMENTS = 8;
const PAGE_GAP = 1000;

/**
 * pdfjs-dist constructPath sub-op codes (OPS.moveTo, OPS.lineTo, etc.).
 * These are NOT 0-based sequential values — they match the actual pdfjs OPS enum.
 */
const DRAW_OP_MOVE_TO = 13;
const DRAW_OP_LINE_TO = 14;
const DRAW_OP_CURVE_TO = 15;       // curveTo (cubic, 3 control points = 6 coords)
const DRAW_OP_CURVE_TO2 = 16;      // curveTo2 (cubic shorthand, 2 control points = 4 coords)
const DRAW_OP_CURVE_TO3 = 17;      // curveTo3 (quadratic, 2 control points = 4 coords)
const DRAW_OP_CLOSE_PATH = 18;
const DRAW_OP_RECTANGLE = 19;
const DEFAULT_PROFILE: ConversionProfile = {
  maxPolylines: 40000,
  maxPointsPerPolyline: 200,
  maxTexts: 12000,
  minPolylineLength: 2,
  minSegmentLength: 0.75,
  pointPrecision: 0.5,
};
const STR_PROFILE: ConversionProfile = {
  maxPolylines: 60000,
  maxPointsPerPolyline: 250,
  maxTexts: 1000,
  minPolylineLength: 1,
  minSegmentLength: 0.5,
  pointPrecision: 0.25,
};
const ARCH_PROFILE: ConversionProfile = {
  maxPolylines: 35000,
  maxPointsPerPolyline: 250,
  maxTexts: 16000,
  minPolylineLength: 2,
  minSegmentLength: 0.5,
  pointPrecision: 0.25,
};
const FINISH_PROFILE: ConversionProfile = {
  maxPolylines: 25000,
  maxPointsPerPolyline: 200,
  maxTexts: 20000,
  minPolylineLength: 2,
  minSegmentLength: 0.5,
  pointPrecision: 0.4,
};

function normalizeTextValue(value: string): string {
  return value
    .replaceAll('\u0000', "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\\/g, "/")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeDxfText(value: string): string {
  return value.replace(/\n/g, " ").replace(/\r/g, " ");
}

function multiplyMatrices(left: Matrix, right: Matrix): Matrix {
  return [
    left[0] * right[0] + left[2] * right[1],
    left[1] * right[0] + left[3] * right[1],
    left[0] * right[2] + left[2] * right[3],
    left[1] * right[2] + left[3] * right[3],
    left[0] * right[4] + left[2] * right[5] + left[4],
    left[1] * right[4] + left[3] * right[5] + left[5],
  ];
}

function applyMatrix(matrix: Matrix, x: number, y: number): Point {
  return {
    x: matrix[0] * x + matrix[2] * y + matrix[4],
    y: matrix[1] * x + matrix[3] * y + matrix[5],
  };
}

function roundCoord(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function samePoint(a: Point | null, b: Point | null): boolean {
  if (!a || !b) return false;
  return Math.abs(a.x - b.x) < 0.001 && Math.abs(a.y - b.y) < 0.001;
}

function distanceBetween(a: Point, b: Point): number {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

function quantizePoint(point: Point, precision: number): Point {
  return {
    x: Math.round(point.x / precision) * precision,
    y: Math.round(point.y / precision) * precision,
  };
}

function polylineLength(points: Point[]): number {
  let total = 0;
  for (let index = 1; index < points.length; index += 1) {
    total += distanceBetween(points[index - 1], points[index]);
  }
  return total;
}

function simplifyPolyline(polyline: Polyline, profile: ConversionProfile): Polyline | null {
  const simplified: Point[] = [];

  for (const sourcePoint of polyline.points) {
    const point = quantizePoint(sourcePoint, profile.pointPrecision);
    const lastPoint = simplified[simplified.length - 1];

    if (!lastPoint || distanceBetween(lastPoint, point) >= profile.minSegmentLength) {
      simplified.push(point);
    }

    if (simplified.length >= profile.maxPointsPerPolyline) {
      break;
    }
  }

  if (simplified.length < 2) return null;

  const length = polylineLength(simplified);
  if (length < profile.minPolylineLength) return null;

  return {
    points: simplified,
    closed: polyline.closed,
  };
}

function reducePolylines(polylines: Polyline[], profile: ConversionProfile): Polyline[] {
  const scored = polylines
    .map((polyline) => {
      const simplified = simplifyPolyline(polyline, profile);
      if (!simplified) return null;
      const length = polylineLength(simplified.points);
      return {
        simplified,
        score: length + simplified.points.length * 0.25,
      };
    })
    .filter((entry): entry is { simplified: Polyline; score: number } => Boolean(entry));

  scored.sort((left, right) => right.score - left.score);

  return scored.slice(0, profile.maxPolylines).map((entry) => entry.simplified);
}

function getProfile(role: "str" | "arch" | "finish" | undefined): ConversionProfile {
  if (role === "str") return STR_PROFILE;
  if (role === "arch") return ARCH_PROFILE;
  if (role === "finish") return FINISH_PROFILE;
  return DEFAULT_PROFILE;
}

function cubicBezierPoint(start: Point, c1: Point, c2: Point, end: Point, t: number): Point {
  const mt = 1 - t;
  const mt2 = mt * mt;
  const t2 = t * t;
  return {
    x: mt2 * mt * start.x + 3 * mt2 * t * c1.x + 3 * mt * t2 * c2.x + t2 * t * end.x,
    y: mt2 * mt * start.y + 3 * mt2 * t * c1.y + 3 * mt * t2 * c2.y + t2 * t * end.y,
  };
}

function encodeDxfPair(code: number, value: string | number): string {
  return `${code}\n${value}\n`;
}

function getDxfInsUnits(role: "str" | "arch" | "finish" | undefined): number {
  if (role === "arch" || role === "finish") return 5;
  return 6;
}

function buildDxfFromPolylines(polylines: Polyline[], insUnits: number): string {
  let body = "";

  for (const polyline of polylines) {
    if (polyline.points.length < 2) continue;

    const points = [...polyline.points];
    if (polyline.closed && !samePoint(points[0], points[points.length - 1])) {
      points.push(points[0]);
    }

    for (let index = 1; index < points.length; index += 1) {
      const start = points[index - 1];
      const end = points[index];
      if (samePoint(start, end)) continue;

      body += encodeDxfPair(0, "LINE");
      body += encodeDxfPair(8, "0");
      body += encodeDxfPair(10, roundCoord(start.x));
      body += encodeDxfPair(20, roundCoord(start.y));
      body += encodeDxfPair(30, 0);
      body += encodeDxfPair(11, roundCoord(end.x));
      body += encodeDxfPair(21, roundCoord(end.y));
      body += encodeDxfPair(31, 0);
    }
  }

  return body;
}

function buildDxfDocument(polylines: Polyline[], texts: DxfTextEntity[], insUnits: number): string {
  let body = "";

  body += buildDxfFromPolylines(polylines, insUnits);

  for (const text of texts) {
    body += encodeDxfPair(0, "TEXT");
    body += encodeDxfPair(8, "0");
    body += encodeDxfPair(10, roundCoord(text.x));
    body += encodeDxfPair(20, roundCoord(text.y));
    body += encodeDxfPair(30, 0);
    body += encodeDxfPair(40, roundCoord(text.height));
    body += encodeDxfPair(1, escapeDxfText(text.text));
  }

  return [
    encodeDxfPair(0, "SECTION"),
    encodeDxfPair(2, "HEADER"),
    encodeDxfPair(9, "$ACADVER"),
    encodeDxfPair(1, "AC1015"),
    encodeDxfPair(9, "$INSUNITS"),
    encodeDxfPair(70, insUnits),
    encodeDxfPair(0, "ENDSEC"),
    encodeDxfPair(0, "SECTION"),
    encodeDxfPair(2, "ENTITIES"),
    body,
    encodeDxfPair(0, "ENDSEC"),
    encodeDxfPair(0, "EOF"),
  ].join("");
}

function scoreTextEntity(text: DxfTextEntity): number {
  const alphaCount = (text.text.match(/[A-Za-z\u0600-\u06FF]/g) || []).length;
  const digitCount = (text.text.match(/[0-9]/g) || []).length;
  return text.text.length + alphaCount * 2 + digitCount;
}

function reduceTextEntities(texts: DxfTextEntity[], profile: ConversionProfile): DxfTextEntity[] {
  const seen = new Set<string>();
  const filtered: DxfTextEntity[] = [];

  for (const text of texts) {
    const normalized = normalizeTextValue(text.text);
    if (!normalized) continue;
    const x = roundCoord(text.x);
    const y = roundCoord(text.y);
    const height = Math.max(0.2, roundCoord(text.height));
    if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(height)) continue;
    const key = `${normalized}|${x}|${y}`;
    if (seen.has(key)) continue;
    seen.add(key);
    filtered.push({ text: normalized, x, y, height });
  }

  filtered.sort((left, right) => scoreTextEntity(right) - scoreTextEntity(left));
  return filtered.slice(0, profile.maxTexts);
}

async function extractTextEntitiesFromPage(params: {
  page: any;
  pageHeight: number;
  pageOffsetX: number;
}): Promise<DxfTextEntity[]> {
  const { page, pageHeight, pageOffsetX } = params;
  const textContent = await page.getTextContent();
  const texts: DxfTextEntity[] = [];

  for (const item of textContent.items as Array<{ str?: string; transform?: number[]; height?: number; width?: number }>) {
    const rawText = typeof item?.str === "string" ? normalizeTextValue(item.str) : "";
    if (!rawText) continue;
    const transform = Array.isArray(item?.transform) ? item.transform : null;
    if (!transform || transform.length < 6) continue;

    const x = Number(transform[4]) + pageOffsetX;
    const y = pageHeight - Number(transform[5]);
    const transformHeight = Math.hypot(Number(transform[2]) || 0, Number(transform[3]) || 0);
    const fallbackHeight = Number(item?.height) || Number(item?.width) || 2.5;
    const height = Math.max(0.2, transformHeight || fallbackHeight);

    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    texts.push({ text: rawText, x, y, height });
  }

  return texts;
}

function extractPolylinesFromOperatorList(params: {
  operatorList: PdfOperatorList;
  pageHeight: number;
  pageOffsetX: number;
}): Polyline[] {
  const { operatorList, pageHeight, pageOffsetX } = params;
  const polylines: Polyline[] = [];
  const stateStack: Matrix[] = [];
  let currentMatrix: Matrix = [...IDENTITY_MATRIX];

  for (let index = 0; index < operatorList.fnArray.length; index += 1) {
    const fn = operatorList.fnArray[index];
    const args = operatorList.argsArray[index] || [];

    if (fn === OPS.save) {
      stateStack.push([...currentMatrix]);
      continue;
    }

    if (fn === OPS.restore) {
      currentMatrix = stateStack.pop() || [...IDENTITY_MATRIX];
      continue;
    }

    if (fn === OPS.transform) {
      const [a, b, c, d, e, f] = args as number[];
      currentMatrix = multiplyMatrices(currentMatrix, [a, b, c, d, e, f]);
      continue;
    }

    if (fn !== OPS.constructPath) {
      continue;
    }

    /**
     * pdfjs-dist constructPath passes 3 args:
     *   args[0] = sub-op codes array (e.g. [13, 14, 14, 18] = moveTo, lineTo, lineTo, closePath)
     *   args[1] = flat coordinates array (x0, y0, x1, y1, ...)
     *   args[2] = copy of args[1] (unused)
     */
    const subOps = normalizeNumericPayload((args as unknown[])[0]);
    const coords = normalizeNumericPayload((args as unknown[])[1]);
    if (subOps.length === 0 || coords.length === 0) continue;

    let coordIndex = 0;
    let currentPolyline: Point[] = [];
    let currentPoint: Point | null = null;
    let startPoint: Point | null = null;

    const flushPolyline = (closed: boolean) => {
      if (currentPolyline.length >= 2) {
        polylines.push({ points: [...currentPolyline], closed });
      }
      currentPolyline = [];
      currentPoint = null;
      startPoint = null;
    };

    const mapPoint = (x: number, y: number): Point => {
      const transformed = applyMatrix(currentMatrix, x, y);
      return {
        x: transformed.x + pageOffsetX,
        y: pageHeight - transformed.y,
      };
    };

    for (let opIdx = 0; opIdx < subOps.length; opIdx += 1) {
      const op = subOps[opIdx];

      if (op === DRAW_OP_MOVE_TO) {
        flushPolyline(false);
        const point = mapPoint(coords[coordIndex], coords[coordIndex + 1]);
        coordIndex += 2;
        currentPolyline.push(point);
        currentPoint = point;
        startPoint = point;
        continue;
      }

      if (op === DRAW_OP_LINE_TO) {
        const point = mapPoint(coords[coordIndex], coords[coordIndex + 1]);
        coordIndex += 2;
        if (!currentPoint) {
          currentPolyline.push(point);
          startPoint = point;
        } else if (!samePoint(currentPoint, point)) {
          currentPolyline.push(point);
        }
        currentPoint = point;
        continue;
      }

      if (op === DRAW_OP_CLOSE_PATH) {
        if (currentPolyline.length >= 2) {
          flushPolyline(true);
        }
        continue;
      }

      if (op === DRAW_OP_RECTANGLE) {
        // Rectangle: 4 coords = x, y, width, height
        flushPolyline(false);
        const rx = coords[coordIndex];
        const ry = coords[coordIndex + 1];
        const rw = coords[coordIndex + 2];
        const rh = coords[coordIndex + 3];
        coordIndex += 4;
        const p1 = mapPoint(rx, ry);
        const p2 = mapPoint(rx + rw, ry);
        const p3 = mapPoint(rx + rw, ry + rh);
        const p4 = mapPoint(rx, ry + rh);
        polylines.push({ points: [p1, p2, p3, p4, p1], closed: true });
        currentPoint = null;
        startPoint = null;
        continue;
      }

      if (op === DRAW_OP_CURVE_TO) {
        // Cubic Bézier: 6 coords (c1x, c1y, c2x, c2y, endX, endY)
        if (!currentPoint) {
          coordIndex += 6;
          continue;
        }

        const c1 = mapPoint(coords[coordIndex], coords[coordIndex + 1]);
        const c2 = mapPoint(coords[coordIndex + 2], coords[coordIndex + 3]);
        const end = mapPoint(coords[coordIndex + 4], coords[coordIndex + 5]);
        coordIndex += 6;

        for (let step = 1; step <= CURVE_SEGMENTS; step += 1) {
          const point = cubicBezierPoint(currentPoint, c1, c2, end, step / CURVE_SEGMENTS);
          if (!samePoint(currentPolyline[currentPolyline.length - 1] || null, point)) {
            currentPolyline.push(point);
          }
        }
        currentPoint = end;
        continue;
      }

      if (op === DRAW_OP_CURVE_TO2 || op === DRAW_OP_CURVE_TO3) {
        // curveTo2/curveTo3: 4 coords (cx, cy, endX, endY)
        if (!currentPoint) {
          coordIndex += 4;
          continue;
        }

        const c1 = mapPoint(coords[coordIndex], coords[coordIndex + 1]);
        const end = mapPoint(coords[coordIndex + 2], coords[coordIndex + 3]);
        coordIndex += 4;
        const c2 = {
          x: (c1.x + end.x) / 2,
          y: (c1.y + end.y) / 2,
        };

        for (let step = 1; step <= CURVE_SEGMENTS; step += 1) {
          const point = cubicBezierPoint(currentPoint, c1, c2, end, step / CURVE_SEGMENTS);
          if (!samePoint(currentPolyline[currentPolyline.length - 1] || null, point)) {
            currentPolyline.push(point);
          }
        }
        currentPoint = end;
        continue;
      }

      // Unknown op — skip
    }

    if (currentPolyline.length >= 2) {
      const isClosed = startPoint ? samePoint(startPoint, currentPolyline[currentPolyline.length - 1]) : false;
      flushPolyline(isClosed);
    }
  }

  return polylines;
}

export async function convertPdfVectorToDxf(params: {
  pdfPath: string;
  outputPath?: string;
  role?: "str" | "arch" | "finish";
}): Promise<{ outputPath: string; pageCount: number; polylineCount: number; textCount: number }> {
  const pdfPath = path.resolve(params.pdfPath);
  const outputPath = path.resolve(
    params.outputPath || pdfPath.replace(/\.pdf$/i, ".runtime.dxf")
  );

  const data = await fs.readFile(pdfPath);
  const loadingTask = getDocument({
    data: new Uint8Array(data),
    disableWorker: true,
    isEvalSupported: false,
    useSystemFonts: false,
    useWasm: false,
    verbosity: VerbosityLevel.ERRORS,
    stopAtErrors: false,
  } as Parameters<typeof getDocument>[0]);

  const document = await loadingTask.promise;
  const profile = getProfile(params.role);
  const insUnits = getDxfInsUnits(params.role);

  try {
    const polylines: Polyline[] = [];
    const texts: DxfTextEntity[] = [];
    let pageOffsetX = 0;

    // Cap for raw polylines across all pages to prevent OOM on large PDFs
    const RAW_POLYLINE_CAP = profile.maxPolylines * 3;
    const RAW_TEXT_CAP = profile.maxTexts * 2;

    for (let pageNumber = 1; pageNumber <= document.numPages; pageNumber += 1) {
      const page = await document.getPage(pageNumber);
      const viewport = page.getViewport({ scale: 1 });
      const operatorList = await page.getOperatorList();
      const pagePolylines = extractPolylinesFromOperatorList({
        operatorList,
        pageHeight: viewport.height,
        pageOffsetX,
      });
      const pageTexts = await extractTextEntitiesFromPage({
        page,
        pageHeight: viewport.height,
        pageOffsetX,
      });

      // Only accumulate up to cap — stop early to avoid unbounded growth
      if (polylines.length < RAW_POLYLINE_CAP) {
        const remaining = RAW_POLYLINE_CAP - polylines.length;
        for (let i = 0; i < Math.min(pagePolylines.length, remaining); i++) {
          polylines.push(pagePolylines[i]);
        }
      }
      if (texts.length < RAW_TEXT_CAP) {
        const remaining = RAW_TEXT_CAP - texts.length;
        for (let i = 0; i < Math.min(pageTexts.length, remaining); i++) {
          texts.push(pageTexts[i]);
        }
      }

      pageOffsetX += viewport.width + PAGE_GAP;
      page.cleanup();
    }

    const reducedPolylines = reducePolylines(polylines, profile);
    const reducedTexts = reduceTextEntities(texts, profile);

    if (reducedPolylines.length === 0 && reducedTexts.length === 0) {
      throw new Error("The PDF does not contain vector paths that can be converted to trusted-mode geometry.");
    }

    await fs.writeFile(outputPath, buildDxfDocument(reducedPolylines, reducedTexts, insUnits), "utf8");

    return {
      outputPath,
      pageCount: document.numPages,
      polylineCount: reducedPolylines.length,
      textCount: reducedTexts.length,
    };
  } finally {
    await document.destroy();
  }
}
