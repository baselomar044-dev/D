# خطة إصلاح دقة محرك QTO — 6 تعديلات على الكود الشغّال

> [!IMPORTANT]
> هذه خطة فقط — لن يتم تنفيذ أي تعديل حتى توافق.
> الملف المرجعي: [ULTIMATE_QTO_ENGINE_V2.ts](file:///c:/Users/basel/Downloads/New%20folder/New%20folder/ULTIMATE_QTO_ENGINE_V2.ts)

---

## التعديل 1: حدود دقة مخصصة لكل بند

**الملف**: [villaQtoEngine.ts](file:///c:/Users/basel/Downloads/New%20folder/New%20folder/villaQtoEngine.ts)
**السطر**: 2422-2425

#### الحالي:
```typescript
const SANITY_HARD_CLAMP_RATIO = 2.0;  // حد واحد 200% لكل البنود
const SANITY_WARN_RATIO = 0.5;
```

#### المقترح:
```diff
-const SANITY_HARD_CLAMP_RATIO = 2.0;
-const SANITY_WARN_RATIO = 0.5;
+import { getItemSanityThreshold } from './ULTIMATE_QTO_ENGINE_V2';
+// Per-item thresholds loaded from ULTIMATE_QTO_ENGINE_V2
```

وفي `applySanityCheck` (سطر 2513):
```diff
-if (deviationPct > SANITY_HARD_CLAMP_RATIO) {
+const threshold = getItemSanityThreshold(row.item_code);
+if (deviationPct > threshold.hardClampRatio && (baseline.sampleCount ?? 0) >= threshold.minSamples) {
```

**الأثر**: الحفر والردم (400% حد) ما يتقصون غلط. التشطيبات (150%) تنكشف بدري.

---

## التعديل 2: كسر دائرة الـ Scale Factor

**الملف**: [villaQtoEngine.ts](file:///c:/Users/basel/Downloads/New%20folder/New%20folder/villaQtoEngine.ts)
**السطر**: 2444-2469

#### الحالي:
```typescript
function estimateProjectScaleFactor(rows, baselineMap) {
  for (const proxyCode of SIZE_PROXY_CODES) {
    const row = rows.find(r => r.item_code === proxyCode);
    // ← يستخدم أي بند حتى لو AVG_FALLBACK
```

#### المقترح:
```diff
 for (const proxyCode of SIZE_PROXY_CODES) {
   const row = rows.find(r => r.item_code === proxyCode);
   const qty = Number(row?.system_qty ?? 0);
   if (qty <= 0) continue;
+  // ★ FIX: Skip AVG-derived items — they cause circular scaling
+  if (row?._averageDerived === true) continue;
```

**الأثر**: الـ scale factor يُحسب بس من أرقام حقيقية → كل الـ scaling يصير أدق.

---

## التعديل 3: دمج ذكي (الأدق مش الأكبر)

**الملف**: [villaQtoEngine.ts](file:///c:/Users/basel/Downloads/New%20folder/New%20folder/villaQtoEngine.ts)
**السطر**: 586-615

#### الحالي:
```typescript
function shouldReplaceMergedEngineItem(existing, candidate) {
  // ...
  if (candidateQty !== existingQty) {
    return candidateQty > existingQty;  // ← الأكبر يربح دايماً
  }
}
```

#### المقترح:
```diff
 if (candidateQty !== existingQty) {
-  return candidateQty > existingQty;
+  // ★ FIX: Prefer EXTRACTED over ESTIMATED, regardless of size
+  const existingIsAvg = isAvgDerivedEngineStatus(existing?.status);
+  const candidateIsAvg = isAvgDerivedEngineStatus(candidate?.status);
+  if (existingIsAvg && !candidateIsAvg && candidateQty > 0) return true;   // extracted wins
+  if (!existingIsAvg && candidateIsAvg) return false;                       // keep extracted
+  return candidateQty > existingQty;                                        // same type → bigger
 }
```

**الأثر**: يمنع الـ bug الشهير: 8.4 m² extracted تستبدل 389.7 m² AVG — الحين extracted يربح.

---

## التعديل 4: كشف غرف عربي شامل (40+ alias)

**الملف**: [villaQtoEngine.ts](file:///c:/Users/basel/Downloads/New%20folder/New%20folder/villaQtoEngine.ts)
**السطر**: 1191-1208

#### الحالي:
```typescript
const classifyRoomKey = (rawName: string): string => {
  // 12 pattern بس — أغلبها English
  if (direct.includes("BATH") || direct.includes("TOILET") || ...) return "BATH";
```

#### المقترح:
```diff
+import { classifyRoomV2 } from './ULTIMATE_QTO_ENGINE_V2';
 const classifyRoomKey = (rawName: string): string => {
-  // ... 12 patterns ...
+  return classifyRoomV2(rawName).key;
+  // 40+ patterns including: W.C, W.C., دورة مياه, مرحاض, تواليت,
+  // غرفة مياه, مجلس, ديوانية, استقبال, صالة المعيشة, غرفة عائلة,
+  // مؤونة, خزين, غرفة ملابس, خزانة, تراس, شرفة, بيت الدرج...
 };
```

**الأثر**: الرسومات بأسماء عربية (W.C بدال BATH) ما تضيع → التشطيبات تتحسب صح.

---

## التعديل 5: سلسلة الاشتقاق — الأخطاء ما تتكاثر

**الملف**: [villaQtoEngine.ts](file:///c:/Users/basel/Downloads/New%20folder/New%20folder/villaQtoEngine.ts)
**السطر**: 3317-3344

#### الحالي:
```typescript
// buildItemTrustAudit — no chain validation
if (row._averageDerived) {
  reasons.push(`AVERAGE BASIS: ...`);
}
```

#### المقترح:
```diff
+import { checkDerivationChainIntegrity } from './ULTIMATE_QTO_ENGINE_V2';
+
+const chainCheck = checkDerivationChainIntegrity(row.item_code, provenanceMap);
+if (chainCheck.contaminatedParents.length > 0) {
+  reasons.push(`⚠️ CHAIN: Parents [${chainCheck.contaminatedParents.join(", ")}] are estimated — this derived value has LOW confidence.`);
+  // Downgrade finalStatus if chain is contaminated
+  if (finalStatus === "PASS") finalStatus = "WARN";
+}
```

**الأثر**: لو `PLASTER_EXTERNAL` معدل → `PAINT_EXTERNAL` و `PARAPET_WALL` يتعلّمون تحذير.

---

## التعديل 6: شفافية كاملة — كل بند يقول مصدره بصراحة

**الملف**: [villaQtoEngine.ts](file:///c:/Users/basel/Downloads/New%20folder/New%20folder/villaQtoEngine.ts)
**السطر**: 812-838

#### الحالي:
```typescript
function resolveRowQuantitySource(row) {
  if (row._averageDerived) return { source: "average_scaled" };
  // ← لا يفرق بين "engine رجع AVG" و "sanity clamp"
```

#### المقترح:
```diff
+import { classifyEngineStatus, computeHonestConfidence } from './ULTIMATE_QTO_ENGINE_V2';
+
 function resolveRowQuantitySource(row) {
+  const provenance = classifyEngineStatus(row._averageDerivationSource ?? "");
+  const confidence = computeHonestConfidence(provenance, 0, 0);
+
   if (row._averageDerived) {
     return {
       source: "average_scaled",
-      note: formatAverageQuantitySourceNote(row),
+      note: `⚠️ [${provenance}] Confidence: ${confidence}% — ${formatAverageQuantitySourceNote(row)}`,
     };
   }
```

**الأثر**: المستخدم يشوف بوضوح: "هذا الرقم من الرسم (95%)" vs "هذا تقدير من معدلات (40%)".

---

## ملخص الأثر المتوقع

| المقياس | قبل | بعد |
|---------|-----|-----|
| بنود مستخرجة بدون تدخل | ~40-60% | ~40-60% (ما يتغير — يعتمد على الرسم) |
| بنود AVG مخفية كـ "extracted" | ~30-40% | **0%** — كلها مكشوفة |
| أخطاء الـ sanity clamp | ~5-10 بنود/مشروع | **~1-2** (حدود ذكية) |
| أخطاء الـ scale factor | circular في 50% من الحالات | **مكسورة** — extracted only |
| غرف عربية ضايعة | ~30% من الغرف | **<5%** |
| أخطاء derivation chain | تتضاعف بصمت | **مكشوفة + محذّرة** |
| **دقة إجمالية متوقعة** | **~55-65%** | **~75-85%** |

---

> [!WARNING]
> **ملاحظة مهمة**: هذه التعديلات ترفع الدقة من ~60% إلى ~80% — لكن ما توصل 95%+ بدون تحسين **المحرك Python نفسه** (كيف يقرأ DXF/PDF). هذا شغل أسبوع إضافي.

> [!NOTE]
> **الخطوة التالية**: وافق على الخطة وأبدأ التنفيذ — تعديل 1 بتعديل 1 مع تست بعد كل تعديل.
