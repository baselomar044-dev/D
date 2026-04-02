# 🏗️ QTO Engine — UAE Quantity Take-Off Automation

# محرك حساب الكميات الآلي — الإمارات العربية المتحدة

> **Transform architectural drawings into accurate BOQ (Bill of Quantities) automatically.**
>
> **حوّل المخططات المعمارية إلى جداول كميات دقيقة تلقائياً.**

---

## 🎯 What Is This? — ما هذا؟

A complete **QTO (Quantity Take-Off) automation system** built for UAE villa construction projects. It reads DXF/PDF drawings, extracts structural and architectural quantities, validates them against historical data from **318 real UAE projects**, and generates professional Excel BOQ reports.

نظام **حساب كميات آلي** متكامل مصمم لمشاريع الفلل في الإمارات. يقرأ مخططات DXF/PDF، يستخرج الكميات الإنشائية والمعمارية، يتحقق منها مقابل بيانات تاريخية من **٣١٨ مشروع حقيقي في الإمارات**، وينتج تقارير BOQ احترافية بصيغة Excel.

---

## ⚡ Quick Start — البداية السريعة

### Prerequisites — المتطلبات

```bash
# Python 3.10+
pip install -r requirements.txt
```

### Run with Sample Data — تشغيل بالبيانات التجريبية

```bash
python -m src.main --sample --type "G+1" --output boq_output.xlsx
```

### Run with DXF Drawing — تشغيل بمخطط DXF

```bash
python -m src.main --input drawing.dxf --type "G+1" --output boq.xlsx --plot-area 557
```

### Run with PDF Drawing (Gemini Vision) — تشغيل بمخطط PDF

```bash
export GEMINI_API_KEY="your-api-key"
python -m src.main --input plan.pdf --type "G+1" --output boq.xlsx --api-key $GEMINI_API_KEY
```

---

## 📦 Project Structure — هيكل المشروع

```
D/
├── src/                          # 🐍 Python QTO Engine Core
│   ├── main.py                   # CLI entry point — نقطة الدخول
│   ├── engine/
│   │   ├── qto_engine.py         # Master orchestrator — المحرك الرئيسي
│   │   ├── sub_structure.py      # Foundations, excavation — الأساسات والحفر
│   │   ├── super_structure.py    # Slabs, beams, columns — البلاطات والكمرات والأعمدة
│   │   └── finishes.py           # Block, plaster, tiles — البلوك واللياسة والبلاط
│   ├── parsers/
│   │   ├── dxf_parser.py         # DXF/DWG extraction — استخراج من ملفات الأوتوكاد
│   │   └── pdf_parser.py         # PDF via Gemini Vision — استخراج من PDF بالذكاء الاصطناعي
│   ├── output/
│   │   └── excel_generator.py    # Professional Excel BOQ — تقرير Excel احترافي
│   └── validation/
│       └── validator.py          # Confidence scoring — تقييم الثقة والدقة
│
├── config/                       # ⚙️ Configuration — الإعدادات
│   ├── formulas.json             # QTO calculation formulas — معادلات الحساب
│   ├── rates.json                # Unit rates in AED — أسعار الوحدات بالدرهم
│   ├── averages.json             # Historical averages (318 projects) — متوسطات تاريخية
│   └── thresholds.json           # Validation thresholds — حدود التحقق
│
├── samples/                      # 📋 Sample Data — بيانات تجريبية
│   └── sample_input.json         # G+1 villa example — مثال فيلا G+1
│
├── ts/                           # 📘 TypeScript/Node.js Runtime
│   ├── villaQtoEngine.ts         # Node.js pipeline orchestrator
│   ├── equationSheetBible.ts     # Master equation definitions
│   ├── pdfVectorToDxf.ts         # PDF vector → DXF conversion
│   ├── cadLayerTraining.ts       # CAD layer ML training
│   ├── asyncQueue.ts             # Concurrency limiter
│   ├── v15Export.ts              # Excel export (ExcelJS)
│   └── runtime/
│       ├── dxfRuntimeUtils.js    # DXF entity processing
│       ├── wallRuntimeUtils.js   # Wall detection algorithms
│       ├── runArch.js            # Architectural extraction
│       ├── runFinish.js          # Finishes extraction
│       ├── runStr.js             # Structural extraction
│       ├── accuracyPolicy.js     # Accuracy gates
│       ├── quality.js            # Quality checks
│       └── rules.js              # QTO rules loader
│
├── data/                         # 📊 Training & Reference Data
│   ├── TRAINING_DATASET_COMPLETE.json
│   ├── project_corpus_manifest.json
│   ├── project_corpus_summary.json
│   ├── agent1_spec_training_pack.json
│   ├── agent2_qto_training_pack.json
│   ├── agent3_boq_training_pack.json
│   └── QTO_AVERAGE_AREA_SUMMARY.txt
│
├── legacy/                       # 🏚️ Legacy V15 Monolith
│   └── UAE_MASTER_QTO_ENGINE_V15.py
│
├── requirements.txt              # Python dependencies
├── pyproject.toml                # Python project config
├── Dockerfile                    # Container deployment
└── README.md                     # This file — هذا الملف
```

---

## 🔢 What It Calculates — ماذا يحسب؟

### Sub-Structure — الأعمال تحت الأرض (١٥ بند)
| # | Item — البند | Unit — الوحدة |
|---|---|---|
| 1 | Excavation — الحفريات | m³ |
| 2 | Foundation Concrete — خرسانة الأساسات | m³ |
| 3 | Plain Concrete (PCC) — خرسانة عادية | m³ |
| 4 | Neck Columns — أعمدة الرقبة | m³ |
| 5 | Tie Beams — كمرات الربط | m³ |
| 6 | Slab on Grade — بلاطة أرضية | m³ |
| 7 | Backfill & Compaction — ردم وحدل | m³ |
| 8 | Anti-Termite — مبيد النمل الأبيض | m² |
| 9 | Polyethylene Sheet — نايلون أسود | m² |
| 10 | Bitumen Waterproofing — عزل بيتومين | m² |
| 11 | Road Base — رصيف طرق | m³ |

### Super-Structure — الأعمال الإنشائية فوق الأرض (١٠ بنود)
| # | Item — البند | Unit — الوحدة |
|---|---|---|
| 1 | Columns — الأعمدة | m³ |
| 2 | First Floor Slab — بلاطة الدور الأول | m³ |
| 3 | Roof Slab — بلاطة السقف | m³ |
| 4 | Floor Beams — كمرات الأدوار | m³ |
| 5 | Staircase — الدرج | m³ |
| 6 | Parapet Wall — تصوينة السطح | m² |

### Finishes — التشطيبات (٢٠+ بند)
| # | Item — البند | Unit — الوحدة |
|---|---|---|
| 1 | External Thermal Block — بلوك حراري خارجي | m² |
| 2 | Internal Block 20cm — بلوك داخلي ٢٠سم | m² |
| 3 | Internal Block 10cm — بلوك داخلي ١٠سم | m² |
| 4 | Internal Plaster — لياسة داخلية | m² |
| 5 | External Plaster — لياسة خارجية | m² |
| 6 | Internal Paint — دهان داخلي | m² |
| 7 | External Paint — دهان خارجي | m² |
| 8 | Dry Area Flooring — بلاط مناطق جافة | m² |
| 9 | Wet Area Flooring — بلاط مناطق مبللة | m² |
| 10 | Wall Tiles — بلاط جدران | m² |
| 11 | Skirting — وزرة | RM |
| 12 | Marble Threshold — عتبة رخام | RM |
| 13 | False Ceiling — سقف مستعار | m² |
| 14 | Waterproofing — عزل مائي | m² |
| 15 | Roof Waterproofing — عزل السقف | m² |
| 16 | Interlock Paving — بلاط متداخل | m² |
| 17 | Boundary Wall — سور خارجي | RM |

---

## 📊 Validation System — نظام التحقق

Every quantity is validated against **318 real UAE villa projects**:

- 🟢 **GREEN** (≥95% confidence): Matches historical data closely
- 🟡 **YELLOW** (90-95%): Minor deviation — check recommended
- 🔴 **RED** (<90%): Significant deviation — manual review required
- 🟣 **PURPLE**: Estimated from historical average (no direct measurement)

**Cross-ratio checks** verify internal consistency:
- Thermal block ÷ Internal block ≈ 1.15 (for G+1)
- Internal plaster ÷ Dry area flooring ≈ 5.82 (for G+1)
- External plaster ÷ Thermal block ≈ 1.70 (for G+1)

---

## 🏗️ Supported Project Types — أنواع المشاريع المدعومة

| Type — النوع | Projects — عدد المشاريع | Avg Cost — متوسط التكلفة |
|---|---|---|
| **G** (Ground only) | 233 | 180,928 AED |
| **G+1** (Ground + First) | 33 | 641,278 AED |
| **G+2** (Ground + First + Second) | 18 | 927,255 AED |
| **G+1 Service** (with service block) | 15 | 2,019,933 AED |

---

## 🐳 Docker — تشغيل بالحاوية

```bash
docker build -t qto-engine .
docker run -v $(pwd)/output:/app/output qto-engine \
    --sample --type "G+1" --output /app/output/boq.xlsx
```

---

## 📄 CLI Options — خيارات سطر الأوامر

```
usage: qto [-h] (--input FILE | --sample) [--type {G,G+1,G+2,G+1 Service}]
            [--output FILE] [--plot-area M2] [--api-key KEY]
            [--gemini-model MODEL] [--max-pages N]

Options:
  --input, -i FILE       DXF, DWG, or PDF drawing file
  --sample               Use built-in sample data
  --type, -t TYPE        Project type (default: G+1)
  --output, -o FILE      Output Excel file (default: boq_output.xlsx)
  --plot-area M2         Plot area override in m²
  --api-key KEY          Gemini API key (required for PDF)
  --gemini-model MODEL   Gemini model (default: gemini-2.0-flash-lite)
  --max-pages N          Max PDF pages to process (default: 3)
```

---

## 🧠 How It Works — كيف يعمل؟

```
Drawing (DXF/PDF) → Parser → Normalized Data → QTO Engine → Validator → Excel BOQ
                                                    ↓
                                            Sub-Structure
                                            Super-Structure
                                            Finishes
```

1. **Parser** reads drawings (DXF via ezdxf, PDF via Gemini Vision AI)
2. **QTO Engine** calculates 50+ items using UAE construction formulas
3. **Validator** scores confidence against 318 real project averages
4. **Excel Generator** produces color-coded professional BOQ

---

## 📜 License — الرخصة

Proprietary — Basel Omar. All rights reserved.

---

## 🤝 Credits — الشكر

Built with domain expertise from **318 real UAE villa projects** across Dubai, Sharjah, Ajman, Umm Al Quwain, and Ras Al Khaimah.

تم بناؤه بخبرة ميدانية من **٣١٨ مشروع فيلا حقيقي في الإمارات** عبر دبي والشارقة وعجمان وأم القيوين ورأس الخيمة.
