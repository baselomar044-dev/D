/**
 * UAE Master QTO Equation Sheet Bible — V15
 * Source of truth for all QTO calculations used by the Villa QTO Engine.
 * Based on UAE construction standards & QS best practices.
 */

export interface EquationDef {
  code: string;
  description: string;
  descriptionAr: string;
  discipline: "STR" | "ARCH" | "FINISH";
  section: number;
  sectionName: string;
  unit: string;
  formula: string;
  notes: string;
}

export interface EquationBible {
  fileName: string;
  sheetName: string;
  updatedAt: string;
  version: string;
  equations: EquationDef[];
}

const MASTER_EQUATIONS: EquationDef[] = [
  // ─── SECTION 1: SUBSTRUCTURE (STR) ───
  { code: "EXCAVATION_M3", description: "Excavation for Foundations", descriptionAr: "حفريات الأساسات", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Σ(Foundation_Area × Excavation_Depth) + Working_Space_Allowance", notes: "Depth from NGL to bottom of PCC. Working space 300mm each side." },
  { code: "PLAIN_CONCRETE_UNDER_FOOTINGS_M3", description: "Plain Concrete Under Footings (PCC)", descriptionAr: "خرسانة عادية تحت القواعد", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Σ(Footing_L × Footing_W × PCC_Thickness)", notes: "PCC thickness typically 100mm. Grade C15/20." },
  { code: "RCC_FOOTINGS_M3", description: "RCC Footings", descriptionAr: "قواعد خرسانة مسلحة", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Σ(Footing_L × Footing_W × Footing_D)", notes: "Isolated/combined/raft as per structural drawing." },
  { code: "NECK_COLUMNS_M3", description: "Neck Columns", descriptionAr: "أعمدة رقبة", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Σ(Col_Width × Col_Depth × Neck_Height) × Count", notes: "Height from top of footing to bottom of tie beam." },
  { code: "TIE_BEAMS_M3", description: "Tie Beams / Ground Beams", descriptionAr: "جسور ربط أرضية", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Σ(Beam_L × Beam_W × Beam_D)", notes: "Measured centre-to-centre between columns." },
  { code: "BACKFILL_COMPACTION_M3", description: "Backfill & Compaction", descriptionAr: "ردم وحدل", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Excavation_Volume - (Footings + PCC + Tie_Beams + Neck_Columns)", notes: "Approved fill material, compacted in 200mm layers." },
  { code: "ANTI_TERMITE_QTY", description: "Anti-Termite Treatment", descriptionAr: "معالجة ضد النمل الأبيض", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m²", formula: "Ground_Floor_Area + Perimeter × Foundation_Depth", notes: "Applied under slab on grade and around foundation walls." },
  { code: "POLYTHENE_SHEET_M2", description: "Polyethylene Sheet", descriptionAr: "شريحة بولي إيثيلين", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m²", formula: "Slab_on_Grade_Area × 1.15", notes: "1000 gauge, 150mm overlap allowance (15%)." },
  { code: "SUBGRADE_FLOOR_SLAB_M3", description: "Slab on Grade", descriptionAr: "بلاطة أرضية", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Floor_Area × Slab_Thickness", notes: "Typical thickness 150-200mm for G+1 villas." },
  { code: "ROAD_BASE_M3", description: "Road Base / Hard Landscaping Base", descriptionAr: "قاعدة طريق", discipline: "STR", section: 1, sectionName: "Substructure", unit: "m³", formula: "Road_Area × Base_Thickness", notes: "Optional item. Only if external hard landscape exists." },

  // ─── SECTION 2: SUPERSTRUCTURE (STR) ───
  { code: "RCC_COLUMNS_M3", description: "RCC Columns (All Floors)", descriptionAr: "أعمدة خرسانة مسلحة", discipline: "STR", section: 2, sectionName: "Superstructure", unit: "m³", formula: "Σ(Col_W × Col_D × Floor_Height) × Count_per_floor", notes: "Measured floor-to-floor. Include all levels." },
  { code: "FIRST_SLAB_M3", description: "First Floor Slab", descriptionAr: "بلاطة الطابق الأول", discipline: "STR", section: 2, sectionName: "Superstructure", unit: "m³", formula: "Slab_Area × Slab_Thickness", notes: "Include cantilevers and balconies in area." },
  { code: "FIRST_SLAB_BEAMS_M3", description: "First Floor Beams", descriptionAr: "جسور الطابق الأول", discipline: "STR", section: 2, sectionName: "Superstructure", unit: "m³", formula: "Σ(Beam_L × Beam_W × (Beam_D - Slab_Thk))", notes: "Beam depth below slab soffit only." },
  { code: "SECOND_SLAB_M3", description: "Roof Slab / Second Floor Slab", descriptionAr: "بلاطة السقف", discipline: "STR", section: 2, sectionName: "Superstructure", unit: "m³", formula: "Slab_Area × Slab_Thickness", notes: "For G+1: this is the roof slab." },
  { code: "SECOND_SLAB_BEAMS_M3", description: "Roof Beams", descriptionAr: "جسور السقف", discipline: "STR", section: 2, sectionName: "Superstructure", unit: "m³", formula: "Σ(Beam_L × Beam_W × (Beam_D - Slab_Thk))", notes: "Same logic as first floor beams." },
  { code: "STAIRS_INTERNAL_M3", description: "Internal Staircase Concrete", descriptionAr: "خرسانة سلم داخلي", discipline: "STR", section: 2, sectionName: "Superstructure", unit: "m³", formula: "Step_Based: 0.2 m³/step. G+1: 24 steps=5.0 m³. G+2: 32 steps=6.4 m³. Rule: >30 steps = G+2.", notes: "Auto-calculated per project type. G+0: 0. G+1: 5.0 m³ (24 steps). G+2: 6.4 m³ (32 steps, 2nd floor = staircase room). 95% accuracy." },
  { code: "BITUMEN_SUBSTRUCTURE_TOTAL_QTY", description: "Bituminous Waterproofing (Substructure)", descriptionAr: "عزل بيتومين تحت الأرض", discipline: "STR", section: 2, sectionName: "Superstructure", unit: "m²", formula: "Perimeter × Foundation_Depth × 2 + Slab_on_Grade_Area", notes: "Two coats applied to all surfaces in contact with soil." },

  // ─── SECTION 3: BLOCKWORK (ARCH) ───
  { code: "BLOCK_EXTERNAL_THERMAL_M2", description: "External Thermal Block (20cm)", descriptionAr: "بلوك حراري خارجي 20سم", discipline: "ARCH", section: 3, sectionName: "Blockwork", unit: "m²", formula: "External_Perimeter × Wall_Height - Opening_Areas", notes: "Thermal block for external walls. Deduct doors & windows > 0.5m²." },
  { code: "BLOCK_INTERNAL_HOLLOW_8_M2", description: "Internal Hollow Block (20cm)", descriptionAr: "بلوك مفرغ داخلي 20سم", discipline: "ARCH", section: 3, sectionName: "Blockwork", unit: "m²", formula: "Internal_Wall_Length × Wall_Height - Opening_Areas", notes: "Partition walls between rooms. Deduct doors > 0.5m²." },
  { code: "BLOCK_INTERNAL_HOLLOW_6_M2", description: "Internal Hollow Block (10cm)", descriptionAr: "بلوك مفرغ داخلي 10سم", discipline: "ARCH", section: 3, sectionName: "Blockwork", unit: "m²", formula: "Partition_Wall_Length × Wall_Height", notes: "For bathrooms, kitchens, thin partitions." },
  { code: "SOLID_BLOCK_WORK_M2", description: "Solid Block / Strap Beam Block", descriptionAr: "بلوك صلب / بلوك جسور", discipline: "ARCH", section: 3, sectionName: "Blockwork", unit: "m²", formula: "Strap_Beam_Length × Beam_Height", notes: "Used for strap beam infill or boundary walls." },

  // ─── SECTION 4: FINISHING (FINISH) ───
  { code: "PLASTER_INTERNAL_M2", description: "Internal Plastering", descriptionAr: "بياض داخلي", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "(Internal_Wall_Area × 2_sides) + Ceiling_Area - Opening_Areas", notes: "Both sides of internal walls. Deduct openings > 0.5m²." },
  { code: "PLASTER_EXTERNAL_M2", description: "External Plastering / Render", descriptionAr: "بياض خارجي", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "External_Perimeter × Wall_Height - Opening_Areas", notes: "External face only. Include parapet walls." },
  { code: "PAINT_INTERNAL_M2", description: "Internal Paint", descriptionAr: "دهان داخلي", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Internal_Plaster_Area (dry areas only)", notes: "Same as plaster area minus tiled areas (wet areas)." },
  { code: "PAINT_EXTERNAL_M2", description: "External Paint / Texture Coating", descriptionAr: "دهان خارجي", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "External_Plaster_Area", notes: "Same area as external plaster." },
  { code: "WALL_TILES_WET_AREAS_M2", description: "Wall Tiles (Wet Areas)", descriptionAr: "بلاط جدران مناطق رطبة", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Σ(Room_Perimeter × Tile_Height) per wet room", notes: "Bathrooms, kitchen, laundry. Tile height typically 2.4m or full height." },
  { code: "DRY_AREA_FLOORING_M2", description: "Dry Area Flooring", descriptionAr: "أرضيات مناطق جافة", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Σ(Room_Area) for dry rooms", notes: "Living, bedrooms, corridors. Marble/porcelain/ceramic." },
  { code: "WET_AREA_FLOORING_M2", description: "Wet Area Flooring", descriptionAr: "أرضيات مناطق رطبة", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Σ(Room_Area) for wet rooms", notes: "Bathrooms, kitchen, laundry. Anti-slip ceramic." },
  { code: "BALCONY_FLOORING_M2", description: "Balcony / Terrace Flooring", descriptionAr: "أرضيات بلكونات", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Σ(Balcony_Area)", notes: "Outdoor porcelain or natural stone." },
  { code: "SKIRTING_LM", description: "Skirting", descriptionAr: "وزرة", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "RM", formula: "Σ(Room_Perimeter) for dry rooms - Door_Widths", notes: "Running metre. Deduct door openings." },
  { code: "MARBLE_THRESHOLD_LM", description: "Marble Threshold", descriptionAr: "عتبة رخام", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "RM", formula: "Count_of_Doors × Door_Width", notes: "At door openings between different floor finishes." },
  { code: "WET_AREAS_BALCONY_WATERPROOF_M2", description: "Waterproofing (Wet Areas & Balcony)", descriptionAr: "عزل مائي مناطق رطبة وبلكونات", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Σ(Wet_Room_Floor_Area + Balcony_Area) + Wall_Upturn_300mm", notes: "Floor + 300mm wall upturn. Two-layer membrane." },
  { code: "ROOF_WATERPROOF_M2", description: "Roof Waterproofing System", descriptionAr: "عزل سقف", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Roof_Slab_Area + Parapet_Upturn", notes: "Combo system: insulation + membrane + screed + tiles." },
  { code: "CEILING_SPRAY_PLASTER_M2", description: "Ceiling Finish (Spray Plaster / Gypsum)", descriptionAr: "تشطيب سقف", discipline: "FINISH", section: 4, sectionName: "Finishing", unit: "m²", formula: "Σ(Room_Area) all rooms", notes: "Spray plaster for dry areas, moisture-resistant for wet areas." },

  // ─── SECTION 5: EXTERNAL WORKS & ADDITIONAL (ARCH+FINISH) ───
  { code: "PARAPET_WALL_M2", description: "Parapet Wall (Roof Boundary)", descriptionAr: "جدار حاجز سطح", discipline: "ARCH", section: 5, sectionName: "External Works", unit: "m²", formula: "Roof_Perimeter × Parapet_Height (0.9-1.2m typical)", notes: "Block + plaster both sides. Height per municipality code. UAE typical 1.0m." },
  { code: "COPING_STONES_LM", description: "Coping Stones (Parapet Top)", descriptionAr: "حجر إفريز أعلى الجدار", discipline: "FINISH", section: 5, sectionName: "External Works", unit: "RM", formula: "Roof_Perimeter", notes: "Pre-cast concrete or natural stone coping. Same length as parapet perimeter." },
  { code: "ROOF_THERMAL_INSULATION_M2", description: "Roof Thermal Insulation", descriptionAr: "عزل حراري للسقف", discipline: "FINISH", section: 5, sectionName: "External Works", unit: "m²", formula: "Roof_Slab_Area", notes: "EPS/XPS board 50mm typical. Applied over waterproofing membrane. UAE energy code mandatory." },
  { code: "INTERLOCK_PAVING_M2", description: "Interlock Paving (External)", descriptionAr: "بلاط متداخل خارجي", discipline: "FINISH", section: 5, sectionName: "External Works", unit: "m²", formula: "Plot_Area - Building_Footprint - Landscaped_Area", notes: "Interlocking concrete pavers for driveways, parking, pathways. 60-80mm thickness." },
  { code: "KERB_STONES_LM", description: "Kerb Stones", descriptionAr: "حجر رصيف", discipline: "FINISH", section: 5, sectionName: "External Works", unit: "RM", formula: "Perimeter_of_Paved_Areas + Internal_Edging", notes: "Pre-cast concrete kerbs. Separates paved areas from landscaping." },
  { code: "BOUNDARY_WALL_LM", description: "Boundary Wall (Compound Wall)", descriptionAr: "سور خارجي", discipline: "ARCH", section: 5, sectionName: "External Works", unit: "RM", formula: "Plot_Perimeter - Gate_Width", notes: "Hollow block 20cm, plastered both sides, 2.0-2.4m height. UAE villa standard." },
  { code: "FALSE_CEILING_M2", description: "False Ceiling (Gypsum Board)", descriptionAr: "سقف مستعار جبس بورد", discipline: "FINISH", section: 5, sectionName: "External Works", unit: "m²", formula: "Σ(Room_Area) for rooms with false ceiling", notes: "12.5mm gypsum board on metal frame. Common in living, dining, corridors." },
];

export function loadEquationSheetBible(): EquationBible {
  return {
    fileName: "UAE_MASTER_V15_BIBLE.xlsx",
    sheetName: "MASTER_QUANTITIES",
    updatedAt: new Date().toISOString(),
    version: "1.5.0",
    equations: MASTER_EQUATIONS,
  };
}

/** Lookup a single equation by item code */
export function getEquation(code: string): EquationDef | undefined {
  return MASTER_EQUATIONS.find(eq => eq.code === code);
}

/** Get all equations for a discipline */
export function getEquationsByDiscipline(discipline: "STR" | "ARCH" | "FINISH"): EquationDef[] {
  return MASTER_EQUATIONS.filter(eq => eq.discipline === discipline);
}

/** Get all equations for a section number */
export function getEquationsBySection(section: number): EquationDef[] {
  return MASTER_EQUATIONS.filter(eq => eq.section === section);
}
