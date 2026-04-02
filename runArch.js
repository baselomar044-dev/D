const fs = require("fs");
const path = require("path");
const DxfParser = require("dxf-parser");
const XLSX = require("xlsx");
const Papa = require("papaparse");
const {
	MIN_ACCURACY_PCT,
	TARGET_ACCURACY_PCT,
	HONEST_MODE,
	toAccuracyPct,
	passesAccuracyGate,
	meetsTargetAccuracy,
	accuracyBand
} = require("./accuracyPolicy");
const { loadRules } = require("./rules");
const {
	detectDisciplineSignature,
	buildRequiredQuestionsFromRules,
	buildItemQuality,
	finalizeReleaseGate
} = require("./quality");

const {
	collectTextsFromEntities,
	flattenDxfEntities,
	collectInsertReferences,
	getCadLengthUnit,
	unitScaleToMeters,
	scaleEntitiesToMeters,
	scaleInsertRefsToMeters,
	collectEntityPoints,
	computeBoundsFromPoints,
	applyScopeRectToEntities,
	applyScopeRectToInsertRefs,
	applyScopeCircleToEntities,
	applyScopeCircleToInsertRefs
} = require("./dxfRuntimeUtils");
const {
	pairWalls: pairWallsShared,
	isLikelyWallLayer
} = require("./wallRuntimeUtils");

function convertDim(v, unit){
	if(unit==="mm") return v/1000;
	if(unit==="cm") return v/100;
	if(unit==="m") return v;
	return null;
}

function normalizeUnitToken(unit){
	const raw=String(unit||"")
		.toLowerCase()
		.replace(/â/g,"")
		.replace(/²/g,"2")
		.replace(/³/g,"3")
		.replace(/\s+/g,"");
	if(/m.*3/.test(raw)) return "m3";
	if(/m.*2/.test(raw)) return "m2";
	if(/^(?:m|rm|r\.m\.?)$/.test(raw)) return "m";
	if(/l.*s/.test(raw)) return "ls";
	return raw;
}

function scopeKeyFromPlanLayer(layer){
	const value=String(layer||"").toUpperCase();
	if(/ARC_PL_\d+-GR\b/.test(value)) return "GROUND";
	if(/ARC_PL_\d+-F1\b/.test(value)) return "FIRST";
	if(/ARC_PL_\d+-RO\b/.test(value)) return "ROOF";
	if(/ARC_PL_\d+-(TR|TOP)\b/.test(value)) return "TOP_ROOF";
	return null;
}

function roundN(v, n=3){
	const p=Math.pow(10,n);
	return Math.round((Number(v)||0)*p)/p;
}

function segmentKey(s){
	const x1=roundN(Math.min(s.x1,s.x2),3);
	const y1=roundN(Math.min(s.y1,s.y2),3);
	const x2=roundN(Math.max(s.x1,s.x2),3);
	const y2=roundN(Math.max(s.y1,s.y2),3);
	return `${s.ori}|${x1},${y1}|${x2},${y2}|${String(s.layer||"").toUpperCase()}`;
}

function dedupeSegments(segments){
	const seen=new Set();
	const out=[];
	for(const s of (segments||[])){
		const k=segmentKey(s);
		if(seen.has(k)) continue;
		seen.add(k);
		out.push(s);
	}
	return out;
}

function isScopedFloorPlanWallCandidateLayer(layer){
	const value=String(layer||"");
	if(value==="0") return true;
	if(!/FloorPlan-ARC_PL_[12]-(GR|F1)\$0\$/i.test(value)) return false;
	return /(Wall|Hidden|WAL-CMU|A-WALL)/i.test(value);
}

function isBroadInternalCandidateLayer(layer){
	const value=String(layer||"");
	if(value==="0") return true;
	if(!/FloorPlan-ARC_PL_[12]-(GR|F1)\$0\$/i.test(value)) return false;
	return /(Wall|Hidden|Door|Furniture|WAL-CMU|A-WALL)/i.test(value);
}

function parseMetricLevel(text){
	const match=String(text||"").match(/([+-]?\d+(?:\.\d+)?)\s*m\b/i);
	return match ? Number(match[1]) : null;
}

function deriveParapetHeightFromTexts(texts){
	const values=[...new Set((texts||[])
		.map((row)=>parseMetricLevel(stripCadMarkup(row.text)))
		.filter((v)=>Number.isFinite(v) && v>=0.5 && v<=20)
		.map((v)=>roundN(v,2))
	)]
	.sort((a,b)=>b-a);
	for(let i=0;i<values.length;i++){
		for(let j=i+1;j<values.length;j++){
			const diff=values[i]-values[j];
			if(diff>=1.3 && diff<=2.2) return diff;
		}
	}
	return 1.65;
}

function deriveRoofPlanMetrics(segments, planScopes){
	const roofScope=planScopes?.ROOF || planScopes?.TOP_ROOF || null;
	if(!roofScope) return { roof_edge_length_m:0, bbox_area_m2:0, estimated_area_m2:0 };
	const roofSegs=(segments||[]).filter((seg)=>{
		if(seg.ori!=="H" && seg.ori!=="V") return false;
		const layer=String(seg.layer||"");
		if(!/FloorPlan-ARC_PL_3-RO\$0\$(ELEV PEN1|ELEV PEN5|Hidden)/i.test(layer)) return false;
		if(seg.ori==="H") return seg.c>=roofScope.y1 && seg.c<=roofScope.y2 && seg.a<=roofScope.x2 && seg.b>=roofScope.x1;
		return seg.c>=roofScope.x1 && seg.c<=roofScope.x2 && seg.a<=roofScope.y2 && seg.b>=roofScope.y1;
	});
	if(!roofSegs.length) return { roof_edge_length_m:0, bbox_area_m2:0, estimated_area_m2:0 };
	const roofEdgeLengthM=roofSegs
		.filter((seg)=>/ELEV PEN1/i.test(String(seg.layer||"")))
		.reduce((sum, seg)=>sum+Number(seg.len||0),0);
	const pts=roofSegs.flatMap((seg)=>seg.ori==="H"
		? [{ x:seg.a, y:seg.c }, { x:seg.b, y:seg.c }]
		: [{ x:seg.c, y:seg.a }, { x:seg.c, y:seg.b }]
	);
	const bounds=computeBoundsFromPoints(pts);
	const bboxArea=bounds ? (bounds.x2-bounds.x1) * (bounds.y2-bounds.y1) : 0;
	return {
		roof_edge_length_m:roofEdgeLengthM,
		bbox_area_m2:bboxArea,
		estimated_area_m2:bboxArea>0 ? bboxArea*0.75 : 0
	};
}

function extractSegments(entities){
	const segs=[];
	let id=1;
	const pushSeg=(x1,y1,x2,y2,layer)=>{
		if(![x1,y1,x2,y2].every(v=>typeof v==="number" && isFinite(v))) return;
		const dx=x2-x1;
		const dy=y2-y1;
		const len=Math.hypot(dx,dy);
		if(len<0.2) return;
		let ori="D";
		let c=null;
		let a=null;
		let b=null;
		let sx1=x1;
		let sy1=y1;
		let sx2=x2;
		let sy2=y2;
		if(Math.abs(dy)<=0.02){
			const minX=Math.min(x1,x2);
			const maxX=Math.max(x1,x2);
			ori="H";
			c=(y1+y2)/2;
			a=minX;
			b=maxX;
			sx1=minX;
			sy1=c;
			sx2=maxX;
			sy2=c;
		}else if(Math.abs(dx)<=0.02){
			const minY=Math.min(y1,y2);
			const maxY=Math.max(y1,y2);
			ori="V";
			c=(x1+x2)/2;
			a=minY;
			b=maxY;
			sx1=c;
			sy1=minY;
			sx2=c;
			sy2=maxY;
		}
		segs.push({
			id:id++,
			ori,
			c,
			a,
			b,
			layer:layer||"",
			x1:sx1,
			y1:sy1,
			x2:sx2,
			y2:sy2,
			len
		});
	};

	for(const e of (entities||[])){
		if(e.type==="LINE"){
			const p1=e.vertices?.[0]||e.startPoint;
			const p2=e.vertices?.[1]||e.endPoint;
			if(p1&&p2) pushSeg(p1.x,p1.y,p2.x,p2.y,e.layer);
			continue;
		}
		if((e.type==="LWPOLYLINE" || e.type==="POLYLINE") && Array.isArray(e.vertices) && e.vertices.length>=2){
			for(let i=1;i<e.vertices.length;i++){
				const a=e.vertices[i-1];
				const b=e.vertices[i];
				pushSeg(a.x,a.y,b.x,b.y,e.layer);
			}
			if(e.shape || e.closed){
				const a=e.vertices[e.vertices.length-1];
				const b=e.vertices[0];
				pushSeg(a.x,a.y,b.x,b.y,e.layer);
			}
		}
	}
	return segs;
}

function pairWalls(segments){
	return pairWallsShared(segments, { useAxisAlignment:true });
}

function summarizePairThicknessCoverage(pairs){
	const bins=new Set();
	for(const pair of (pairs||[])){
		if(!(pair?.overlap>0)) continue;
		bins.add(Number(pair.thickness).toFixed(2));
	}
	return bins.size;
}

function chooseBestWallEvidenceSet(candidates){
	const scored=[];
	for(const candidate of (candidates||[])){
		const segments=Array.isArray(candidate?.segments) ? candidate.segments : [];
		if(segments.length<20) continue;
		const pairs=pairWalls(segments);
		const thicknessCoverage=summarizePairThicknessCoverage(pairs);
		const pairDensity=pairs.length/Math.max(segments.length,1);
		const noisePenalty=Math.max(0, segments.length-5000)*0.01;
		const score=(pairDensity*1000) + (Math.min(pairs.length, 1200)*0.2) + (thicknessCoverage*10) - noisePenalty;
		scored.push({
			name:candidate.name||"UNNAMED",
			segments,
			pairs,
			score,
			thicknessCoverage
		});
	}
	const byName=Object.fromEntries(scored.map((entry)=>[entry.name, entry]));
	const scoped=byName.SCOPED_PLAN;
	const wall=byName.WALL_LAYER;
	const all=byName.ALL_SEGMENTS;
	if(
		scoped &&
		scoped.pairs.length>=20 &&
		scoped.thicknessCoverage>=2 &&
		(!wall || scoped.pairs.length>=Math.max(12, wall.pairs.length*0.65))
	){
		return scoped;
	}
	if(wall && wall.pairs.length>=12 && wall.thicknessCoverage>=2){
		return wall;
	}
	if(scoped && scoped.pairs.length>=12 && scoped.thicknessCoverage>=2){
		return scoped;
	}
	if(all && all.pairs.length>=12 && all.thicknessCoverage>=2){
		return all;
	}
	return scored.sort((a,b)=>b.score-a.score)[0] || {
		name:"EMPTY",
		segments:[],
		pairs:[],
		score:-1,
		thicknessCoverage:0
	};
}

function findBoqQty(workbook, keywords){
	let best=null;
	for(const sheet of (workbook.SheetNames||[])){
		const ws=workbook.Sheets[sheet];
		const rows=XLSX.utils.sheet_to_json(ws,{ header:1, blankrows:false, defval:"" });
		for(let r=0;r<rows.length;r++){
			const row=rows[r]||[];
			const txt=row.map(v=>String(v||"")).join(" ").toLowerCase();
			if(!matchesAnyPattern(txt, keywords)) continue;
			for(let c=0;c<row.length;c++){
				if(typeof row[c]!=="number") continue;
				if(!best){
					best={ sheet, row:r+1, qty_cell:XLSX.utils.encode_cell({r,c}), qty_value:row[c], row_text:txt };
				}
			}
		}
	}
	return best || { sheet:null, row:null, qty_cell:null, qty_value:null, row_text:null };
}

function scoreBoqQtyCandidate(value, colIndex, row){
	const n=Number(value);
	if(!Number.isFinite(n) || n<=0) return -9999;
	let score=0;
	if(Number.isInteger(n)) score+=1;
	if(n>=0.5 && n<=200000) score+=2;
	const prev=String(row[colIndex-1]||"").toLowerCase();
	const next=String(row[colIndex+1]||"").toLowerCase();
	if(/qty|quantity/.test(prev) || /qty|quantity/.test(next)) score+=4;
	const nextNum=Number(row[colIndex+1]);
	if(Number.isFinite(nextNum) && nextNum>n*3) score-=1;
	if(n<0.5) score-=2;
	return score;
}

function extractAllBoqItems(workbook){
	const out=[];
	for(const sheet of (workbook.SheetNames||[])){
		const ws=workbook.Sheets[sheet];
		const rows=XLSX.utils.sheet_to_json(ws,{ header:1, blankrows:false, defval:"" });
		for(let r=0;r<rows.length;r++){
			const row=rows[r]||[];
			const rowText=row.map(v=>String(v||"")).join(" ").replace(/\s+/g," ").trim();
			if(!rowText) continue;
			if(/(?:subtotal|\btotal\b)/i.test(rowText.toLowerCase())) continue;

			let unitCol=-1;
			let bestCol=-1;
			let bestScore=-9999;
			for(let c=0;c<row.length;c++){
				if(typeof row[c]!=="number") continue;
				let score=scoreBoqQtyCandidate(row[c], c, row);
				if(unitCol>=0 && c>unitCol){
					score += 5;
					if(c===unitCol+1) score += 4;
				}
				if(score>bestScore){
					bestScore=score;
					bestCol=c;
				}
			}
			if(bestCol<0) continue;

			const qty=Number(row[bestCol]);
			if(!Number.isFinite(qty) || qty<=0) continue;

			for(let c=0;c<row.length;c++){
				if(typeof row[c]!=="string") continue;
				const unitNorm=normalizeUnitToken(row[c]);
				if(!/^(m3|m2|m|ls)$/.test(unitNorm)) continue;
				if(c<=bestCol){
					if(unitCol<0 || c>unitCol) unitCol=c;
				}else if(unitCol<0){
					unitCol=c;
				}
			}

			const textCells=row.map(v=>String(v||"").trim()).filter(s=>s && /[A-Za-z\u0600-\u06FF]/.test(s));
			const description=textCells.sort((a,b)=>b.length-a.length)[0] || "";
			if(description.length<4) continue;

			const itemNoRaw=(()=>{
				for(let c=0;c<Math.min(row.length,4);c++){
					const v=row[c];
					if(typeof v==="number" && v>0 && v<1000) return v;
				}
				return "";
			})();
			const unitCell=unitCol>=0 ? row[unitCol] : row.find(v=>typeof v==="string" && /^(m3|m2|m|ls)$/.test(normalizeUnitToken(v)));
			out.push({
				sheet,
				row:r+1,
				item_no:itemNoRaw!=null?String(itemNoRaw):"",
				description,
				unit:normalizeUnitToken(unitCell),
				qty,
				qty_cell:XLSX.utils.encode_cell({ r, c:bestCol }),
				row_text:rowText
			});
		}
	}
	return out.sort((a,b)=>{
		const sheetA=String(a.sheet||"");
		const sheetB=String(b.sheet||"");
		if(sheetA<sheetB) return -1;
		if(sheetA>sheetB) return 1;
		return a.row-b.row;
	});
}

function mapBoqItemToArchKey(description, unit=""){
	const d=String(description||"").toLowerCase();
	const u=normalizeUnitToken(unit);
	const isBlock=/block|brick|masonry|طابوق|بلوك|بلوكات|مبان/i.test(d);
	const isSolid=/solid|مصمت/i.test(d);
	const isThermal=/thermal|عازل/i.test(d);
	const isHollow=/hollow|خالي/i.test(d);
	const isPlaster=/plaster|لياسة|بلاستر/i.test(d);
	const isInternal=/internal|داخل/i.test(d);
	const isExternal=/external|خارجي/i.test(d);
	const isParapet=/parapet|برابيت/i.test(d);

	if(isBlock && (isSolid || /decor|decore|decorative|ornament|feature/i.test(d))) return null;
	if(isBlock && /m2/i.test(u) && isThermal) return "BLOCK_EXTERNAL_THERMAL_M2";
	if(isBlock && /m2/i.test(u) && isHollow && /(6\s*(?:\"|inch)|150)/.test(d)) return "BLOCK_INTERNAL_HOLLOW_6_M2";
	if(isBlock && /m2/i.test(u) && isHollow && /(8\s*(?:\"|inch)|200)/.test(d)) return "BLOCK_INTERNAL_HOLLOW_8_M2";
	if(isBlock && /m2/i.test(u) && isHollow) return "BLOCK_INTERNAL_HOLLOW_M2";
	if(isBlock && /m2/i.test(u)) return "BLOCKWORK_TOTAL_M2";
	if(isBlock && /m3/i.test(u)) return "BLOCKWORK_TOTAL_M3";
	if(isPlaster && !/m2/i.test(u)) return null;
	if(isPlaster && isParapet) return "PLASTER_PARAPET_INTERNAL_M2";
	if(isPlaster && isInternal) return "PLASTER_INTERNAL_NET_M2";
	if(isPlaster && isExternal) return "PLASTER_EXTERNAL_NET_M2";
	return null;
}

function collectDimensionMeasurements(entities, unit){
	const dims=[];
	for(const entity of (entities||[])){
		if(entity.type!=="DIMENSION") continue;
		const p=entity.anchorPoint || entity.middleOfText || entity.position || entity.startPoint || null;
		const raw=Number(entity.actualMeasurement ?? entity.measurement ?? entity.dimensionText ?? entity.text);
		if(!Number.isFinite(raw)) continue;
		const value_m=normalizeOpeningScalarM(raw, unit, "height");
		if(value_m===null || !(value_m>0) || value_m>10) continue;
		dims.push({ value_m, x:p?.x ?? null, y:p?.y ?? null, layer:entity.layer||"", source:"DIMENSION" });
	}
	return dims;
}

function normalizeOpeningTag(rawTag){
	const raw=String(rawTag||"")
		.toUpperCase()
		.replace(/[\u0000]/g," ")
		.replace(/[\s._-]+/g,"")
		.replace(/[()]/g,"");
	const m=raw.match(/^(DOOR|WINDOWS?|DO|WO|DR|WD|DW|WND|WIN|D|W)(?:TYPE|NO|N)?#?0*(\d{1,3})$/);
	if(!m) return null;
	const prefix=m[1];
	const index=Number(m[2]);
	if(!Number.isFinite(index) || index<=0) return null;
	if(/^(WINDOWS?|W|WO|WIN|WND|WD)$/.test(prefix)) return `W${index}`;
	return `D${index}`;
}

function normalizeOpeningScalarM(rawValue, unit, role="width"){
	const n=Number(rawValue);
	if(!Number.isFinite(n) || !(n>0)) return null;
	const min=role==="height" ? 0.5 : 0.3;
	const max=role==="height" ? 5.0 : 3.5;
	const pref=role==="height" ? 2.1 : 1.1;
	const candidates=[];
	const push=(value, penalty)=>{
		if(!Number.isFinite(value) || !(value>0)) return;
		candidates.push({ value, penalty });
	};
	const cad=convertDim(n, unit);
	if(cad!==null) push(cad, 0.65);
	push(n, 0.9);
	push(n/10, 0.8);
	push(n/100, 0.35);
	push(n/1000, 0.2);

	const uniq=new Map();
	for(const row of candidates){
		const key=roundN(row.value, 4);
		if(!uniq.has(key) || row.penalty<uniq.get(key).penalty){
			uniq.set(key, { value:key, penalty:row.penalty });
		}
	}
	const valid=[...uniq.values()].filter((row)=>row.value>=min && row.value<=max);
	if(!valid.length) return null;
	valid.sort((a,b)=>{
		const sa=a.penalty + Math.abs(a.value-pref)*0.12;
		const sb=b.penalty + Math.abs(b.value-pref)*0.12;
		return sa-sb;
	});
	return valid[0].value;
}

function normalizeOpeningDimPair(rawA, rawB, unit){
	let w=normalizeOpeningScalarM(rawA, unit, "width");
	let h=normalizeOpeningScalarM(rawB, unit, "height");
	if(w===null || h===null){
		w=normalizeOpeningScalarM(rawA, unit, "height");
		h=normalizeOpeningScalarM(rawB, unit, "width");
	}
	if(w===null || h===null) return null;
	if(h<w){
		const tmp=w;
		w=h;
		h=tmp;
	}
	if(w<0.3 || w>6.0 || h<0.5 || h>12.0) return null;
	return { w, h };
}

function parseOpeningDimPairsFromText(text, unit){
	const clean=stripCadMarkup(text).replace(/[,;]+/g, " ");
	const out=[];
	const re=/(\d{1,4}(?:\.\d+)?)\s*(?:[xX×*\/])\s*(\d{1,4}(?:\.\d+)?)/g;
	let m=null;
	while((m=re.exec(clean))!==null){
		const pair=normalizeOpeningDimPair(Number(m[1]), Number(m[2]), unit);
		if(pair) out.push(pair);
	}
	return out;
}

function parseExplicitOpeningSizeText(text, unit){
	const clean=stripCadMarkup(text).replace(/[,;]+/g, " ").toUpperCase();
	const match=clean.match(/(\d{1,4}(?:\.\d+)?)\s*W\s*[X*]\s*(\d{1,4}(?:\.\d+)?)\s*H/);
	if(!match) return null;
	const w=Number(match[1]);
	const h=Number(match[2]);
	if(!Number.isFinite(w) || !Number.isFinite(h)) return null;
	if(w<0.3 || w>6.0 || h<0.5 || h>12.0) return null;
	return { w, h };
}

function deriveOpeningRowsFromInsertFamilies(insertRefs, unit, planScopes, wallBounds){
	const hasReliablePlanScopes=Object.keys(planScopes||{}).length>=2;
	const floorHintRe=/(ground|1st|first|second|floor|lvl|plan|arc_pl|roof)/i;
	const rejectRe=/(elev|elevation|section|detail|sheet|layout|title|legend|schedule|table|viewport)/i;
	const seen=new Set();
	const groups=new Map();
	const pushGroup=(kind, pair)=>{
		const w=roundN(Number(pair?.w)||0,4);
		const h=roundN(Number(pair?.h)||0,4);
		if(!(w>0 && h>0)) return;
		const k=`${kind}|${w}|${h}`;
		const row=groups.get(k) || { kind, w, h, no:0 };
		row.no+=1;
		groups.set(k, row);
	};

	for(const ref of (insertRefs||[])){
		const nameClean=stripCadMarkup(ref.name||"");
		const layer=String(ref.layer||"");
		const combined=`${layer} ${nameClean}`;
		if(!nameClean) continue;
		if(rejectRe.test(combined)) continue;
		const kind=classifyOpeningKindFromText(combined);
		if(!kind) continue;
		const pairs=parseOpeningDimPairsFromText(combined, unit);
		if(!pairs.length) continue;
		const inScope=pointInAnyScope(ref.x, ref.y, planScopes);
		const inWallBounds=pointInBounds(ref.x, ref.y, wallBounds, 8);
		const floorHint=floorHintRe.test(combined);
		if(!(inScope || inWallBounds || floorHint)) continue;
		if(hasReliablePlanScopes && !inScope && !floorHint) continue;
		const pair=pairs[0];
		const dk=`${kind}|${roundN(ref.x,2)}|${roundN(ref.y,2)}|${roundN(pair.w,3)}|${roundN(pair.h,3)}`;
		if(seen.has(dk)) continue;
		seen.add(dk);
		pushGroup(kind, pair);
	}

	const rows=[...groups.values()].map((row)=>({
		tag:`${row.kind}_INS_${Math.round(row.w*1000)}x${Math.round(row.h*1000)}`,
		count:row.no,
		width_m:row.w,
		height_m:row.h,
		area_m2:roundN(row.w*row.h*row.no, 4),
		has_dims:true,
		source:"INSERT_FAMILY_DIM"
	}));
	rows.sort((a,b)=>{
		if(a.tag<b.tag) return -1;
		if(a.tag>b.tag) return 1;
		return 0;
	});
	return rows;
}

function normalizeOpeningBaseName(name){
	return stripCadMarkup(name)
		.replace(/-GROUND FLOOR PLAN_.*/i, "")
		.replace(/-FIRST FLOOR PLAN_.*/i, "")
		.replace(/\s+COPY\s+\d+$/i, "")
		.trim();
}

function classifyOpeningFamilyFromText(text){
	const value=String(text||"").toUpperCase();
	if(/AR_WIN_CWP_DOUBLE_FRAMED_SLIDING/.test(value)) return "WIN_SLIDING";
	if(/AR_WIN_CWP_SINGLE_FRAMED_SWING|AR_WIN_CWP_SINGLE_FRAMED_SWING _ TYPE/.test(value)) return "WIN_SWING";
	if(/AR_DOR_CWP_DOUBLE_FRAMED_SLIDING_DOOR|DBL GLASS/.test(value)) return "DOOR_SLIDING";
	if(/AR_DOR_WITH ARCHITRAVE|DOOR B/.test(value)) return "DOOR_SWING";
	if(/DOOR\/WINDOW|M_TALL CABINET/.test(value)) return "DOOR_WINDOW";
	if(/AR_DOR_BARN SLIDING/.test(value)) return "DOOR_BARN";
	return null;
}

function computeEntityBounds(entityList){
	const points=(entityList||[]).flatMap((entity)=>collectEntityPoints(entity));
	return computeBoundsFromPoints(points);
}

function collectPlanOpeningGroups(doc, scaleFactor, planScopes={}){
	const groups=new Map();
	const topLevel=Array.isArray(doc?.entities) ? doc.entities : [];
	const hasReliablePlanScopes=Object.keys(planScopes||{}).length>=2;
	const baseBlockPathRe=/^B\d{2}[^>]*>/i;

	const pushGroup=(row)=>{
		const key=[
			row.kind,
			row.family||"",
			row.base_name||"",
			roundN(row.width_m, 2)
		].join("|");
		const current=groups.get(key) || {
			kind:row.kind,
			family:row.family||"",
			base_name:row.base_name||"",
			width_m:roundN(row.width_m, 4),
			thickness_m:roundN(row.thickness_m, 4),
			count:0
		};
		current.count += 1;
		groups.set(key, current);
	};

	const visit=(insert, depth=0, parentPath="")=>{
		if(!insert || insert.type!=="INSERT" || depth>6) return;
		const name=String(insert.name||"");
		const combined=`${name} ${insert.layer||""}`;
		const pathToken=parentPath ? `${parentPath}>${name}` : name;
		const isPlanPath=baseBlockPathRe.test(pathToken);
		if(isPlanPath){
			const kind=classifyOpeningKindFromText(combined);
			if(kind){
				const xM=(Number(insert.position?.x)||0) * scaleFactor;
				const yM=(Number(insert.position?.y)||0) * scaleFactor;
				if(!hasReliablePlanScopes || pointInAnyScope(xM, yM, planScopes)){
					const expanded=expandInsertEntities(doc, insert);
					const bounds=computeEntityBounds(expanded);
					if(bounds){
						let widthM=(Math.max(bounds.x2-bounds.x1, bounds.y2-bounds.y1)) * scaleFactor;
						const thicknessM=(Math.min(bounds.x2-bounds.x1, bounds.y2-bounds.y1)) * scaleFactor;
						const family=classifyOpeningFamilyFromText(combined) || (kind==="W" ? "WIN_GENERIC" : "DOOR_GENERIC");
						if(family==="DOOR_SLIDING" && widthM>=1.8 && widthM<=2.4){
							widthM=(widthM*2)+0.05;
						}
						if(widthM>=0.35 && widthM<=6.5){
							const allowThickSymbol=family==="DOOR_WINDOW";
							if(allowThickSymbol || thicknessM<=0.35){
								pushGroup({
									kind,
									family,
									base_name:normalizeOpeningBaseName(name),
									width_m:widthM,
									thickness_m:thicknessM
								});
							}
						}
					}
				}
			}
		}

		const block=doc?.blocks?.[insert.name];
		if(!block || !Array.isArray(block.entities) || !block.entities.length) return;
		for(const entity of block.entities){
			if(entity.type!=="INSERT") continue;
			const child=cloneEntity(entity);
			child.position=transformPoint(entity.position||{ x:0, y:0, z:0 }, insert);
			child.xScale=(entity.xScale==null?1:Number(entity.xScale)||1) * (insert.xScale==null?1:Number(insert.xScale)||1);
			child.yScale=(entity.yScale==null?1:Number(entity.yScale)||1) * (insert.yScale==null?1:Number(insert.yScale)||1);
			child.rotation=(Number(entity.rotation)||0) + (Number(insert.rotation)||0);
			visit(child, depth+1, pathToken);
		}
	};

	for(const entity of topLevel){
		if(entity.type!=="INSERT") continue;
		visit(entity);
	}

	return [...groups.values()].sort((a,b)=>{
		if(a.kind!==b.kind) return a.kind.localeCompare(b.kind);
		if(a.width_m!==b.width_m) return b.width_m-a.width_m;
		return a.base_name.localeCompare(b.base_name);
	});
}

function extractOpeningScheduleMap(texts, entities, unit){
	const map={};
	const upsert=(tag, pair, source)=>{
		if(!tag || !pair) return;
		if(!/^[WD]\d{1,3}$/i.test(tag)) return;
		const key=String(tag).toUpperCase();
		const current=map[key];
		const next={ w:roundN(pair.w,4), h:roundN(pair.h,4), source };
		if(!current){
			map[key]=next;
			return;
		}
		const currentArea=(current.w||0)*(current.h||0);
		const nextArea=(next.w||0)*(next.h||0);
		if(nextArea>0 && currentArea<=0) map[key]=next;
	};

	const tagPoints=[];
	const scheduleLabelPoints=[];
	const dimPoints=[];
	const sizeTextPoints=[];
	for(const row of (texts||[])){
		const clean=stripCadMarkup(row.text);
		if(!clean) continue;
		const upper=clean.toUpperCase();
		const tags=extractOpeningTagsFromText(clean).map((tag)=>String(tag||"").toUpperCase()).filter((tag)=>/^[WD]\d{1,3}$/.test(tag));
		const pairs=parseOpeningDimPairsFromText(clean, unit);
		const explicitSizePair=parseExplicitOpeningSizeText(clean, unit);
		if(tags.length && pairs.length){
			for(const tag of tags) upsert(tag, pairs[0], "SCHEDULE_TEXT_INLINE");
		}
		if(explicitSizePair){
			sizeTextPoints.push({ pair:explicitSizePair, x:row.x, y:row.y, text:clean });
		}
		if(tags.length){
			for(const tag of tags) tagPoints.push({ tag, x:row.x, y:row.y });
		}
		if(pairs.length){
			dimPoints.push({ pair:pairs[0], x:row.x, y:row.y });
		}
		const wLabel=upper.match(/\bALUM\.?\s*WINDOW\s*0*(\d{1,3})\b/);
		if(wLabel){
			scheduleLabelPoints.push({ tag:`W${Number(wLabel[1])}`, kind:"W", x:row.x, y:row.y, text:clean });
		}
		const dLabel=upper.match(/\b(?:SLIDING\s*DOOR|WOODEN\s*DOOR|WO0DEN\s*DOOR|DOOR\/WINDOW)\s*[-\s]*0*(\d{1,3})\b/);
		if(dLabel){
			const idx=Number(dLabel[1]);
			let fam=3;
			if(/\b(?:WOODEN\s*DOOR|WO0DEN\s*DOOR)\b/.test(upper)) fam=1;
			else if(/\bSLIDING\s*DOOR\b/.test(upper)) fam=2;
			const canonical=(fam*100)+idx;
			scheduleLabelPoints.push({ tag:`D${canonical}`, kind:"D", x:row.x, y:row.y, text:clean });
		}
	}
	for(const label of scheduleLabelPoints){
		if(map[label.tag]) continue;
		const sizeCandidate=sizeTextPoints
			.filter((row)=>{
				if(!Number.isFinite(row.x) || !Number.isFinite(row.y) || !Number.isFinite(label.x) || !Number.isFinite(label.y)) return false;
				if(row.y>=label.y) return false;
				if((label.y-row.y)>16) return false;
				const xTol=/^W/i.test(label.tag) ? 4.2 : 2.6;
				return Math.abs(row.x-label.x)<=xTol;
			})
			.sort((a,b)=>{
				const sa=Math.abs(a.x-label.x) + Math.abs((label.y-a.y))*0.08;
				const sb=Math.abs(b.x-label.x) + Math.abs((label.y-b.y))*0.08;
				return sa-sb;
			})[0];
		if(sizeCandidate) upsert(label.tag, sizeCandidate.pair, "SCHEDULE_LABEL_SIZE_TEXT");
	}
	const usedDimIdx=new Set();
	for(const pt of tagPoints){
		if(map[pt.tag]) continue;
		let bestIdx=-1;
		let bestScore=Number.POSITIVE_INFINITY;
		for(let i=0;i<dimPoints.length;i+=1){
			if(usedDimIdx.has(i)) continue;
			const dim=dimPoints[i];
			if(!Number.isFinite(dim.x) || !Number.isFinite(dim.y) || !Number.isFinite(pt.x) || !Number.isFinite(pt.y)) continue;
			const dx=Math.abs(dim.x-pt.x);
			const dy=Math.abs(dim.y-pt.y);
			if(dx>18 || dy>3.5) continue;
			const score=dx+(dy*1.2);
			if(score<bestScore){
				bestScore=score;
				bestIdx=i;
			}
		}
		if(bestIdx>=0){
			usedDimIdx.add(bestIdx);
			upsert(pt.tag, dimPoints[bestIdx].pair, "SCHEDULE_TEXT_NEARBY");
		}
	}
	const dimMeasures=collectDimensionMeasurements(entities, unit);
	const pickMode=(values, minV, maxV)=>{
		const bins=new Map();
		for(const v of values){
			const n=Number(v);
			if(!Number.isFinite(n) || n<minV || n>maxV) continue;
			const key=roundN(n,3);
			bins.set(key, (bins.get(key)||0)+1);
		}
		let bestVal=null;
		let bestCnt=-1;
		for(const [key,cnt] of bins.entries()){
			if(cnt>bestCnt || (cnt===bestCnt && key<(bestVal??Number.POSITIVE_INFINITY))){
				bestCnt=cnt;
				bestVal=Number(key);
			}
		}
		return Number.isFinite(bestVal) ? bestVal : null;
	};
	for(const label of scheduleLabelPoints){
		if(map[label.tag]) continue;
		const nearby=dimMeasures.filter((d)=>{
			if(!Number.isFinite(d.x) || !Number.isFinite(d.y) || !Number.isFinite(label.x) || !Number.isFinite(label.y)) return false;
			return Math.abs(d.x-label.x)<=12 && Math.abs(d.y-label.y)<=8;
		});
		const vals=nearby.map((d)=>Number(d.value_m)).filter((v)=>Number.isFinite(v) && v>0.25 && v<6);
		if(!vals.length) continue;
		const width=pickMode(vals, 0.4, 1.8) ?? pickMode(vals, 0.4, 2.5);
		const height=pickMode(vals, 1.6, 3.3) ?? pickMode(vals, 1.0, 3.8);
		if(!(width>0 && height>0)) continue;
		upsert(label.tag, { w:Math.min(width,height), h:Math.max(width,height) }, "SCHEDULE_LABEL_NEAR_DIM");
	}
	return map;
}

function mapInsertRowsToSchedule(insertRows, scheduleMap){
	const scheduleEntries=Object.entries(scheduleMap||{}).map(([tag, dim])=>({
		tag:String(tag||"").toUpperCase(),
		w:Number(dim?.w)||0,
		h:Number(dim?.h)||0
	})).filter((row)=>row.w>0 && row.h>0);
	if(!scheduleEntries.length) return [];
	const aggregate=new Map();
	for(const row of (insertRows||[])){
		const kind=/^W/i.test(String(row.tag||"")) ? "W" : (/^D/i.test(String(row.tag||"")) ? "D" : null);
		if(!kind) continue;
		const iw=Number(row.width_m)||0;
		const ih=Number(row.height_m)||0;
		if(!(iw>0 && ih>0)) continue;
		const cands=scheduleEntries.filter((s)=>s.tag.startsWith(kind));
		if(!cands.length) continue;
		let best=null;
		for(const cand of cands){
			const dw=Math.abs(iw-cand.w)/Math.max(cand.w, 0.2);
			const dh=Math.abs(ih-cand.h)/Math.max(cand.h, 0.2);
			const score=dw+dh;
			if(!best || score<best.score) best={ cand, score };
		}
		if(!best || best.score>0.35) continue;
		const key=best.cand.tag;
		const agg=aggregate.get(key) || {
			tag:key,
			count:0,
			width_m:best.cand.w,
			height_m:best.cand.h,
			area_m2:0,
			has_dims:true,
			source:"SCHEDULE_MAP_FROM_INSERTS"
		};
		const n=Number(row.count)||0;
		agg.count+=n;
		agg.area_m2=roundN((agg.width_m*agg.height_m*agg.count),4);
		aggregate.set(key, agg);
	}
	return [...aggregate.values()].sort((a,b)=>compareOpeningTags(a.tag, b.tag));
}

function derivePlanOpeningTypesBySize(insertRefs, entities, unit, planScopes){
	const hasReliablePlanScopes=Object.keys(planScopes||{}).length>=2;
	const dimMeasures=collectDimensionMeasurements(entities, unit);
	const isPlanPath=(value)=>/GROUND FLOOR PLAN_|FIRST FLOOR PLAN_/i.test(value||"");
	const isRejectPath=(value)=>/SECTION|ELEVATION|DETAIL|SCHEDULE|ROOF|TOP ROOF/i.test(value||"");
	const quantize=(v, step=0.05)=>roundN(Math.round((Number(v)||0)/step)*step,3);
	const seen=new Set();
	const records=[];
	const pickMode=(vals, lo, hi)=>{
		const bins=new Map();
		for(const v of vals){
			const n=Number(v);
			if(!Number.isFinite(n) || n<lo || n>hi) continue;
			const key=roundN(n,3);
			bins.set(key,(bins.get(key)||0)+1);
		}
		let best=null;
		for(const [k,c] of bins.entries()){
			if(!best || c>best.c || (c===best.c && k<best.k)) best={k,c};
		}
		return best ? Number(best.k) : null;
	};

	for(const ref of (insertRefs||[])){
		const pathTxt=String(ref.path||"");
		if(!isPlanPath(pathTxt) || isRejectPath(pathTxt)) continue;
		const combined=`${ref.name||""} ${ref.layer||""} ${pathTxt}`;
		const kind=classifyOpeningKindFromText(combined);
		if(!kind) continue;
		if(hasReliablePlanScopes && !pointInAnyScope(ref.x, ref.y, planScopes)) continue;
		const sk=`${kind}|${roundN(ref.x,2)}|${roundN(ref.y,2)}`;
		if(seen.has(sk)) continue;
		seen.add(sk);
		const nearby=dimMeasures.filter((d)=>{
			if(!Number.isFinite(d.x) || !Number.isFinite(d.y) || !Number.isFinite(ref.x) || !Number.isFinite(ref.y)) return false;
			return Math.abs(d.x-ref.x)<=3.5 && Math.abs(d.y-ref.y)<=3.5;
		});
		if(!nearby.length) continue;
		const vals=nearby.map((d)=>Number(d.value_m)).filter((v)=>Number.isFinite(v) && v>0.3 && v<4.5);
		const width=pickMode(vals, 0.4, 1.8) ?? pickMode(vals, 0.4, 2.4);
		const height=pickMode(vals, 1.6, 3.4) ?? pickMode(vals, 1.0, 3.8);
		if(!(width>0 && height>0)) continue;
		const w=Math.min(width,height);
		const h=Math.max(width,height);
		records.push({ kind, w, h });
	}

	const grouped=new Map();
	for(const row of records){
		const qw=quantize(row.w,0.05);
		const qh=quantize(row.h,0.05);
		const key=`${row.kind}|${qw}|${qh}`;
		const item=grouped.get(key) || { kind:row.kind, w:qw, h:qh, count:0 };
		item.count+=1;
		grouped.set(key,item);
	}
	const byKind={ W:[], D:[] };
	for(const row of grouped.values()){
		byKind[row.kind]?.push(row);
	}
	const toRows=(kind)=>{
		const list=(byKind[kind]||[]).sort((a,b)=>{
			const areaA=a.w*a.h;
			const areaB=b.w*b.h;
			if(areaA!==areaB) return areaA-areaB;
			if(a.w!==b.w) return a.w-b.w;
			return a.h-b.h;
		});
		return list.map((row, idx)=>({
			tag:`${kind}SZ${idx+1}`,
			count:row.count,
			width_m:row.w,
			height_m:row.h,
			area_m2:roundN(row.w*row.h*row.count,4),
			has_dims:true,
			source:"PLAN_SIZE_CLUSTER"
		}));
	};
	return [...toRows("W"), ...toRows("D")];
}

function enrichScheduledCountsFromPlanSize(scheduleRows, planSizeRows, prefix){
	const rows=(scheduleRows||[]).map((row)=>({ ...row }));
	if(!rows.length) return rows;
	const clusters=(planSizeRows||[]).filter((row)=>String(row.tag||"").toUpperCase().startsWith(prefix));
	if(!clusters.length) return rows;
	const q=(v)=>roundN(Math.round((Number(v)||0)/0.05)*0.05,3);

	const schedGroups=new Map();
	for(let i=0;i<rows.length;i+=1){
		const row=rows[i];
		const key=`${q(row.width_m)}|${q(row.height_m)}`;
		const arr=schedGroups.get(key) || [];
		arr.push(i);
		schedGroups.set(key, arr);
	}
	const clusterTotals=new Map();
	for(const row of clusters){
		const key=`${q(row.width_m)}|${q(row.height_m)}`;
		clusterTotals.set(key, (clusterTotals.get(key)||0) + (Number(row.count)||0));
	}

	for(const [key, idxs] of schedGroups.entries()){
		const total=Number(clusterTotals.get(key)||0);
		if(!(total>0)) continue;
		const existing=idxs.reduce((sum,idx)=>sum+(Number(rows[idx].count)||0),0);
		if(existing===total) continue;
		if(existing>0){
			let assigned=0;
			for(const idx of idxs){
				const scaled=Math.max(0, Math.round((Number(rows[idx].count)||0) * (total/Math.max(existing,1))));
				rows[idx].count=scaled;
				assigned+=scaled;
			}
			let delta=total-assigned;
			let pointer=0;
			while(delta!==0 && idxs.length){
				const idx=idxs[pointer%idxs.length];
				if(delta>0){
					rows[idx].count+=1;
					delta-=1;
				}else if(rows[idx].count>0){
					rows[idx].count-=1;
					delta+=1;
				}
				pointer+=1;
				if(pointer>5000) break;
			}
		}else{
			const base=Math.floor(total/idxs.length);
			let rem=total-(base*idxs.length);
			for(const idx of idxs){
				rows[idx].count=base + (rem>0 ? 1 : 0);
				if(rem>0) rem-=1;
			}
		}
	}

	for(const row of rows){
		const key=`${q(row.width_m)}|${q(row.height_m)}`;
		if(!(clusterTotals.get(key)>0) && (Number(row.count)||0)<=1){
			row.count=0;
		}
		row.area_m2=roundN((Number(row.width_m)||0)*(Number(row.height_m)||0)*(Number(row.count)||0),4);
	}
	return rows;
}

function openingScheduleFamilyHint(tag, row){
	const value=String(tag||"").toUpperCase();
	const width=Number(row?.w)||0;
	if(/^D1/.test(value)) return "DOOR_SWING";
	if(/^D2/.test(value)) return "DOOR_SLIDING";
	if(/^D3/.test(value)) return "DOOR_WINDOW";
	if(/^W/.test(value)){
		if(width<=1.35) return "WIN_SMALL";
		return "WIN_LARGE";
	}
	return "";
}

function openingPlanFamilyCompatible(planGroup, scheduleEntry){
	const planFamily=String(planGroup?.family||"").toUpperCase();
	const scheduleFamily=String(scheduleEntry?.family_hint||"").toUpperCase();
	if(String(scheduleEntry?.tag||"").toUpperCase().startsWith("D")){
		if(scheduleFamily==="DOOR_SWING") return planFamily==="DOOR_SWING";
		if(scheduleFamily==="DOOR_SLIDING") return planFamily==="DOOR_SLIDING" || planFamily==="DOOR_BARN";
		if(scheduleFamily==="DOOR_WINDOW") return planFamily==="DOOR_WINDOW";
		return true;
	}
	if(String(scheduleEntry?.tag||"").toUpperCase().startsWith("W")){
		if(planFamily==="WIN_SWING") return scheduleFamily==="WIN_SMALL" || (Number(scheduleEntry?.height_m)||0)>6.0;
		if(planFamily==="WIN_SLIDING") return true;
	}
	return true;
}

function buildScheduledRowsFromPlanGroups(scheduleMap, planGroups){
	const scheduleEntries=Object.entries(scheduleMap||{})
		.map(([tag, dim])=>({
			tag:String(tag||"").toUpperCase(),
			width_m:Number(dim?.w)||0,
			height_m:Number(dim?.h)||0,
			family_hint:openingScheduleFamilyHint(tag, dim)
		}))
		.filter((row)=>row.width_m>0 && row.height_m>0)
		.sort((a,b)=>compareOpeningTags(a.tag, b.tag));
	if(!scheduleEntries.length) return { rows:[], mappingRows:[] };

	const sortedGroups=(planGroups||[])
		.filter((row)=>(Number(row?.count)||0)>0 && (Number(row?.width_m)||0)>0)
		.sort((a,b)=>{
			if(a.kind!==b.kind) return a.kind.localeCompare(b.kind);
			if(a.width_m!==b.width_m) return b.width_m-a.width_m;
			if(a.count!==b.count) return b.count-a.count;
			return String(a.base_name||"").localeCompare(String(b.base_name||""));
		});

	const unmatched=new Set(scheduleEntries.map((_, idx)=>idx));
	const assignments=[];
	const resolvedRows=new Map(scheduleEntries.map((entry)=>[
		entry.tag,
		{
			tag:entry.tag,
			count:0,
			width_m:entry.width_m,
			height_m:entry.height_m,
			area_m2:0,
			has_dims:true,
			source:"OPENING_SCHEDULE_PLAN_WIDTH"
		}
	]));

	const assignGroup=(group, useUnmatchedOnly)=>{
		const candidates=scheduleEntries
			.map((entry, idx)=>({ entry, idx }))
			.filter(({ entry, idx })=>{
				if(useUnmatchedOnly && !unmatched.has(idx)) return false;
				if(String(entry.tag||"").charAt(0)!==String(group.kind||"").charAt(0)) return false;
				if(!openingPlanFamilyCompatible(group, entry)) return false;
				const widthDiff=Math.abs((Number(group.width_m)||0)-(Number(entry.width_m)||0));
				const tol=String(group.kind||"")==="D" ? 0.45 : 0.30;
				return widthDiff<=tol;
			})
			.sort((a,b)=>{
				const diffA=Math.abs((Number(group.width_m)||0)-a.entry.width_m);
				const diffB=Math.abs((Number(group.width_m)||0)-b.entry.width_m);
				if(diffA!==diffB) return diffA-diffB;
				return compareOpeningTags(a.entry.tag, b.entry.tag);
			});
		return candidates[0] || null;
	};

	for(const group of sortedGroups){
		let chosen=assignGroup(group, true);
		if(!chosen) chosen=assignGroup(group, false);
		if(!chosen) continue;
		unmatched.delete(chosen.idx);
		const row=resolvedRows.get(chosen.entry.tag);
		row.count += Number(group.count)||0;
		row.area_m2=roundN((row.width_m*row.height_m*row.count), 4);
		assignments.push({
			tag:chosen.entry.tag,
			kind:group.kind,
			family:group.family,
			base_name:group.base_name,
			plan_width_m:roundN(group.width_m, 4),
			plan_count:Number(group.count)||0,
			schedule_width_m:roundN(chosen.entry.width_m, 4),
			schedule_height_m:roundN(chosen.entry.height_m, 4)
		});
	}

	return {
		rows:[...resolvedRows.values()].sort((a,b)=>compareOpeningTags(a.tag, b.tag)),
		mappingRows:assignments.sort((a,b)=>compareOpeningTags(a.tag, b.tag))
	};
}

function extractOpeningTagsFromText(text){
	const clean=stripCadMarkup(text);
	const tags=[];
	const seen=new Set();
	const re=/((?:DOOR|WINDOWS?|DO|WO|DR|WD|DW|WND|WIN|D|W)\s*(?:TYPE|NO\.?|#)?\s*[-_:]?\s*0*\d{1,3})/ig;
	let m=null;
	while((m=re.exec(clean))!==null){
		const tag=normalizeOpeningTag(m[1]);
		if(!tag || seen.has(tag)) continue;
		seen.add(tag);
		tags.push(tag);
	}
	return tags;
}

function classifyOpeningKindFromText(text){
	const value=String(text||"").toUpperCase();
	const hasWindowToken=/(^|[^A-Z0-9])(WINDOWS?|WIN|WND|WO)([^A-Z0-9]|$)/.test(value) || /AR_WIN[_-]/.test(value);
	const hasDoorToken=/(^|[^A-Z0-9])(DOOR|DO|DR|PASSAGE|ENTRANCE|OPENING)([^A-Z0-9]|$)/.test(value) || /AR_DOR[_-]/.test(value);
	if(hasWindowToken || /\bW\s*[-_:]?\s*0*\d{1,3}\b/.test(value)) return "W";
	if(hasDoorToken || /\bD\s*[-_:]?\s*0*\d{1,3}\b/.test(value)) return "D";
	return null;
}

function pointInAnyScope(x, y, scopes){
	if(!Number.isFinite(x) || !Number.isFinite(y)) return false;
	return Object.values(scopes||{}).some((scope)=>{
		const x1=Math.min(Number(scope?.x1||0), Number(scope?.x2||0));
		const x2=Math.max(Number(scope?.x1||0), Number(scope?.x2||0));
		const y1=Math.min(Number(scope?.y1||0), Number(scope?.y2||0));
		const y2=Math.max(Number(scope?.y1||0), Number(scope?.y2||0));
		if(!Number.isFinite(x1) || !Number.isFinite(x2) || !Number.isFinite(y1) || !Number.isFinite(y2)) return false;
		return x>=x1 && x<=x2 && y>=y1 && y<=y2;
	});
}

function pointInBounds(x, y, bounds, padding=0){
	if(!Number.isFinite(x) || !Number.isFinite(y) || !bounds) return false;
	const x1=Math.min(Number(bounds.x1||0), Number(bounds.x2||0))-padding;
	const x2=Math.max(Number(bounds.x1||0), Number(bounds.x2||0))+padding;
	const y1=Math.min(Number(bounds.y1||0), Number(bounds.y2||0))-padding;
	const y2=Math.max(Number(bounds.y1||0), Number(bounds.y2||0))+padding;
	return x>=x1 && x<=x2 && y>=y1 && y<=y2;
}

function deriveBoundsFromSegments(segments){
	const points=(segments||[]).flatMap((seg)=>[
		{ x:seg?.x1, y:seg?.y1 },
		{ x:seg?.x2, y:seg?.y2 }
	]).filter((p)=>Number.isFinite(p.x) && Number.isFinite(p.y));
	return computeBoundsFromPoints(points);
}

function parseOpenings(texts, entities, unit, planScopes={}, insertRefs=[], wallSegments=[], planOpeningGroups=[]){
	const detailDoorRe=/\b(?:DOOR|D)\s*(?:TYPE|NO\.?|#)?\s*[-_:]?\s*0*(\d{1,3})\b/i;
	const detailWindowRe=/\b(?:WINDOWS?|W)\s*(?:TYPE|NO\.?|#)?\s*[-_:]?\s*0*(\d{1,3})\b/i;
	const wallBounds=deriveBoundsFromSegments(wallSegments);
	const points=[];
	const dims=[];
	const dimHintsByTag={};
	const genericInsertSeen=new Set();
	const genericInsertCandidates=[];
	const autoCounters={ D:0, W:0 };
	const hasReliablePlanScopes=Object.keys(planScopes||{}).length>=2;
	const pushHint=(tag, pair, source, score=0)=>{
		if(!tag || !pair) return;
		const existing=dimHintsByTag[tag];
		if(!existing || (score>existing.score)){
			dimHintsByTag[tag]={ ...pair, source, score };
		}
	};
	for(const t of texts){
		const clean=stripCadMarkup(t.text);
		if(!clean) continue;
		const layer=String(t.layer||"");
		const tags=extractOpeningTagsFromText(clean);
		const dimPairs=parseOpeningDimPairsFromText(clean, unit);
		const layerLooksOpenings=/(?:furn|door|win|open)/i.test(layer);
		const textLooksOpenings=/(?:door|window|opening|باب|ابواب|شباك|نوافذ|فتحات)/i.test(clean);
		const inScope=pointInAnyScope(t.x, t.y, planScopes);
		const inWallBounds=pointInBounds(t.x, t.y, wallBounds, 4);
		const looksLegend=/schedule|legend|table|detail|section|elev|elevation|typ/i.test(clean);
		const hasSpatialEvidence=inScope || (!hasReliablePlanScopes && (inWallBounds || layerLooksOpenings));
		const contextScore=(layerLooksOpenings?1:0)+(textLooksOpenings?2:0)+(inScope?2:0)+(inWallBounds?1:0);
		if(tags.length){
			if(!hasSpatialEvidence) continue;
			if(looksLegend && !(inScope || inWallBounds)) continue;
			for(const tag of tags){
				points.push({ tag, x:t.x, y:t.y, text:clean, source:"TEXT_TAG" });
				if(dimPairs[0]) pushHint(tag, dimPairs[0], "TEXT_TAG_DIM", contextScore+3);
			}
		}
		if(dimPairs.length && hasSpatialEvidence && (contextScore>0 || tags.length)){
			for(const pair of dimPairs){
				dims.push({ ...pair, x:t.x, y:t.y, source:"TEXT_DIM_PAIR", context:contextScore });
			}
		}
	}
	for(const ref of (insertRefs||[])){
		const nameClean=stripCadMarkup(ref.name||"");
		const layer=String(ref.layer||"");
		if(!nameClean) continue;
		const combined=`${layer} ${nameClean}`;
		if(/schedule|legend|table|detail|section|elev|elevation|title|sheet|layout|viewport/i.test(combined)) continue;
		const tags=extractOpeningTagsFromText(nameClean);
		const dimPairs=parseOpeningDimPairsFromText(nameClean, unit);
		const kind=classifyOpeningKindFromText(combined);
		const inScope=pointInAnyScope(ref.x, ref.y, planScopes);
		const inWallBounds=pointInBounds(ref.x, ref.y, wallBounds, 6);
		const contextScore=(kind?2:0)+(inScope?2:0)+(inWallBounds?1:0);
		if(tags.length){
			if(!(inScope || (!hasReliablePlanScopes && inWallBounds))) continue;
			for(const tag of tags){
				points.push({ tag, x:ref.x, y:ref.y, text:nameClean, source:"INSERT_TAG" });
				if(dimPairs[0]) pushHint(tag, dimPairs[0], "INSERT_NAME_DIM", contextScore+2);
			}
			continue;
		}
		if(!kind) continue;
		if(!(inScope || (!hasReliablePlanScopes && (inWallBounds || dimPairs.length)))) continue;
		const gk=`${kind}|${roundN(ref.x,1)}|${roundN(ref.y,1)}`;
		if(genericInsertSeen.has(gk)) continue;
		genericInsertSeen.add(gk);
		genericInsertCandidates.push({
			kind,
			x:ref.x,
			y:ref.y,
			text:nameClean,
			pair:dimPairs[0]||null,
			score:contextScore
		});
	}
	const hasDoorTaggedPoint=points.some((row)=>/^D/.test(String(row.tag||"")));
	const hasWindowTaggedPoint=points.some((row)=>/^W/.test(String(row.tag||"")));
	for(const candidate of genericInsertCandidates){
		const needKind = candidate.kind==="D" ? !hasDoorTaggedPoint : !hasWindowTaggedPoint;
		if(!needKind) continue;
		autoCounters[candidate.kind]+=1;
		const syntheticTag=`${candidate.kind}_AUTO_${autoCounters[candidate.kind]}`;
		points.push({
			tag:syntheticTag,
			x:candidate.x,
			y:candidate.y,
			text:candidate.text,
			source:"INSERT_KIND_FALLBACK"
		});
		if(candidate.pair) pushHint(syntheticTag, candidate.pair, "INSERT_KIND_DIM", candidate.score+1);
	}
	const uniquePoints=[];
	const dedupeDistance=0.75;
	const dedupeDistance2=dedupeDistance*dedupeDistance;
	for(const p of points){
		const hit=uniquePoints.find((row)=>{
			if(row.tag!==p.tag) return false;
			const dx=(Number(row.x)||0)-(Number(p.x)||0);
			const dy=(Number(row.y)||0)-(Number(p.y)||0);
			return (dx*dx+dy*dy)<=dedupeDistance2;
		});
		if(hit) continue;
		uniquePoints.push(p);
	}
	const counts={};
	for(const p of uniquePoints) counts[p.tag]=(counts[p.tag]||0)+1;

	const detailAnchors={};
	for(const t of texts){
		const clean=stripCadMarkup(t.text);
		let m=clean.match(detailDoorRe);
		if(m) detailAnchors[`D${Number(m[1])}`]={ x:t.x, y:t.y, kind:"DOOR", text:t.text };
		m=clean.match(detailWindowRe);
		if(m) detailAnchors[`W${Number(m[1])}`]={ x:t.x, y:t.y, kind:"WINDOW", text:t.text };
	}
	const dimMeasures=collectDimensionMeasurements(entities, unit);
	const pickNearest=(arr, scoreFn)=>{
		if(!arr.length) return null;
		return [...arr].sort((a,b)=>scoreFn(a)-scoreFn(b))[0] || null;
	};

	const byTag={};
	const groupedPointsByTag=uniquePoints.reduce((acc,row)=>{
		(acc[row.tag] ||= []).push(row);
		return acc;
	}, {});
	for(const tag of Object.keys(counts)){
		if(dimHintsByTag[tag]){
			byTag[tag]={ w:dimHintsByTag[tag].w, h:dimHintsByTag[tag].h, source:dimHintsByTag[tag].source };
			continue;
		}
		const anchor=detailAnchors[tag];
		if(anchor){
			const nearby=dimMeasures.filter(d=>typeof d.x==="number" && typeof d.y==="number" && Math.abs(d.x-anchor.x)<=8 && Math.abs(d.y-anchor.y)<=18);
			const widthPool=nearby.filter(d=>d.value_m>=0.3 && d.value_m<=3.5 && d.y<=anchor.y+3);
			const heightPool=nearby.filter(d=>d.value_m>=0.8 && d.value_m<=5.0 && d.y>=anchor.y+1);
			const width=pickNearest(widthPool, d=>Math.abs((d.y||0)-anchor.y) + Math.abs((d.x||0)-anchor.x)*0.4);
			const height=pickNearest(heightPool, d=>Math.abs((d.x||0)-anchor.x) + Math.abs((d.y||0)-(anchor.y+4))*0.15);
			if(width && height){
				byTag[tag]={ w:width.value_m, h:height.value_m, source:"DETAIL_DIMENSION" };
				continue;
			}
		}

		const tagPoints=groupedPointsByTag[tag]||[];
		const planPoint=tagPoints[0];
		const ordered=[...dims].sort((a,b)=>{
			const score=(row)=>{
				let best=Number.POSITIVE_INFINITY;
				for(const pt of tagPoints){
					const d=Math.abs((row.x||0)-(pt?.x||0))+Math.abs((row.y||0)-(pt?.y||0));
					if(d<best) best=d;
				}
				return best-((row.context||0)*0.2);
			};
			return score(a)-score(b);
		});
		if(ordered[0]) byTag[tag]=ordered[0];
	}
	const familyDims={ D:[], W:[] };
	for(const [tag, dim] of Object.entries(byTag)){
		if(!dim?.w || !dim?.h) continue;
		if(/^D/.test(tag)) familyDims.D.push(dim);
		if(/^W/.test(tag)) familyDims.W.push(dim);
	}
	const familyMedian=(arr)=>{
		if(!arr.length) return null;
		const ws=arr.map((row)=>row.w).sort((a,b)=>a-b);
		const hs=arr.map((row)=>row.h).sort((a,b)=>a-b);
		const mid=Math.floor(arr.length/2);
		return { w:ws[mid], h:hs[mid], source:"FAMILY_MEDIAN_DIM" };
	};
	const dMedian=familyMedian(familyDims.D);
	const wMedian=familyMedian(familyDims.W);
	for(const tag of Object.keys(counts)){
		if(byTag[tag]) continue;
		if(/^D/.test(tag) && dMedian) byTag[tag]=dMedian;
		if(/^W/.test(tag) && wMedian) byTag[tag]=wMedian;
	}

	let totalArea=0;
	const rows=[];
	for(const tag of Object.keys(counts).sort()){
		const c=counts[tag];
		const d=byTag[tag];
		const eachArea=d?(d.w*d.h):0;
		const area=eachArea*c;
		totalArea += area;
		rows.push({ tag, count:c, width_m:d?d.w:0, height_m:d?d.h:0, area_m2:area, has_dims:Boolean(d), source:d?.source||"" });
	}
	const insertRows=deriveOpeningRowsFromInsertFamilies(insertRefs, unit, planScopes, wallBounds);
	const scheduleMap=extractOpeningScheduleMap(texts, entities, unit);
	const scheduledFromPlan=buildScheduledRowsFromPlanGroups(scheduleMap, planOpeningGroups);
	const typed=(list, prefix)=>list.filter((row)=>String(row.tag||"").toUpperCase().startsWith(prefix));
	const buildScheduleRows=(prefix)=>{
		const prefixRows=Object.entries(scheduleMap||{})
			.filter(([tag])=>String(tag||"").toUpperCase().startsWith(prefix))
			.map(([tag, dim])=>({ tag:String(tag).toUpperCase(), dim }))
			.sort((a,b)=>compareOpeningTags(a.tag, b.tag));
		if(!prefixRows.length) return [];
		const tagRows=typed(rows, prefix);
		const insertMappedRows=typed(mapInsertRowsToSchedule(insertRows, scheduleMap), prefix);
		const tagCountByTag=new Map();
		for(const row of tagRows){
			tagCountByTag.set(String(row.tag||"").toUpperCase(), Number(row.count)||0);
		}
		const insertCountByTag=new Map();
		for(const row of insertMappedRows){
			const key=String(row.tag||"").toUpperCase();
			insertCountByTag.set(key, Number(row.count)||0);
		}
		const out=[];
		for(const item of prefixRows){
			const key=item.tag;
			const insertCount=Number(insertCountByTag.get(key)||0);
			const rawTagCount=Number(tagCountByTag.get(key)||0);
			const cappedTagCount=rawTagCount>40 ? 0 : rawTagCount;
			const count=insertCount>0 ? insertCount : cappedTagCount;
			const w=Number(item.dim?.w)||0;
			const h=Number(item.dim?.h)||0;
			if(!(w>0 && h>0)) continue;
			out.push({
				tag:key,
				count,
				width_m:w,
				height_m:h,
				area_m2:roundN(w*h*count,4),
				has_dims:true,
				source:"OPENING_SCHEDULE_TYPED"
			});
		}
		return out;
	};
	const chooseRows=(primary, fallback, prefix)=>{
		const p=typed(primary, prefix);
		const f=typed(fallback, prefix);
		if(!f.length) return p;
		const pCount=p.reduce((sum,row)=>sum+(Number(row.count)||0),0);
		const fCount=f.reduce((sum,row)=>sum+(Number(row.count)||0),0);
		const pTypes=p.length;
		const fTypes=f.length;
		if((fTypes>=2 && pTypes<=1) || (fCount>pCount*1.35)) return f;
		return p;
	};
	const planSizeRows=derivePlanOpeningTypesBySize(insertRefs, entities, unit, planScopes);
	const hasPlanScheduledWindows=typed(scheduledFromPlan.rows, "W").length>0;
	const hasPlanScheduledDoors=typed(scheduledFromPlan.rows, "D").length>0;
	const windowsScheduledRaw=hasPlanScheduledWindows ? typed(scheduledFromPlan.rows, "W") : buildScheduleRows("W");
	const doorsScheduledRaw=hasPlanScheduledDoors ? typed(scheduledFromPlan.rows, "D") : buildScheduleRows("D");
	const windowsScheduled=hasPlanScheduledWindows ? windowsScheduledRaw : enrichScheduledCountsFromPlanSize(windowsScheduledRaw, planSizeRows, "W");
	const doorsScheduled=hasPlanScheduledDoors ? doorsScheduledRaw : enrichScheduledCountsFromPlanSize(doorsScheduledRaw, planSizeRows, "D");
	let windowsChosen=windowsScheduled.length ? windowsScheduled : chooseRows(rows, insertRows, "W");
	let doorsChosen=doorsScheduled.length ? doorsScheduled : chooseRows(rows, insertRows, "D");
	const planSizeWindows=typed(planSizeRows, "W");
	const planSizeDoors=typed(planSizeRows, "D");
	const winNonZero=windowsChosen.filter((r)=>(Number(r.count)||0)>0).length;
	const doorNonZero=doorsChosen.filter((r)=>(Number(r.count)||0)>0).length;
	if(planSizeWindows.length>=2 && winNonZero<=1){
		windowsChosen=planSizeWindows;
	}
	if(planSizeDoors.length>=2 && doorNonZero<=1){
		doorsChosen=planSizeDoors;
	}
	const merged=[...windowsChosen, ...doorsChosen].sort((a,b)=>compareOpeningTags(a.tag, b.tag));
	if(!merged.length){
		const emptyTotal=0;
		return { rows:merged, totalArea:emptyTotal, mappingRows:scheduledFromPlan.mappingRows||[] };
	}
	const mergedTotal=merged.reduce((sum,row)=>sum+(Number(row.area_m2)||0),0);
	return { rows:merged, totalArea:mergedTotal, mappingRows:scheduledFromPlan.mappingRows||[] };
}

function stripCadMarkup(text){
	return String(text||"")
		.replace(/\\[A-Za-z][^;]*;/g, " ")
		.replace(/[{}]/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function detectPlanScopes(texts, segments=[]){
	const names=[
		{ key:"GROUND", re:/\b(?:GROUND\s+FLOOR|G\.?\s*F\.?)\s+PLAN\b|مسقط\s+الدور\s+الارضي|الدور\s+الارضي/i },
		{ key:"FIRST", re:/\b(?:FIRST|1ST)\s+FLOOR\s+PLAN\b|مسقط\s+الدور\s+الاول|الدور\s+الاول/i },
		{ key:"ROOF", re:/\b(?:ROOF|R\.?\s*F\.?)\s+PLAN\b|مسقط\s+السطح|السطح/i },
		{ key:"TOP_ROOF", re:/\bTOP(?:\s+OF)?\s+ROOF\s+PLAN\b|مسقط\s+اعلى\s+السطح/i }
	];
	const matches=[];
	for(const row of (texts||[])){
		if(typeof row.x!=="number" || typeof row.y!=="number") continue;
		const cleaned=stripCadMarkup(row.text).toUpperCase();
		const match=names.find(entry=>entry.re.test(cleaned));
		if(match) matches.push({ name:match.key, x:row.x, y:row.y, raw:row.text });
	}
	if(!matches.length){
		const grouped={};
		const pushPoint=(key, x, y)=>{
			if(!key || typeof x!=="number" || typeof y!=="number") return;
			(grouped[key] ||= []).push({ x, y });
		};
		for(const row of (texts||[])){
			const key=scopeKeyFromPlanLayer(row.layer);
			pushPoint(key, row.x, row.y);
		}
		for(const seg of (segments||[])){
			const key=scopeKeyFromPlanLayer(seg.layer);
			if(!key) continue;
			pushPoint(key, seg.x1, seg.y1);
			pushPoint(key, seg.x2, seg.y2);
		}
		const scopes={};
		for(const [key, pts] of Object.entries(grouped)){
			const bounds=computeBoundsFromPoints(pts);
			if(!bounds || pts.length<8) continue;
			const margin=1.5;
			scopes[key]={
				name:key,
				title_x:(bounds.x1+bounds.x2)/2,
				title_y:bounds.y2,
				x1:bounds.x1-margin,
				x2:bounds.x2+margin,
				y1:bounds.y1-margin,
				y2:bounds.y2+margin,
				source:"LAYER_SCOPE"
			};
		}
		return scopes;
	}

	const groups=[];
	for(const item of matches.sort((a,b)=>b.y-a.y)){
		const group=groups.find(entry=>Math.abs(entry.y-item.y)<=1.0);
		if(group) group.items.push(item);
		else groups.push({ y:item.y, items:[item] });
	}
	const best=groups
		.map((group)=>{
			const dedup=Object.values(group.items.reduce((acc,item)=>{
				if(!acc[item.name] || item.x<acc[item.name].x) acc[item.name]=item;
				return acc;
			}, {}));
			return { y:group.y, items:dedup };
		})
		.filter(group=>group.items.length>=2 && group.items.some(item=>item.name==="GROUND") && group.items.some(item=>item.name==="FIRST"))
		.sort((a,b)=>b.items.length-a.items.length || b.y-a.y)[0];
	if(!best) return {};

	const yValues=[];
	for(const row of (texts||[])){
		if(typeof row?.y==="number" && isFinite(row.y)) yValues.push(row.y);
	}
	for(const seg of (segments||[])){
		if(seg.ori==="H") yValues.push(seg.c);
		else if(seg.ori==="V"){
			yValues.push(seg.a);
			yValues.push(seg.b);
		}
	}
	const minGlobalY=yValues.length ? Math.min(...yValues) : (best.y-3500);
	const maxGlobalY=yValues.length ? Math.max(...yValues) : (best.y+200);
	const yPad=10;

	const items=[...best.items].sort((a,b)=>a.x-b.x);
	const scopes={};
	for(let i=0;i<items.length;i++){
		const current=items[i];
		const prev=items[i-1];
		const next=items[i+1];
		const span=next ? (next.x-current.x) : (current.x-(prev?.x??(current.x-65)));
		const x1=prev ? ((prev.x+current.x)/2) : (current.x-(span/2));
		const x2=next ? ((current.x+next.x)/2) : (current.x+(span/2));
		scopes[current.name]={
			name:current.name,
			title_x:current.x,
			title_y:current.y,
			x1,
			x2,
			y1:minGlobalY-yPad,
			y2:maxGlobalY+yPad,
			source:"TITLE_BAND_EXPANDED"
		};
	}
	const valid=Object.fromEntries(
		Object.entries(scopes).filter(([, scope])=>(Math.abs(scope.x2-scope.x1)>=3) && (Math.abs(scope.y2-scope.y1)>=10))
	);
	return valid;
}

function inferPlanScopesFromTopLevelBlocks(doc, scaleFactor, texts=[]){
	const topLevel=Array.isArray(doc?.entities) ? doc.entities : [];
	const codeToScope={ "00":"GROUND", "01":"FIRST", "02":"ROOF", "03":"TOP_ROOF" };
	const candidates=[];
	for(const entity of topLevel){
		if(entity?.type!=="INSERT") continue;
		const name=String(entity.name||"");
		const match=name.match(/^B(\d{2})-/i);
		if(!match) continue;
		const scopeName=codeToScope[match[1]];
		if(!scopeName) continue;
		const expanded=expandInsertEntities(doc, entity);
		const points=expanded.flatMap((row)=>collectEntityPoints(row));
		if(!points.length) continue;
		const scaledPoints=points.map((point)=>({ x:point.x*scaleFactor, y:point.y*scaleFactor }));
		const bounds=computeBoundsFromPoints(scaledPoints);
		if(!bounds) continue;
		const rect={
			name:scopeName,
			x1:bounds.x1-1.0,
			x2:bounds.x2+1.0,
			y1:bounds.y1-1.0,
			y2:bounds.y2+1.0
		};
		const textHits=(texts||[]).filter((row)=>{
			const x=Number(row?.x);
			const y=Number(row?.y);
			return Number.isFinite(x) && Number.isFinite(y) && x>=rect.x1 && x<=rect.x2 && y>=rect.y1 && y<=rect.y2;
		}).length;
		candidates.push({
			...rect,
			title_x:(rect.x1+rect.x2)/2,
			title_y:(rect.y1+rect.y2)/2,
			source:"TOP_LEVEL_BLOCK_SCOPE",
			text_hits:textHits,
			area_m2:Math.max(0,(rect.x2-rect.x1)*(rect.y2-rect.y1))
		});
	}
	const bestByScope={};
	for(const candidate of candidates){
		const current=bestByScope[candidate.name];
		if(
			!current ||
			candidate.text_hits>current.text_hits ||
			(candidate.text_hits===current.text_hits && candidate.area_m2>current.area_m2)
		){
			bestByScope[candidate.name]=candidate;
		}
	}
	return bestByScope;
}

function pairRect(pair){
	if(pair.ori==="H"){
		return {
			x1:Math.max(pair.s1.a,pair.s2.a),
			x2:Math.min(pair.s1.b,pair.s2.b),
			y1:Math.min(pair.s1.c,pair.s2.c),
			y2:Math.max(pair.s1.c,pair.s2.c)
		};
	}
	return {
		x1:Math.min(pair.s1.c,pair.s2.c),
		x2:Math.max(pair.s1.c,pair.s2.c),
		y1:Math.max(pair.s1.a,pair.s2.a),
		y2:Math.min(pair.s1.b,pair.s2.b)
	};
}

function pairMidpoint(pair){
	const rect=pairRect(pair);
	return { x:(rect.x1+rect.x2)/2, y:(rect.y1+rect.y2)/2 };
}

function filterPairsInScope(pairs, scope){
	if(!scope) return [];
	return (pairs||[]).filter((pair)=>{
		const mid=pairMidpoint(pair);
		return mid.x>=scope.x1 && mid.x<=scope.x2 && mid.y>=scope.y1 && mid.y<=scope.y2;
	});
}

function normalizeScopeRectValues(scope){
	if(!scope) return null;
	const x1=Number(scope.x1);
	const x2=Number(scope.x2);
	const y1=Number(scope.y1);
	const y2=Number(scope.y2);
	if(!Number.isFinite(x1) || !Number.isFinite(x2) || !Number.isFinite(y1) || !Number.isFinite(y2)) return null;
	return {
		x1:Math.min(x1,x2),
		x2:Math.max(x1,x2),
		y1:Math.min(y1,y2),
		y2:Math.max(y1,y2)
	};
}

function scopeRectsFromMap(scopes){
	return Object.values(scopes||{})
		.map(normalizeScopeRectValues)
		.filter(Boolean);
}

function filterPairsInAnyScope(pairs, scopes){
	const rects=scopeRectsFromMap(scopes);
	if(!rects.length) return [];
	return (pairs||[]).filter((pair)=>{
		const mid=pairMidpoint(pair);
		return rects.some((rect)=>mid.x>=rect.x1 && mid.x<=rect.x2 && mid.y>=rect.y1 && mid.y<=rect.y2);
	});
}

function segmentMidpoint(seg){
	return {
		x:(Number(seg?.x1)+Number(seg?.x2))/2,
		y:(Number(seg?.y1)+Number(seg?.y2))/2
	};
}

function filterSegmentsInAnyScope(segments, scopes){
	const rects=scopeRectsFromMap(scopes);
	if(!rects.length) return segments||[];
	return (segments||[]).filter((seg)=>{
		const mid=segmentMidpoint(seg);
		if(!Number.isFinite(mid.x) || !Number.isFinite(mid.y)) return false;
		return rects.some((rect)=>mid.x>=rect.x1 && mid.x<=rect.x2 && mid.y>=rect.y1 && mid.y<=rect.y2);
	});
}

function rectOverlapRatio(a, b){
	const ax1=Math.min(a.x1,a.x2); const ax2=Math.max(a.x1,a.x2);
	const ay1=Math.min(a.y1,a.y2); const ay2=Math.max(a.y1,a.y2);
	const bx1=Math.min(b.x1,b.x2); const bx2=Math.max(b.x1,b.x2);
	const by1=Math.min(b.y1,b.y2); const by2=Math.max(b.y1,b.y2);
	const ix=Math.max(0, Math.min(ax2,bx2)-Math.max(ax1,bx1));
	const iy=Math.max(0, Math.min(ay2,by2)-Math.max(ay1,by1));
	const inter=ix*iy;
	if(!(inter>0)) return 0;
	const areaA=Math.max(0, (ax2-ax1)*(ay2-ay1));
	const areaB=Math.max(0, (bx2-bx1)*(by2-by1));
	const minArea=Math.min(areaA, areaB);
	if(!(minArea>0)) return 0;
	return inter/minArea;
}

function inferPlanScopesFromPairs(pairs, opts={}){
	const points=(pairs||[])
		.map((pair, index)=>({ index, ...pairMidpoint(pair) }))
		.filter((point)=>Number.isFinite(point.x) && Number.isFinite(point.y));
	const minPts=Math.max(8, Number(opts.min_pts||12));
	if(points.length<minPts) return {};

	const eps=Math.max(8, Number(opts.eps_m||28));
	const eps2=eps*eps;
	const cell=Math.max(5, eps);
	const cellMap=new Map();
	const keyOf=(ix,iy)=>`${ix},${iy}`;

	for(let i=0;i<points.length;i+=1){
		const p=points[i];
		const ix=Math.floor(p.x/cell);
		const iy=Math.floor(p.y/cell);
		const key=keyOf(ix,iy);
		const bucket=cellMap.get(key) || [];
		bucket.push(i);
		cellMap.set(key, bucket);
	}

	const visited=new Uint8Array(points.length);
	const clusters=[];
	for(let i=0;i<points.length;i+=1){
		if(visited[i]) continue;
		visited[i]=1;
		const queue=[i];
		const cluster=[];
		while(queue.length){
			const idx=queue.pop();
			cluster.push(idx);
			const p=points[idx];
			const ix=Math.floor(p.x/cell);
			const iy=Math.floor(p.y/cell);
			for(let dx=-1;dx<=1;dx+=1){
				for(let dy=-1;dy<=1;dy+=1){
					const bucket=cellMap.get(keyOf(ix+dx, iy+dy));
					if(!bucket || !bucket.length) continue;
					for(const j of bucket){
						if(visited[j]) continue;
						const q=points[j];
						const ddx=q.x-p.x;
						const ddy=q.y-p.y;
						if((ddx*ddx)+(ddy*ddy)>eps2) continue;
						visited[j]=1;
						queue.push(j);
					}
				}
			}
		}
		if(cluster.length>=minPts) clusters.push(cluster);
	}
	if(!clusters.length) return {};

	const clusterRects=clusters.map((cluster)=>{
		const clusterPoints=cluster.map((idx)=>points[idx]);
		const bounds=computeBoundsFromPoints(clusterPoints);
		if(!bounds) return null;
		const pad=Math.max(1.5, eps*0.25);
		return {
			count:cluster.length,
			x1:bounds.x1-pad,
			x2:bounds.x2+pad,
			y1:bounds.y1-pad,
			y2:bounds.y2+pad,
			cx:(bounds.x1+bounds.x2)/2,
			cy:(bounds.y1+bounds.y2)/2
		};
	}).filter(Boolean).sort((a,b)=>b.count-a.count);

	const selected=[];
	for(const rect of clusterRects){
		if(selected.some((existing)=>rectOverlapRatio(existing, rect)>0.65)) continue;
		selected.push(rect);
		if(selected.length>=4) break;
	}
	if(!selected.length) return {};

	const names=["GROUND", "FIRST", "ROOF", "TOP_ROOF"];
	const ordered=selected.sort((a,b)=>{
		const yDelta=Math.abs(a.cy-b.cy);
		if(yDelta<=8) return a.cx-b.cx;
		return b.cy-a.cy;
	});
	const scopes={};
	for(let i=0;i<ordered.length && i<names.length;i+=1){
		const rect=ordered[i];
		scopes[names[i]]={
			name:names[i],
			x1:rect.x1,
			x2:rect.x2,
			y1:rect.y1,
			y2:rect.y2,
			title_x:rect.cx,
			title_y:rect.cy,
			source:"AUTO_PAIR_CLUSTER",
			pair_count:rect.count
		};
	}
	return scopes;
}

function summarizeOpeningAreas(rows, options={}){
	const maxDeductionHeightM=Number.isFinite(Number(options.maxDeductionHeightM))
		? Number(options.maxDeductionHeightM)
		: Number.POSITIVE_INFINITY;
	return (rows||[]).reduce((acc,row)=>{
		const area=Number(row.area_m2)||0;
		const heightM=Number(row.height_m)||0;
		if(!(area>0)) return acc;
		if(heightM>maxDeductionHeightM) return acc;
		if(/^W/i.test(String(row.tag||""))) acc.windows_m2 += area;
		else if(/^D/i.test(String(row.tag||""))) acc.doors_m2 += area;
		return acc;
	}, { windows_m2:0, doors_m2:0 });
}

function openingTagSortKey(tag){
	const value=String(tag||"").toUpperCase();
	const type=value.startsWith("W") ? "W" : (value.startsWith("D") ? "D" : "Z");
	let index=999999;
	let isAuto=1;
	const direct=value.match(/^[WD](\d+)$/);
	if(direct){
		index=Number(direct[1]);
		isAuto=0;
	}else{
		const auto=value.match(/^[WD]_AUTO_(\d+)$/);
		if(auto) index=Number(auto[1]);
	}
	return [type, isAuto, Number.isFinite(index) ? index : 999999, value];
}

function compareOpeningTags(a, b){
	const ka=openingTagSortKey(a);
	const kb=openingTagSortKey(b);
	for(let i=0;i<ka.length;i++){
		if(ka[i]<kb[i]) return -1;
		if(ka[i]>kb[i]) return 1;
	}
	return 0;
}

function buildOpeningScheduleRows(rows){
	return (rows||[])
		.map((row)=>({
			tag:String(row.tag||""),
			width_m:roundN(Number(row.width_m)||0,4),
			height_m:roundN(Number(row.height_m)||0,4),
			no:Number(row.count)||0,
			area_m2:roundN(Number(row.area_m2)||0,4)
		}))
		.sort((a,b)=>compareOpeningTags(a.tag, b.tag));
}

function buildOpeningTypeTables(rows){
	const scheduleRows=buildOpeningScheduleRows(rows);
	const toTableRows=(prefix)=>scheduleRows
		.filter((row)=>new RegExp(`^${prefix}\\d{1,3}$`,"i").test(row.tag))
		.sort((a,b)=>compareOpeningTags(a.tag, b.tag))
		.map((row, idx)=>({
			TYPE:`${prefix}${idx+1}`,
			W:roundN(Number(row.width_m)||0,4),
			H:roundN(Number(row.height_m)||0,4),
			NO:Number(row.no)||0,
			AREA:roundN(Number(row.area_m2)||0,4)
		}));
	const windows=toTableRows("W");
	const doors=toTableRows("D");
	return { windows, doors };
}

function buildOpeningTypeTablesText(rows){
	const { windows, doors }=buildOpeningTypeTables(rows);
	const render=(title, tableRows)=>{
		const lines=[title, "TYPE,W,H,NO,AREA"];
		for(const row of tableRows){
			lines.push(`${row.TYPE},${row.W},${row.H},${row.NO},${row.AREA}`);
		}
		return lines.join("\n");
	};
	return [render("WINDOWS_TABLE", windows), "", render("DOORS_TABLE", doors)].join("\n");
}

function buildExactOpeningScheduleAoA(rows){
	const { windows, doors }=buildOpeningTypeTables(rows);
	const sum=(list)=>roundN(list.reduce((acc,row)=>acc+(Number(row.AREA)||0),0),4);
	const windowsTotal=sum(windows);
	const doorsTotal=sum(doors);
	const grandTotal=roundN(windowsTotal+doorsTotal,4);
	const aoa=[
		["DOOR, WINDOW SCHEDULE","","","","","",""],
		["Tag","Description","L (m)","W (m)","Count","Area/Unit (m²)","Total Area (m²)"],
		["WINDOW SCHEDULE","","","","","",""]
	];
	for(const row of windows){
		const areaUnit=roundN((Number(row.W)||0)*(Number(row.H)||0),4);
		aoa.push([
			row.TYPE,
			"Extracted window type",
			row.H,
			row.W,
			row.NO,
			areaUnit,
			row.AREA
		]);
	}
	aoa.push(["","Subtotal - WINDOW SCHEDULE","","","","",windowsTotal]);
	aoa.push(["DOOR SCHEDULE","","","","","",""]);
	for(const row of doors){
		const areaUnit=roundN((Number(row.W)||0)*(Number(row.H)||0),4);
		aoa.push([
			row.TYPE,
			"Extracted door type",
			row.H,
			row.W,
			row.NO,
			areaUnit,
			row.AREA
		]);
	}
	aoa.push(["","Subtotal - DOOR SCHEDULE","","","","",doorsTotal]);
	aoa.push(["","Grand Total","","","","",grandTotal]);
	return aoa;
}

function deriveThermalSplitRatio(pairs, scopes){
	const targetScopes=[scopes.GROUND, scopes.FIRST].filter(Boolean);
	if(!targetScopes.length) return { ratio:0.5, source:"FALLBACK_NO_PLAN_SCOPE", scoped_length_m:0 };

	let extLen=0;
	let totalLen=0;
	for(const scope of targetScopes){
		const scopedPairs=filterPairsInScope(pairs, scope).filter(pair=>Math.abs(pair.thickness-0.20)<=0.02);
		if(!scopedPairs.length) continue;
		const rects=scopedPairs.map((pair)=>({ pair, rect:pairRect(pair) }));
		const rectPoints=rects.flatMap((entry)=>[
			{ x:entry.rect.x1, y:entry.rect.y1 },
			{ x:entry.rect.x2, y:entry.rect.y2 }
		]);
		const rectBounds=computeBoundsFromPoints(rectPoints);
		if(!rectBounds) continue;
		const bounds={
			minX:rectBounds.x1,
			maxX:rectBounds.x2,
			minY:rectBounds.y1,
			maxY:rectBounds.y2
		};
		for(const entry of rects){
			const distToEdge=Math.min(
				Math.abs(entry.rect.x1-bounds.minX),
				Math.abs(entry.rect.x2-bounds.maxX),
				Math.abs(entry.rect.y1-bounds.minY),
				Math.abs(entry.rect.y2-bounds.maxY)
			);
			totalLen += entry.pair.overlap;
			if(distToEdge<=5.0) extLen += entry.pair.overlap;
		}
	}
	if(!(totalLen>0)) return { ratio:0.5, source:"FALLBACK_NO_SCOPED_20CM", scoped_length_m:0 };
	return {
		ratio:Math.max(0.2, Math.min(0.8, extLen/totalLen)),
		source:"PLAN_SCOPE_ENVELOPE_T5",
		scoped_length_m:totalLen
	};
}

function buildArchDerivedModel({ byT, h, openings, thermalSplit }){
	const gross20Area=(Number(byT["0.20"])||0)*h;
	const gross10Area=(Number(byT["0.10"])||0)*h;
	const gross15Area=(Number(byT["0.15"])||0)*h;
	const gross25Area=(Number(byT["0.25"])||0)*h;
	const openingAreas=summarizeOpeningAreas(openings.rows, {
		// Full-height glazing and curtain-wall-like schedule items should remain in the schedule,
		// but they should not erase blockwork wall faces during quantity deduction.
		maxDeductionHeightM:Math.max(2.8, h-0.6)
	});
	const mainDoorAreaM2=(openings.rows||[])
		.filter((row)=>/^D/i.test(String(row.tag||"")))
		.reduce((best,row)=>Math.max(best, Number(row.area_m2)||0), 0);
	const internalDoorAreaM2=Math.max(0, openingAreas.doors_m2-mainDoorAreaM2);
	const external20GrossArea=gross20Area*thermalSplit.ratio;
	const internal20GrossArea=Math.max(0, gross20Area-external20GrossArea);
	const internalSixGrossArea=gross10Area+gross15Area;
	const internalGrossArea=internal20GrossArea+gross10Area+gross15Area;
	const externalGrossArea=external20GrossArea+gross25Area;
	const externalNetArea=Math.max(0, externalGrossArea-openingAreas.windows_m2-mainDoorAreaM2);
	const internalDoorDeductionAreaM2=internalDoorAreaM2*0.4;
	const internal20DoorDeductionAreaM2=internalGrossArea>0 ? (internalDoorDeductionAreaM2*(internal20GrossArea/internalGrossArea)) : 0;
	const internalSixDoorDeductionAreaM2=internalGrossArea>0 ? (internalDoorDeductionAreaM2*(internalSixGrossArea/internalGrossArea)) : 0;
	const internal20NetArea=Math.max(0, internal20GrossArea-internal20DoorDeductionAreaM2);
	const internalSixNetArea=Math.max(0, internalSixGrossArea-internalSixDoorDeductionAreaM2);
	const internalNetArea=internal20NetArea+internalSixNetArea;
	const totalGrossArea=externalGrossArea+internalGrossArea;
	const totalNetArea=externalNetArea+internalNetArea;

	const externalGrossVolume=(external20GrossArea*0.20)+(gross25Area*0.25);
	const internalGrossVolume=(internal20GrossArea*0.20)+(gross10Area*0.10)+(gross15Area*0.15);
	const internalAvgThickness=internalGrossArea>0 ? (internalGrossVolume/internalGrossArea) : 0.20;
	const externalAvgThickness=externalGrossArea>0 ? (externalGrossVolume/externalGrossArea) : 0.20;
	const externalNetVolume=Math.max(0, externalGrossVolume-((openingAreas.windows_m2+mainDoorAreaM2)*externalAvgThickness));
	const internalNetVolume=Math.max(0, internalGrossVolume-(internalDoorDeductionAreaM2*internalAvgThickness));

	const internalPlasterGrossM2=(internalGrossArea*2)+externalGrossArea;
	const externalPlasterGrossM2=externalGrossArea;
	const internalPlasterNetM2=Math.max(0, internalPlasterGrossM2-(openingAreas.doors_m2*2)-openingAreas.windows_m2);
	const externalPlasterNetM2=Math.max(0, externalPlasterGrossM2-openingAreas.windows_m2);

	return {
		thermalSplit,
		openingAreas,
		external25GrossAreaM2:gross25Area,
		external20GrossAreaM2:external20GrossArea,
		externalGrossAreaM2:externalGrossArea,
		internal20GrossAreaM2:internal20GrossArea,
		internal20NetAreaM2:internal20NetArea,
		internal10GrossAreaM2:gross10Area,
		internal15GrossAreaM2:gross15Area,
		internalSixNetAreaM2:internalSixNetArea,
		internalGrossAreaM2:internalGrossArea,
		externalNetAreaM2:externalNetArea,
		internalNetAreaM2:internalNetArea,
		totalGrossAreaM2:totalGrossArea,
		totalNetAreaM2:totalNetArea,
		externalGrossVolumeM3:externalGrossVolume,
		internalGrossVolumeM3:internalGrossVolume,
		totalGrossVolumeM3:externalGrossVolume+internalGrossVolume,
		externalNetVolumeM3:externalNetVolume,
		internalNetVolumeM3:internalNetVolume,
		totalNetVolumeM3:externalNetVolume+internalNetVolume,
		internalPlasterGrossM2,
		externalPlasterGrossM2,
		internalPlasterNetM2,
		externalPlasterNetM2,
		internalPaintNetM2:internalPlasterNetM2,
		externalPaintNetM2:externalPlasterNetM2,
		mainDoorAreaM2,
		internalDoorDeductionAreaM2
	};
}

function calibrateThermalSplitRatio({ baseSplit, byT, h, openings, referenceTotalsByKey }){
	const extTarget=Number(referenceTotalsByKey.BLOCK_EXTERNAL_THERMAL_M2||0);
	const intTarget=Number(referenceTotalsByKey.BLOCK_INTERNAL_HOLLOW_M2||0);
	const plasterTarget=Number(referenceTotalsByKey.PLASTER_INTERNAL_NET_M2||referenceTotalsByKey.PLASTER_INTERNAL_GROSS_M2||0);
	const targetCount=[extTarget,intTarget,plasterTarget].filter(v=>v>0).length;
	if(targetCount<2) return { ...baseSplit, calibrated:false };

	let best={ ratio:baseSplit.ratio, minAccuracy:-1, avgAccuracy:-1 };
	const lower=Math.max(0.35, baseSplit.ratio-0.20);
	const upper=Math.min(0.70, baseSplit.ratio+0.20);
	for(let ratio=lower; ratio<=upper+0.0001; ratio+=0.0025){
		const model=buildArchDerivedModel({ byT, h, openings, thermalSplit:{ ratio, source:"REFERENCE_ASSISTED_SCAN", scoped_length_m:baseSplit.scoped_length_m } });
		const accuracies=[];
		if(extTarget>0){
			accuracies.push(Math.max(0, 100-Math.abs(((model.externalNetAreaM2-extTarget)/extTarget)*100)));
		}
		if(intTarget>0){
			accuracies.push(Math.max(0, 100-Math.abs(((model.internalNetAreaM2-intTarget)/intTarget)*100)));
		}
		if(plasterTarget>0){
			accuracies.push(Math.max(0, 100-Math.abs(((model.internalPlasterNetM2-plasterTarget)/plasterTarget)*100)));
		}
		const minAccuracy=accuracies.length ? accuracies.reduce((min,value)=>Math.min(min, value), Infinity) : 0;
		const avgAccuracy=accuracies.reduce((sum,value)=>sum+value,0)/(accuracies.length||1);
		if(
			minAccuracy>best.minAccuracy ||
			(minAccuracy===best.minAccuracy && avgAccuracy>best.avgAccuracy)
		){
			best={ ratio, minAccuracy, avgAccuracy };
		}
	}
	return {
		ratio:best.ratio,
		source:"REFERENCE_ASSISTED_SPLIT_CALIBRATION",
		scoped_length_m:baseSplit.scoped_length_m,
		base_ratio:baseSplit.ratio,
		calibrated:true
	};
}

function buildArchSystemItems({
	blockItems,
	model,
	evidenceConfidence
}){
	const rows=[];
	for(const item of (blockItems||[])){
		const wallArea=item.length_m*item.height_m;
		rows.push({
			item:item.code,
			category:item.code.includes("EXTERNAL")?"EXTERNAL":"INTERNAL",
			unit:"m3",
			qty:roundN(item.volume_m3,4),
			length_m:roundN(item.length_m,4),
			height_m:roundN(item.height_m,4),
			thickness_m:roundN(item.thickness_m,3),
			wall_area_m2:roundN(wallArea,4),
			evidence_confidence:evidenceConfidence,
			source:"ARCH_WALL_PAIRING"
		});
	}
	rows.push(
		{ item:"BLOCK_EXTERNAL_THERMAL_M2", category:"EXTERNAL", unit:"m2", qty:roundN(model.externalNetAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_EXTERNAL_MINUS_WINDOWS_AND_MAIN_DOOR" },
		{ item:"BLOCK_INTERNAL_HOLLOW_8_M2", category:"INTERNAL", unit:"m2", qty:roundN(model.internal20NetAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_INTERNAL_20CM_MINUS_40PCT_INTERNAL_DOORS" },
		{ item:"BLOCK_INTERNAL_HOLLOW_6_M2", category:"INTERNAL", unit:"m2", qty:roundN(model.internalSixNetAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_INTERNAL_10_15CM_MINUS_40PCT_INTERNAL_DOORS" },
		{ item:"BLOCK_INTERNAL_HOLLOW_M2", category:"INTERNAL", unit:"m2", qty:roundN(model.internalNetAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_20CM_SCOPE_SPLIT_PLUS_INTERNAL_BLOCKS_MINUS_DOORS" },
		{ item:"INTERNAL_WALL_AREA_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.internalGrossAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_GROSS_WALL_MODEL" },
		{ item:"EXTERNAL_WALL_AREA_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.externalGrossAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_GROSS_WALL_MODEL" },
		{ item:"BLOCKWORK_GROSS_AREA_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.totalGrossAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_GROSS_WALL_MODEL" },
		{ item:"OPENINGS_AREA_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.openingAreas.windows_m2+model.openingAreas.doors_m2,4), evidence_confidence:evidenceConfidence, source:"ARCH_OPENINGS" },
		{ item:"WINDOWS_AREA_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.openingAreas.windows_m2,4), evidence_confidence:evidenceConfidence, source:"ARCH_OPENINGS" },
		{ item:"DOORS_AREA_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.openingAreas.doors_m2,4), evidence_confidence:evidenceConfidence, source:"ARCH_OPENINGS" },
		{ item:"BLOCKWORK_NET_AREA_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.totalNetAreaM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_SPLIT_MINUS_OPENINGS" },
		{ item:"BLOCKWORK_TOTAL_GROSS_M3", category:"SUMMARY", unit:"m3", qty:roundN(model.totalGrossVolumeM3,4), evidence_confidence:evidenceConfidence, source:"ARCH_GROSS_WALL_MODEL" },
		{ item:"BLOCKWORK_TOTAL_NET_M3", category:"SUMMARY", unit:"m3", qty:roundN(model.totalNetVolumeM3,4), evidence_confidence:evidenceConfidence, source:"ARCH_SPLIT_MINUS_OPENINGS" },
		{ item:"PLASTER_INTERNAL_GROSS_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.internalPlasterGrossM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_RULE_INTERNAL_PLASTER_GROSS" },
		{ item:"PLASTER_EXTERNAL_GROSS_M2", category:"SUMMARY", unit:"m2", qty:roundN(model.externalPlasterGrossM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_RULE_EXTERNAL_PLASTER_GROSS" },
		{ item:"PLASTER_INTERNAL_NET_M2", category:"FINISH", unit:"m2", qty:roundN(model.internalPlasterNetM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_RULE_INTERNAL_PLASTER_NET" },
		{ item:"PLASTER_EXTERNAL_NET_M2", category:"FINISH", unit:"m2", qty:roundN(model.externalPlasterNetM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_RULE_EXTERNAL_PLASTER_NET" },
		{ item:"PAINT_INTERNAL_NET_M2", category:"FINISH", unit:"m2", qty:roundN(model.internalPaintNetM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_RULE_INTERNAL_PAINT_NET" },
		{ item:"PAINT_EXTERNAL_NET_M2", category:"FINISH", unit:"m2", qty:roundN(model.externalPaintNetM2,4), evidence_confidence:evidenceConfidence, source:"ARCH_RULE_EXTERNAL_PAINT_NET" }
	);
	return rows;
}

function runArchPipeline({ archDxfPath, referencePath, allowExternalReference=false, inputs, outDir }){
	if(referencePath && !allowExternalReference){
		throw new Error("External reference input is disabled. ARCH pipeline accepts drawing inputs only.");
	}
	const { rules, source:rulesSource, signature:rulesSignature } = loadRules("VILLA_G1");
	const parser=new DxfParser();
	const dxf=parser.parseSync(fs.readFileSync(archDxfPath, "utf8"));
	const cadUnit=getCadLengthUnit(dxf.header, inputs?.dimUnit||null);
	const scaleFactor=unitScaleToMeters(cadUnit);
	const entitiesRaw=scaleEntitiesToMeters(flattenDxfEntities(dxf), scaleFactor);
	const insertRefsRaw=scaleInsertRefsToMeters(collectInsertReferences(dxf), scaleFactor);
	const entitiesScopedRect=applyScopeRectToEntities(entitiesRaw, inputs?.scopeRect);
	const insertRefsScopedRect=applyScopeRectToInsertRefs(insertRefsRaw, inputs?.scopeRect);
	const entities=applyScopeCircleToEntities(entitiesScopedRect, inputs?.scopeCenter, inputs?.scopeRadiusM ?? inputs?.scopeRadius);
	const insertRefs=applyScopeCircleToInsertRefs(insertRefsScopedRect, inputs?.scopeCenter, inputs?.scopeRadiusM ?? inputs?.scopeRadius);
	const texts=collectTextsFromEntities(entities);
	const disciplineSig = detectDisciplineSignature(texts);
	const allSegs=dedupeSegments(extractSegments(entities));
	const wallLayerSegs=allSegs.filter(s=>isLikelyWallLayer(s.layer));
	const layerSupportRatio = allSegs.length ? (wallLayerSegs.length/allSegs.length) : 0;
	const initialPlanScopes=detectPlanScopes(texts, allSegs);
	let scopedFloorPlanSegs=allSegs.filter((seg)=>isScopedFloorPlanWallCandidateLayer(seg.layer));
	if(Object.keys(initialPlanScopes).length){
		scopedFloorPlanSegs=filterSegmentsInAnyScope(scopedFloorPlanSegs, initialPlanScopes);
	}
	const bestEvidence=chooseBestWallEvidenceSet([
		{ name:"WALL_LAYER", segments:wallLayerSegs },
		{ name:"SCOPED_PLAN", segments:scopedFloorPlanSegs },
		{ name:"ALL_SEGMENTS", segments:allSegs }
	]);
	const selectedSegs=bestEvidence.segments.length ? bestEvidence.segments : allSegs;
	const pairsRaw=bestEvidence.pairs.length ? bestEvidence.pairs : pairWalls(selectedSegs);
	const selectedEvidenceSource=bestEvidence.name || "ALL_SEGMENTS";
	const segById=Object.fromEntries(selectedSegs.map(seg=>[seg.id, seg]));
	const resolvedPairs=pairsRaw.map((pair)=>({
		...pair,
		s1:pair.s1 || pair.s1Ref || segById[pair.s1],
		s2:pair.s2 || pair.s2Ref || segById[pair.s2]
	})).filter(pair=>pair.s1 && pair.s2);
	let planScopes=Object.keys(initialPlanScopes).length ? initialPlanScopes : detectPlanScopes(texts, selectedSegs);
	if(Object.keys(planScopes).length<2){
		const blockScopes=inferPlanScopesFromTopLevelBlocks(dxf, scaleFactor, texts);
		if(Object.keys(blockScopes).length>Object.keys(planScopes).length) planScopes=blockScopes;
	}
	if(!Object.keys(planScopes).length){
		const inferredScopes=inferPlanScopesFromPairs(resolvedPairs);
		if(Object.keys(inferredScopes).length) planScopes=inferredScopes;
	}
	const detectedScopeCount=Object.keys(planScopes).length;
	const scopedPairs=filterPairsInAnyScope(resolvedPairs, planScopes);
	const minScopedPairs=Math.max(12, Math.floor(resolvedPairs.length*0.20));
	const scopedCoverage=resolvedPairs.length ? (scopedPairs.length/resolvedPairs.length) : 0;
	const useScopedPairs=detectedScopeCount>=2 && scopedPairs.length>=minScopedPairs && scopedCoverage>=0.45;
	const pairs=useScopedPairs ? scopedPairs : resolvedPairs;

	const byT={ "0.10":0, "0.15":0, "0.20":0, "0.25":0 };
	for(const p of pairs){
		const k=p.thickness.toFixed(2);
		if(byT[k]!==undefined) byT[k]+=p.overlap;
	}

	const hInput=Number(inputs?.levels?.g_floor_to_floor_m||0);
	const h=(hInput>4.15) ? 4.0 : hInput;
	const itemStops=[];
	if(!h) itemStops.push({ item:"ARCH_WALLS", reason:"MISSING_HEIGHT_INPUT", reason_ar:"Missing wall height input." });
	if(!pairs.length) itemStops.push({ item:"ARCH_WALLS", reason:"NO_WALL_PAIR_EVIDENCE", reason_ar:"No wall pair evidence detected." });

	const blockItems=[];
	const addBlock=(code,th,len)=>{
		if(len<=0 || !h) return;
		const m3=len*th*h;
		blockItems.push({ code, thickness_m:th, length_m:len, height_m:h, volume_m3:m3 });
	};
	addBlock("BLOCK_INTERNAL_10",0.10,byT["0.10"]);
	addBlock("BLOCK_INTERNAL_15",0.15,byT["0.15"]);
	addBlock("BLOCK_INTERNAL_20",0.20,byT["0.20"]);
	addBlock("BLOCK_EXTERNAL_25_THERMAL",0.25,byT["0.25"]);

	const planOpeningGroups=collectPlanOpeningGroups(dxf, scaleFactor, planScopes);
	const openings=parseOpenings(texts, entities, cadUnit, planScopes, insertRefs, selectedSegs, planOpeningGroups);
	if(openings.rows.length && !openings.rows.some(r=>r.has_dims)){
		itemStops.push({ item:"OPENINGS", reason:"OPENING_DIMS_NOT_RESOLVED", reason_ar:"Opening dimensions could not be resolved." });
	}

	const hasExternalReference=false;
	const referenceTotalsByKey={};

	const baseThermalSplit=(detectedScopeCount>=2)
		? deriveThermalSplitRatio(pairs, planScopes)
		: { ratio:0.5, source:"FALLBACK_INSUFFICIENT_PLAN_SCOPE", scoped_length_m:0 };
	const referenceAssistedThermalSplit=null;
	const thermalSplit=baseThermalSplit;
	const model=buildArchDerivedModel({ byT, h, openings, thermalSplit });
	let broadInternalSegs=allSegs.filter((seg)=>isBroadInternalCandidateLayer(seg.layer));
	if(detectedScopeCount>=2) broadInternalSegs=filterSegmentsInAnyScope(broadInternalSegs, planScopes);
	const broadInternalPairs=pairWalls(broadInternalSegs);
	const broadInternalByT={ "0.10":0, "0.15":0, "0.20":0, "0.25":0 };
	for(const pair of broadInternalPairs){
		const key=pair.thickness.toFixed(2);
		if(broadInternalByT[key]!==undefined) broadInternalByT[key]+=pair.overlap;
	}
	const useBroadBlend=broadInternalPairs.length>=Math.max(8, Math.floor(pairs.length*0.15));
	const broadInternalModel=useBroadBlend
		? buildArchDerivedModel({
			byT:broadInternalByT,
			h,
			openings:{ rows:[], totalArea:0 },
			thermalSplit:{ ratio:0.5, source:"BROAD_INTERNAL_BLEND", scoped_length_m:0 }
		})
		: null;
	const scopedSixArea=model.internal10GrossAreaM2 + model.internal15GrossAreaM2;
	const broadSixArea=useBroadBlend ? (broadInternalModel.internal10GrossAreaM2 + broadInternalModel.internal15GrossAreaM2) : scopedSixArea;
	const rawBlendedEightArea=useBroadBlend ? ((model.internal20GrossAreaM2 + broadInternalModel.internal20GrossAreaM2) / 2) : model.internal20GrossAreaM2;
	const rawBlendedSixArea=useBroadBlend ? ((scopedSixArea + broadSixArea) / 2) : scopedSixArea;
	const referenceHollow8=0;
	const referenceHollow6=0;
	const referenceHollowTotal=referenceHollow8+referenceHollow6;
	const blendedInternalArea=rawBlendedEightArea + rawBlendedSixArea;
	const blendedEightArea=(referenceHollowTotal>0 && blendedInternalArea>0)
		? (blendedInternalArea * (referenceHollow8/referenceHollowTotal))
		: rawBlendedEightArea;
	const blendedSixArea=(referenceHollowTotal>0 && blendedInternalArea>0)
		? (blendedInternalArea * (referenceHollow6/referenceHollowTotal))
		: rawBlendedSixArea;
	const externalWallLengthM=h>0 ? (model.externalGrossAreaM2/h) : 0;
	const parapetHeightM=deriveParapetHeightFromTexts(texts);
	const roofPlanMetrics=deriveRoofPlanMetrics(filterSegmentsInAnyScope(allSegs, planScopes), planScopes);
	const externalFinishUpliftM2=(externalWallLengthM * (parapetHeightM + 0.20)) + (Number(roofPlanMetrics.roof_edge_length_m||0) * 0.25);
	model.internal20GrossAreaM2=blendedEightArea;
	model.internal10GrossAreaM2=blendedSixArea;
	model.internal15GrossAreaM2=0;
	const blendedInternalDoorDeductionAreaM2=Number(model.internalDoorDeductionAreaM2||0);
	const blendedEightDoorDeductionAreaM2=blendedInternalArea>0 ? (blendedInternalDoorDeductionAreaM2*(blendedEightArea/blendedInternalArea)) : 0;
	const blendedSixDoorDeductionAreaM2=blendedInternalArea>0 ? (blendedInternalDoorDeductionAreaM2*(blendedSixArea/blendedInternalArea)) : 0;
	model.internal20NetAreaM2=Math.max(0, blendedEightArea-blendedEightDoorDeductionAreaM2);
	model.internalSixNetAreaM2=Math.max(0, blendedSixArea-blendedSixDoorDeductionAreaM2);
	model.internalGrossAreaM2=blendedInternalArea;
	model.internalNetAreaM2=model.internal20NetAreaM2 + model.internalSixNetAreaM2;
	model.totalGrossAreaM2=model.externalGrossAreaM2 + blendedInternalArea;
	model.totalNetAreaM2=model.externalNetAreaM2 + model.internalNetAreaM2;
	model.internalGrossVolumeM3=(blendedEightArea*0.20) + (blendedSixArea*0.15);
	model.internalNetVolumeM3=(model.internal20NetAreaM2*0.20) + (model.internalSixNetAreaM2*0.15);
	model.totalGrossVolumeM3=model.externalGrossVolumeM3 + model.internalGrossVolumeM3;
	model.totalNetVolumeM3=model.externalNetVolumeM3 + model.internalNetVolumeM3;
	model.internalPlasterGrossM2=(blendedInternalArea*2) + model.externalGrossAreaM2;
	model.internalPlasterNetM2=model.internalPlasterGrossM2;
	model.internalPaintNetM2=model.internalPlasterNetM2;
	model.externalPlasterGrossM2=model.externalGrossAreaM2 + externalFinishUpliftM2;
	model.externalPlasterNetM2=model.externalPlasterGrossM2;
	model.externalPaintNetM2=model.externalPlasterNetM2;
	blockItems.length=0;
	addBlock("BLOCK_INTERNAL_6_HOLLOW",0.15,h?blendedSixArea/h:0);
	addBlock("BLOCK_INTERNAL_20_HOLLOW",0.20,h?model.internal20GrossAreaM2/h:0);
	addBlock("BLOCK_EXTERNAL_20_THERMAL",0.20,h?model.external20GrossAreaM2/h:0);
	addBlock("BLOCK_EXTERNAL_25_THERMAL",0.25,h?model.external25GrossAreaM2/h:0);
	const evidenceConfidence=(pairs.length>=20 && h>0 && (layerSupportRatio>=0.05 || wallLayerSegs.length>=80)) ? "HIGH" : (pairs.length>=8 && h>0 ? "MEDIUM" : "LOW");
	const systemItems=buildArchSystemItems({
		blockItems,
		model,
		evidenceConfidence
	});

	const systemQtyByKey={
		BLOCK_EXTERNAL_THERMAL_M2:model.externalNetAreaM2,
		BLOCK_INTERNAL_HOLLOW_8_M2:model.internal20NetAreaM2,
		BLOCK_INTERNAL_HOLLOW_6_M2:model.internalSixNetAreaM2,
		BLOCK_INTERNAL_HOLLOW_M2:model.internalNetAreaM2,
		BLOCKWORK_TOTAL_M2:model.totalNetAreaM2,
		BLOCKWORK_TOTAL_M3:model.totalNetVolumeM3,
		PLASTER_INTERNAL_NET_M2:model.internalPlasterNetM2,
		PLASTER_EXTERNAL_NET_M2:model.externalPlasterNetM2,
		PLASTER_INTERNAL_GROSS_M2:model.internalPlasterNetM2,
		PLASTER_EXTERNAL_GROSS_M2:model.externalPlasterNetM2,
		PAINT_INTERNAL_NET_M2:model.internalPaintNetM2,
		PAINT_EXTERNAL_NET_M2:model.externalPaintNetM2,
		PLASTER_PARAPET_INTERNAL_M2:0
	};
	const band={ code:"UNSCORED", label:"Unscored", highlight:evidenceConfidence==="LOW"?"red":"amber" };
	const qtoMode="QTO_ONLY";

	const warnings=[];
	const hardBlocks=[];
	if(!inputs?.allowMixedDiscipline && disciplineSig.likely==="STR" && disciplineSig.strScore>=3){
		hardBlocks.push(`WRONG_DISCIPLINE_DXF: likely STR (str=${disciplineSig.strScore}, arch=${disciplineSig.archScore}, finish=${disciplineSig.finishScore})`);
	}
	if(hInput>4.15 && h===4.0){
		warnings.push({ severity:"HIGH", code:"ARCH_CLEAR_WALL_HEIGHT_RULE", message:`ARCH wall-face quantities used 4.00 m clear wall height instead of ${roundN(hInput,2)} m floor-to-floor height to avoid overstating blockwork and finishes.`, action:"Review only if this project truly bills full floor-to-floor wall height." });
	}
	if(openings.totalArea>0){
		warnings.push({ severity:"HIGH", code:"ARCH_OPENING_SPLIT_RULE", message:"Openings are split as windows on external walls and doors on internal walls for net quantities.", action:"Review only if this project has significant external door area that must be isolated." });
	}
	if(model.external25GrossAreaM2>0 && model.external20GrossAreaM2>0){
		warnings.push({ severity:"HIGH", code:"ARCH_25CM_ADDED_TO_EXTERNAL", message:`${roundN(model.external25GrossAreaM2,2)} m2 of 25 cm wall traces were added to the external thermal envelope.`, action:"Review only if the project bills 250 mm walls separately from the external thermal block package." });
	}
	if(evidenceConfidence==="LOW"){
		warnings.push({ severity:"CRITICAL", code:"ARCH_LOW_EVIDENCE_CONFIDENCE", message:"Wall-pair evidence confidence is LOW.", action:"Use an architectural floor plan DXF with wall layers and rerun." });
	}
	const allItemsFailCount=0;

	const quantities={
		scope:"ARCH quantities (blockwork first, drawing-first mode)",
		measurement_mode:"QTO_ONLY",
		external_reference_enabled:false,
		external_reference_mode:"NONE",
		projectType:"VILLA_G1",
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		computed_summary:{
			drawing_unit:cadUnit,
			internal_wall_area_m2:model.internalGrossAreaM2,
			external_wall_area_m2:model.externalGrossAreaM2,
			blockwork_gross_area_m2:model.totalGrossAreaM2,
			openings_total_m2:model.openingAreas.windows_m2+model.openingAreas.doors_m2,
			windows_area_m2:model.openingAreas.windows_m2,
			doors_area_m2:model.openingAreas.doors_m2,
			blockwork_net_area_m2:model.totalNetAreaM2,
			blockwork_gross_m3:model.totalGrossVolumeM3,
			opening_deduction_m3:model.totalGrossVolumeM3-model.totalNetVolumeM3,
			blockwork_net_m3:model.totalNetVolumeM3,
			external_thermal_block_net_m2:model.externalNetAreaM2,
			internal_hollow_block_net_m2:model.internalNetAreaM2,
			internal_plaster_gross_m2:model.internalPlasterGrossM2,
			external_plaster_gross_m2:model.externalPlasterGrossM2,
			internal_plaster_net_m2:model.internalPlasterNetM2,
			external_plaster_net_m2:model.externalPlasterNetM2,
			internal_paint_net_m2:model.internalPaintNetM2,
			external_paint_net_m2:model.externalPaintNetM2,
			thermal_split_ratio:model.thermalSplit.ratio,
			wall_pairs_count:pairs.length,
			wall_layer_segment_count:wallLayerSegs.length,
			layer_support_ratio:layerSupportRatio,
			evidence_confidence:evidenceConfidence
		},
		items:blockItems,
		system_items:systemItems,
		item_stop:itemStops,
		accuracy_notice:"QTO quantities are produced from drawing evidence only in runtime.",
		professional_warnings:warnings
	};

	let runStatus="REJECTED";
	let rejectedReason=null;
	let releaseGate="BLOCKED";
	if(blockItems.length && h>0 && pairs.length && evidenceConfidence!=="LOW"){
		runStatus="OK";
		rejectedReason=null;
		releaseGate="ELIGIBLE";
	}else{
		const reasons=[];
		if(!h) reasons.push("MISSING_HEIGHT_INPUT");
		if(!pairs.length) reasons.push("NO_WALL_PAIR_EVIDENCE");
		if(!blockItems.length) reasons.push("NO_BLOCKWORK_ITEMS");
		if(evidenceConfidence==="LOW") reasons.push("LOW_EVIDENCE_CONFIDENCE");
		rejectedReason=reasons.join(" | ") || "BLOCKED";
	}
	if(hardBlocks.length){
		runStatus="REJECTED";
		rejectedReason=[...hardBlocks, rejectedReason].filter(Boolean).join(" | ");
		releaseGate="BLOCKED_HARD_RULE";
	}

	const evidence={
		file:path.basename(archDxfPath),
		stats:{
			text_entities_total:texts.length,
			segment_count:allSegs.length,
			wall_layer_segment_count:wallLayerSegs.length,
			selected_segment_count:selectedSegs.length,
			selected_segment_source:selectedEvidenceSource,
			wall_pair_count:pairs.length,
			plan_scopes_detected:Object.keys(planScopes).length
		},
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		plan_scopes:planScopes,
		thermal_split:model.thermalSplit,
		split_preview:referenceAssistedThermalSplit,
		notes:["ARCH blockwork default output active.", "Run mode: QTO_ONLY"]
	};

	const requiredQuestions = buildRequiredQuestionsFromRules(rules, inputs);
	const releaseDecision = finalizeReleaseGate({ runStatus, warnings, requiredQuestions, hardBlocks, mode:"QTO_ONLY" });

	const runMeta={
		projectId:inputs?._meta?.projectId||null,
		runId:inputs?._meta?.runId||null,
		timestamp:inputs?._meta?.timestamp||new Date().toISOString(),
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		discipline_signature:disciplineSig,
		discipline:"ARCH",
		measurement_mode:"QTO_ONLY",
		external_reference_enabled:false,
		external_reference_mode:"NONE",
		min_accuracy_pct:MIN_ACCURACY_PCT,
		target_accuracy_pct:TARGET_ACCURACY_PCT,
		honest_mode:HONEST_MODE,
		evidence_confidence:evidenceConfidence,
		run_status:runStatus,
		rejected_reason:rejectedReason,
		release_gate:releaseDecision.gate || releaseGate,
		item_accuracy:{
			blockwork:{ item:"Blockwork", accuracy_pct:null, band:{ code:"UNSCORED", label:"Unscored", highlight:band.highlight }, min90_status:"N/A", target100_status:"N/A" }
		},
		external_reference_items:0,
		all_items_failed_gate_count:allItemsFailCount,
		professional_warnings:warnings,
		explanations:{
			policy:"No assumption policy active. Quantity output is generated from drawing evidence in QTO-only runtime mode.",
			release_condition:"Requires evidence confidence != LOW and usable wall-pair blockwork output."
		}
	};

	const qualityReport={
		discipline:"ARCH",
		policy:{ min_accuracy_pct:MIN_ACCURACY_PCT, target_accuracy_pct:TARGET_ACCURACY_PCT },
		discipline_signature:disciplineSig,
		item_accuracy:runMeta.item_accuracy,
		warnings,
		required_questions:requiredQuestions,
		hard_blocks:hardBlocks,
		release_decision:releaseDecision
	};

	fs.writeFileSync(path.join(outDir,"arch_qto_mode.json"), JSON.stringify({
		mode:"QTO_ONLY",
		external_reference_enabled:false,
		external_reference_totals:{},
		system_qty_by_key:systemQtyByKey
	},null,2));
	const openingScheduleRows=buildOpeningScheduleRows(openings.rows);
	const openingTypeTables=buildOpeningTypeTables(openings.rows);
	const openingTablesText=buildOpeningTypeTablesText(openings.rows);
	const exactScheduleAoA=buildExactOpeningScheduleAoA(openings.rows);
	const exactScheduleSheet=XLSX.utils.aoa_to_sheet(exactScheduleAoA);
	const exactScheduleBook=XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(exactScheduleBook, exactScheduleSheet, "Schedule");
	fs.writeFileSync(path.join(outDir,"arch_quantities.json"), JSON.stringify(quantities,null,2));
	fs.writeFileSync(path.join(outDir,"arch_run_meta.json"), JSON.stringify(runMeta,null,2));
	fs.writeFileSync(path.join(outDir,"arch_evidence.json"), JSON.stringify(evidence,null,2));
	fs.writeFileSync(path.join(outDir,"arch_openings_table.csv"), Papa.unparse(openings.rows));
	fs.writeFileSync(path.join(outDir,"arch_openings_schedule.csv"), Papa.unparse(openingScheduleRows));
	fs.writeFileSync(path.join(outDir,"arch_openings_windows_table.csv"), Papa.unparse(openingTypeTables.windows));
	fs.writeFileSync(path.join(outDir,"arch_openings_doors_table.csv"), Papa.unparse(openingTypeTables.doors));
	fs.writeFileSync(path.join(outDir,"arch_openings_plan_mapping.csv"), Papa.unparse(openings.mappingRows||[]));
	fs.writeFileSync(path.join(outDir,"arch_openings_type_tables.txt"), openingTablesText);
	fs.writeFileSync(path.join(outDir,"arch_openings_schedule_exact.csv"), XLSX.utils.sheet_to_csv(exactScheduleSheet));
	XLSX.writeFile(exactScheduleBook, path.join(outDir,"arch_openings_schedule_exact.xlsx"));
	fs.writeFileSync(path.join(outDir,"arch_system_items.csv"), Papa.unparse(systemItems));
	fs.writeFileSync(path.join(outDir,"arch_required_questions.json"), JSON.stringify(requiredQuestions,null,2));
	fs.writeFileSync(path.join(outDir,"arch_quality_report.json"), JSON.stringify(qualityReport,null,2));

	return {
		qtoModeFile:"arch_qto_mode.json",
		quantitiesFile:"arch_quantities.json",
		runMetaFile:"arch_run_meta.json",
		evidenceFile:"arch_evidence.json",
		openingsTableFile:"arch_openings_table.csv",
		openingsScheduleCsvFile:"arch_openings_schedule.csv",
		openingsWindowsTableCsvFile:"arch_openings_windows_table.csv",
		openingsDoorsTableCsvFile:"arch_openings_doors_table.csv",
		openingsPlanMappingCsvFile:"arch_openings_plan_mapping.csv",
		openingsTypeTablesTxtFile:"arch_openings_type_tables.txt",
		openingsExactScheduleCsvFile:"arch_openings_schedule_exact.csv",
		openingsExactScheduleXlsxFile:"arch_openings_schedule_exact.xlsx",
		systemCsv:"arch_system_items.csv",
		requiredQuestionsFile:"arch_required_questions.json",
		qualityReportFile:"arch_quality_report.json"
	};
}

module.exports = { runArchPipeline };
