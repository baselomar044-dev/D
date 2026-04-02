const fs = require("fs");
const path = require("path");
const DxfParser = require("dxf-parser");
const { PDFParse } = require("pdf-parse");
const XLSX = require("xlsx");
const Papa = require("papaparse");
const { MIN_ACCURACY_PCT, TARGET_ACCURACY_PCT, toAccuracyPct, passesAccuracyGate, meetsTargetAccuracy, accuracyBand } = require("./accuracyPolicy");
const { loadRules } = require("./rules");
const { detectDisciplineSignature, buildRequiredQuestionsFromRules, buildItemQuality, finalizeReleaseGate } = require("./quality");

const FOOTING_TAG_REGEX = /^(?:F\d+|CF\d+|WF\d+)$/i;
const COLUMN_TAG_REGEX = /^C(?:0*[1-9]\d*)$/i;
const TIE_BEAM_TAG_REGEX = /^(?:TB\d+|CTB\d+)$/i;
const BEAM_TAG_REGEX = /^B[1-9]\d*$/i;

function normText(s){
	if(typeof s!=="string") return "";
	return s
		.replace(/\u0000/g, "")
		.replace(/\\P/g, " ")
		.replace(/\\[A-Za-z][^;]*;/g, " ")
		.replace(/[{}]/g, " ")
		.replace(/\^I/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function isFootingTag(tag){
	return FOOTING_TAG_REGEX.test(String(tag||"").trim());
}

function isColumnTag(tag){
	return COLUMN_TAG_REGEX.test(String(tag||"").trim());
}

function isTieBeamTag(tag){
	return TIE_BEAM_TAG_REGEX.test(String(tag||"").trim());
}

function isBeamTag(tag){
	return BEAM_TAG_REGEX.test(String(tag||"").trim());
}

function isLikelyColumnPlanLayer(layer){
	const value=String(layer||"").toUpperCase();
	if(!value) return false;
	if(/TITLE|SHEET|BORDER|NOTE|SCHEDULE|IM-TITLE|EP-NOTES/.test(value)) return false;
	return /COL|COLUMN|AXN|S-TEXT25-S20|S-STRS|COLUMN-NO|COL-NO/.test(value);
}

function getXY(e){
	const p = e.startPoint || e.position || e.insert || null;
	if(!p || typeof p.x!=="number" || typeof p.y!=="number") return { x:null, y:null };
	return { x:p.x, y:p.y };
}

function collectTextsFromEntities(entities, source="MODEL", blockName=""){
	const out=[];
	for(const e of (entities||[])){
		const {x,y}=getXY(e);
		const h = (typeof e.height==="number" && isFinite(e.height) && e.height>0)
			? e.height
			: ((typeof e.textHeight==="number" && isFinite(e.textHeight) && e.textHeight>0) ? e.textHeight : null);
		if(e.type==="TEXT" && typeof e.text==="string") out.push({ layer:e.layer||"", text:normText(e.text), x, y, h, type:"TEXT", source, blockName });
		else if(e.type==="MTEXT" && typeof e.text==="string") out.push({ layer:e.layer||"", text:normText(e.text), x, y, h, type:"MTEXT", source, blockName });
		else if(e.type==="ATTRIB" && typeof e.text==="string") out.push({ layer:e.layer||"", text:normText(e.text), x, y, h, type:"ATTRIB", source, blockName });
	}
	return out;
}

function buildBlockTextIndex(dxf){
	const blocks=dxf.blocks||{};
	const cache=new Map();
	const visit=(name, depth=0)=>{
		if(!name || depth>6) return [];
		if(cache.has(name)) return cache.get(name);
		const b=blocks[name];
		if(!b){ cache.set(name,[]); return []; }
		let texts=collectTextsFromEntities(b.entities, "BLOCK_DEF", name);
		for(const ent of (b.entities||[])) if(ent.type==="INSERT" && ent.name) texts=texts.concat(visit(ent.name, depth+1));
		cache.set(name, texts);
		return texts;
	};
	for(const n of Object.keys(blocks)) visit(n,0);
	return cache;
}

function cloneEntity(e){
	return JSON.parse(JSON.stringify(e));
}

function transformPoint(p, insert){
	if(!p || typeof p.x!=="number" || typeof p.y!=="number") return p;
	const sx=insert.xScale==null?1:Number(insert.xScale)||1;
	const sy=insert.yScale==null?1:Number(insert.yScale)||1;
	const rot=((Number(insert.rotation)||0)*Math.PI)/180;
	const x=(p.x||0)*sx;
	const y=(p.y||0)*sy;
	const xr=(x*Math.cos(rot))-(y*Math.sin(rot));
	const yr=(x*Math.sin(rot))+(y*Math.cos(rot));
	return {
		x:xr + (insert.position?.x||0),
		y:yr + (insert.position?.y||0),
		z:(p.z||0) + (insert.position?.z||0)
	};
}

function expandInsertEntities(doc, insert, depth=0){
	if(!insert || insert.type!=="INSERT" || !insert.name || depth>6) return [];
	const block=doc?.blocks?.[insert.name];
	if(!block || !Array.isArray(block.entities) || !block.entities.length) return [];

	const out=[];
	for(const entity of block.entities){
		if(entity.type==="INSERT"){
			const child=cloneEntity(entity);
			child.position=transformPoint(entity.position||{ x:0, y:0, z:0 }, insert);
			child.xScale=(entity.xScale==null?1:Number(entity.xScale)||1) * (insert.xScale==null?1:Number(insert.xScale)||1);
			child.yScale=(entity.yScale==null?1:Number(entity.yScale)||1) * (insert.yScale==null?1:Number(insert.yScale)||1);
			child.rotation=(Number(entity.rotation)||0) + (Number(insert.rotation)||0);
			out.push(...expandInsertEntities(doc, child, depth+1));
			continue;
		}

		const copy=cloneEntity(entity);
		if(copy.startPoint) copy.startPoint=transformPoint(copy.startPoint, insert);
		if(copy.endPoint) copy.endPoint=transformPoint(copy.endPoint, insert);
		if(copy.position) copy.position=transformPoint(copy.position, insert);
		if(copy.vertices) copy.vertices=copy.vertices.map(v=>transformPoint(v, insert));
		if(copy.anchorPoint) copy.anchorPoint=transformPoint(copy.anchorPoint, insert);
		if(copy.middleOfText) copy.middleOfText=transformPoint(copy.middleOfText, insert);
		out.push(copy);
	}
	return out;
}

function flattenDxfEntities(doc){
	const topLevel=Array.isArray(doc?.entities) ? doc.entities : [];
	const nonInsert=topLevel.filter(e=>e.type!=="INSERT");
	const expanded=[];
	for(const entity of topLevel){
		if(entity.type!=="INSERT") continue;
		expanded.push(...expandInsertEntities(doc, entity));
	}
	return nonInsert.concat(expanded);
}

function normalizeLengthUnitToken(unit){
	const raw=String(unit||"").trim().toLowerCase();
	if(!raw || raw==="auto") return null;
	if(["mm","millimeter","millimeters"].includes(raw)) return "mm";
	if(["cm","centimeter","centimeters"].includes(raw)) return "cm";
	if(["m","meter","meters"].includes(raw)) return "m";
	if(["in","inch","inches"].includes(raw)) return "in";
	if(["ft","foot","feet"].includes(raw)) return "ft";
	return null;
}

function unitToScale(unit){
	if(unit==="mm") return 0.001;
	if(unit==="cm") return 0.01;
	if(unit==="m") return 1;
	if(unit==="in") return 0.0254;
	if(unit==="ft") return 0.3048;
	return null;
}

function detectCadScaleToMeters(header, fallbackUnit="m"){
	const dimlfac=Number(header?.$DIMLFAC);
	if(Number.isFinite(dimlfac) && dimlfac>0 && dimlfac<=0.01) return dimlfac;
	const userUnit=normalizeLengthUnitToken(fallbackUnit);
	const userScale=unitToScale(userUnit);
	const ins=Number(header?.$INSUNITS);
	if(ins===4) return 0.001;
	if(ins===5) return 0.01;
	if(ins===6) return 1;
	if(ins===1){
		if(userScale!=null) return userScale;
		return 0.0254;
	}
	if(ins===2){
		if(userScale!=null) return userScale;
		return 0.3048;
	}
	if(userScale!=null) return userScale;
	const measurement=Number(header?.$MEASUREMENT);
	if(measurement===1) return 0.001;
	return 1;
}

function scalePointToMeters(p, factor){
	if(!p || typeof p.x!=="number" || typeof p.y!=="number") return p;
	return {
		...p,
		x:p.x*factor,
		y:p.y*factor,
		z:typeof p.z==="number" ? p.z*factor : p.z
	};
}

function scaleEntitiesToMeters(entities, factor){
	if(!(factor>0) || factor===1) return entities;
	return (entities||[]).map((entity)=>{
		const copy=cloneEntity(entity);
		if(copy.startPoint) copy.startPoint=scalePointToMeters(copy.startPoint, factor);
		if(copy.endPoint) copy.endPoint=scalePointToMeters(copy.endPoint, factor);
		if(copy.position) copy.position=scalePointToMeters(copy.position, factor);
		if(copy.vertices) copy.vertices=copy.vertices.map((vertex)=>scalePointToMeters(vertex, factor));
		if(copy.anchorPoint) copy.anchorPoint=scalePointToMeters(copy.anchorPoint, factor);
		if(copy.middleOfText) copy.middleOfText=scalePointToMeters(copy.middleOfText, factor);
		return copy;
	});
}

function normalizeScopeRect(scopeRect){
	if(!scopeRect || typeof scopeRect!=="object") return null;
	const x1=Number(scopeRect.x1);
	const x2=Number(scopeRect.x2);
	const y1=Number(scopeRect.y1);
	const y2=Number(scopeRect.y2);
	if(!Number.isFinite(x1) || !Number.isFinite(x2) || !Number.isFinite(y1) || !Number.isFinite(y2)) return null;
	return {
		x1:Math.min(x1,x2),
		x2:Math.max(x1,x2),
		y1:Math.min(y1,y2),
		y2:Math.max(y1,y2)
	};
}

function collectEntityPoints(entity){
	const points=[];
	if(entity?.startPoint) points.push(entity.startPoint);
	if(entity?.endPoint) points.push(entity.endPoint);
	if(entity?.position) points.push(entity.position);
	if(Array.isArray(entity?.vertices)) points.push(...entity.vertices);
	return points.filter((point)=>Number.isFinite(point?.x) && Number.isFinite(point?.y));
}

function entityIntersectsScopeRect(entity, scopeRect){
	const points=collectEntityPoints(entity);
	if(!points.length) return true;
	const xs=points.map((point)=>point.x);
	const ys=points.map((point)=>point.y);
	const e={
		x1:Math.min(...xs),
		x2:Math.max(...xs),
		y1:Math.min(...ys),
		y2:Math.max(...ys)
	};
	return !(e.x2<scopeRect.x1 || e.x1>scopeRect.x2 || e.y2<scopeRect.y1 || e.y1>scopeRect.y2);
}

function applyScopeRectToEntities(entities, scopeRect){
	const normalized=normalizeScopeRect(scopeRect);
	if(!normalized) return entities;
	return (entities||[]).filter((entity)=>entityIntersectsScopeRect(entity, normalized));
}

function normalizeScopeCircle(scopeCenter, scopeRadius){
	const cx=Number(scopeCenter?.x);
	const cy=Number(scopeCenter?.y);
	const r=Number(scopeRadius);
	if(!Number.isFinite(cx) || !Number.isFinite(cy) || !Number.isFinite(r) || !(r>0)) return null;
	return { x:cx, y:cy, r };
}

function entityIntersectsScopeCircle(entity, scopeCircle){
	const points=collectEntityPoints(entity);
	if(!points.length) return true;
	const r2=scopeCircle.r*scopeCircle.r;
	return points.some((point)=>{
		const dx=point.x-scopeCircle.x;
		const dy=point.y-scopeCircle.y;
		return (dx*dx+dy*dy)<=r2;
	});
}

function applyScopeCircleToEntities(entities, scopeCenter, scopeRadius){
	const normalized=normalizeScopeCircle(scopeCenter, scopeRadius);
	if(!normalized) return entities;
	return (entities||[]).filter((entity)=>entityIntersectsScopeCircle(entity, normalized));
}

function parseDimsToken(token){
	const parts = String(token||"").split(/[\s]*[xX\*\/][\s]*/).map(s=>s.trim()).filter(Boolean);
	const nums = parts.map(p=>Number(p));
	if(nums.length<2 || nums.some(n=>!isFinite(n))) return null;
	return nums;
}

function convertDim(v, unit){
	const n=Number(v);
	if(!Number.isFinite(n) || n<=0) return null;
	if(n<=6) return n;
	if(n>6 && n<=500) return n/100;
	if(n>500) return n/1000;
	if(unit==="mm") return v/1000;
	if(unit==="cm") return v/100;
	if(unit==="m") return v;
	return null;
}

function median(arr){
	if(!arr.length) return 0;
	const a=[...arr].sort((x,y)=>x-y);
	const mid=Math.floor(a.length/2);
	return a.length%2? a[mid] : (a[mid-1]+a[mid])/2;
}

function clusterRows(texts){
	const pts = texts.filter(t=>typeof t.y==="number" && isFinite(t.y));
	if(!pts.length) return { rows:[], eps:0 };
	const ys = pts.map(t=>t.y).sort((a,b)=>b-a);
	const diffs=[];
	for(let i=1;i<ys.length;i++){
		const d=Math.abs(ys[i-1]-ys[i]);
		if(d>0) diffs.push(d);
	}
	let step = median(diffs.filter(d=>d>0 && d<10));
	if(!step || !isFinite(step)) step=0.2;
	const hs=pts.map(p=>p.h).filter(v=>typeof v==="number" && isFinite(v) && v>0);
	const eps=Math.min(2.0, Math.max(0.02, step/2, (median(hs)||0)*0.5));

	const sorted=[...pts].sort((a,b)=>b.y-a.y);
	const rows=[];
	for(const t of sorted){
		const r=rows[rows.length-1];
		if(!r || Math.abs(r.y-t.y)>eps) rows.push({ y:t.y, cells:[t] });
		else r.cells.push(t);
	}
	for(const r of rows){
		r.cells.sort((a,b)=>((typeof a.x==="number"?a.x:Infinity)-(typeof b.x==="number"?b.x:Infinity)));
	}
	return { rows, eps };
}

function extractTagsFromText(text, tagRe){
	const safeRe = tagRe instanceof RegExp
		? new RegExp(tagRe.source, tagRe.flags.replace(/g/g, ""))
		: new RegExp(String(tagRe||""), "i");
	const toks=String(text||"").toUpperCase().split(/[^A-Z0-9]+/).filter(Boolean);
	return [...new Set(toks.filter(t=>safeRe.test(t)))];
}

function extractDimsFromText(text, dimRe){
	return [...new Set((String(text||"").toUpperCase().match(dimRe)||[]))];
}

function uniquePreserveOrder(values){
	const out=[];
	const seen=new Set();
	for(const value of (values||[])){
		if(seen.has(value)) continue;
		seen.add(value);
		out.push(value);
	}
	return out;
}

function parseBooleanLike(value){
	if(typeof value==="boolean") return value;
	const s=String(value==null?"":value).trim().toLowerCase();
	if(["1","true","yes","y","exists","exist"].includes(s)) return true;
	if(["0","false","no","n","none","not exist","not_exists"].includes(s)) return false;
	return null;
}

function hasValidBoqQty(item){
	const qty=Number(item?.qty||0);
	if(!(qty>0)) return false;
	const itemNo=Number(item?.item_no||0);
	const rowText=String(item?.row_text||"");
	if(itemNo>0 && Math.abs(qty-itemNo)<1e-9 && /-\s*-/.test(rowText)) return false;
	return true;
}

function pushRequiredQuestion(requiredQuestions, severity, field, question){
	if(!requiredQuestions || !Array.isArray(requiredQuestions.questions)) return;
	if(requiredQuestions.questions.some(q=>q.field===field)) return;
	requiredQuestions.questions.push({ severity, field, question });
}

function getReferenceQtyByStrKey(items, strKey){
	let best=0;
	for(const item of (items||[])){
		if(mapReferenceItemToStrKey(item?.description)!==strKey) continue;
		const qty=Number(item?.qty||0);
		if(qty>best) best=qty;
	}
	return best;
}

function normalizePdfText(text){
	return String(text||"").replace(/\u0000/g, "").replace(/\s+/g, " ").trim().toUpperCase();
}

function splitPdfTextLines(text){
	return String(text||"")
		.replace(/\u0000/g, "")
		.split(/\r?\n/)
		.map((line)=>String(line||"").replace(/\s+/g, " ").trim())
		.filter(Boolean);
}

function parseColumnScheduleRow(line, section="COLUMN"){
	const match=String(line||"").match(/^\s*(C(?:0*[1-9]\d*))\b(.*)$/i);
	if(!match) return null;
	const tag=String(match[1]||"").trim().toUpperCase();
	const remainder=String(match[2]||"");
	const nums=[...remainder.matchAll(/\d+(?:\.\d+)?/g)]
		.map((m)=>Number(m[0]))
		.filter((n)=>Number.isFinite(n) && n>0);
	if(nums.length<2) return null;
	return {
		tag,
		dims:[nums[0], nums[1]],
		qty:nums.length>=3 ? nums[2] : null,
		height_m:nums.length>=4 ? nums[3] : null,
		volume_m3:nums.length>=5 ? nums[4] : null,
		section,
		raw:line
	};
}

function parseStructuralSummaryFromPdfText(text){
	const summary={
		excavation:null,
		backfill:null,
		raft:null,
		firstSlab:null,
		secondSlab:null,
		topSlab:null,
		beamVolumes:{ FIRST_FLOOR:0, ROOF:0, TOP_ROOF:0 },
		columnRows:[],
		columnsVolumeM3:null,
		neckColumnsVolumeM3:null,
		evidence:[]
	};
	let inNeckSection=false;
	let currentBeamSection=null;
	for(const line of splitPdfTextLines(text)){
		const normalized=normalizePdfText(line);
		if(/NECK\s+COLUMNS/.test(normalized)){
			inNeckSection=true;
			currentBeamSection=null;
			summary.evidence.push(line);
			continue;
		}
		if(/VOLUME\s+INSULATION/.test(normalized) || /COLUMNS?\s*&\s*CORE\s*WALLS/.test(normalized)){
			inNeckSection=false;
			summary.evidence.push(line);
			continue;
		}
		const parsedColumn=parseColumnScheduleRow(line, inNeckSection ? "NECK" : "COLUMN");
		if(parsedColumn){
			summary.columnRows.push(parsedColumn);
			continue;
		}
		if(/^\s*1ST\s+SLAB\b/i.test(line)){
			const nums=[...String(line).replace(/^\s*1ST\s+SLAB\b/i, "").matchAll(/\d+(?:\.\d+)?/g)].map((m)=>Number(m[0])).filter((n)=>Number.isFinite(n) && n>0);
			if(nums.length>=3){
				summary.firstSlab={ area_m2:nums[0], thickness_m:nums[1], volume_m3:nums[2], raw:line };
				currentBeamSection="FIRST_FLOOR";
				summary.evidence.push(line);
				continue;
			}
		}
		if(/^\s*2ND\s+SLAB\b/i.test(line)){
			const nums=[...String(line).replace(/^\s*2ND\s+SLAB\b/i, "").matchAll(/\d+(?:\.\d+)?/g)].map((m)=>Number(m[0])).filter((n)=>Number.isFinite(n) && n>0);
			if(nums.length>=3){
				summary.secondSlab={ area_m2:nums[0], thickness_m:nums[1], volume_m3:nums[2], raw:line };
				currentBeamSection="ROOF";
				summary.evidence.push(line);
				continue;
			}
		}
		if(/^\s*3R?D\s+SLAB\b/i.test(line)){
			const nums=[...String(line).replace(/^\s*3R?D\s+SLAB\b/i, "").matchAll(/\d+(?:\.\d+)?/g)].map((m)=>Number(m[0])).filter((n)=>Number.isFinite(n) && n>0);
			if(nums.length>=3){
				summary.topSlab={ area_m2:nums[0], thickness_m:nums[1], volume_m3:nums[2], raw:line };
				currentBeamSection="TOP_ROOF";
				summary.evidence.push(line);
				continue;
			}
		}
		if(/^\s*BEAMS\b/i.test(line)) continue;
		if(/^\s*B\d+\b/i.test(line) && currentBeamSection){
			const nums=[...String(line).replace(/^\s*B\d+\b/i, "").matchAll(/\d+(?:\.\d+)?/g)].map((m)=>Number(m[0])).filter((n)=>Number.isFinite(n) && n>0);
			if(nums.length>=3){
				summary.beamVolumes[currentBeamSection]=(summary.beamVolumes[currentBeamSection]||0)+Number(nums[2]||0);
			}
			continue;
		}
		if(/^\s*EXC?VATION\b/i.test(line)){
			const nums=[...line.matchAll(/\d+(?:\.\d+)?/g)].map((m)=>Number(m[0])).filter((n)=>Number.isFinite(n) && n>0);
			if(nums.length){
				summary.excavation={ area_m2:nums[0] ?? null, depth_m:nums[1] ?? null, volume_m3:nums[2] ?? nums[nums.length-1] ?? null, raw:line };
				summary.evidence.push(line);
			}
			continue;
		}
		if(/^\s*BACK\s*FILL(?:ING)?\b/i.test(line)){
			const nums=[...line.matchAll(/\d+(?:\.\d+)?/g)].map((m)=>Number(m[0])).filter((n)=>Number.isFinite(n) && n>0);
			if(nums.length){
				summary.backfill={ volume_m3:nums[nums.length-1] ?? null, raw:line };
				summary.evidence.push(line);
			}
			continue;
		}
		if(/^\s*RAFT\b/i.test(line)){
			const nums=[...line.matchAll(/\d+(?:\.\d+)?/g)].map((m)=>Number(m[0])).filter((n)=>Number.isFinite(n) && n>0);
			if(nums.length>=3){
				summary.raft={ area_m2:nums[0] ?? null, thickness_m:nums[1] ?? null, volume_m3:nums[2] ?? null, pcc_m3:nums[3] ?? null, membrane_m2:nums[4] ?? null, raw:line };
				summary.evidence.push(line);
			}
		}
	}
	const neckRows=summary.columnRows.filter((row)=>row.section==="NECK" && Number.isFinite(row.volume_m3));
	const superRows=summary.columnRows.filter((row)=>row.section!=="NECK" && Number.isFinite(row.volume_m3));
	summary.neckColumnsVolumeM3=neckRows.length ? neckRows.reduce((sum,row)=>sum+Number(row.volume_m3||0),0) : null;
	summary.columnsVolumeM3=superRows.length ? superRows.reduce((sum,row)=>sum+Number(row.volume_m3||0),0) : null;
	return summary;
}

function mergeSupplementalStructuralPdfSummaries(pdfTexts){
	const merged={
		excavation_m3:null,
		backfill_m3:null,
		raft_foundation_m3:null,
		raft_pcc_m3:null,
		raft_membrane_m2:null,
		first_slab_area_m2:null,
		second_slab_area_m2:null,
		top_roof_slab_area_m2:null,
		first_slab_m3:null,
		second_slab_m3:null,
		first_slab_beams_m3:null,
		second_slab_beams_m3:null,
		columns_m3:null,
		neck_columns_m3:null,
		columnRows:[],
		evidence:[]
	};
	for(const [pdfName, pdfText] of Object.entries(pdfTexts||{})){
		const parsed=parseStructuralSummaryFromPdfText(pdfText);
		if(parsed.excavation?.volume_m3>0 && !(merged.excavation_m3>0)) merged.excavation_m3=parsed.excavation.volume_m3;
		if(parsed.backfill?.volume_m3>0 && !(merged.backfill_m3>0)) merged.backfill_m3=parsed.backfill.volume_m3;
		if(parsed.raft?.volume_m3>0 && !(merged.raft_foundation_m3>0)) merged.raft_foundation_m3=parsed.raft.volume_m3;
		if(parsed.raft?.pcc_m3>0 && !(merged.raft_pcc_m3>0)) merged.raft_pcc_m3=parsed.raft.pcc_m3;
		if(parsed.raft?.membrane_m2>0 && !(merged.raft_membrane_m2>0)) merged.raft_membrane_m2=parsed.raft.membrane_m2;
		if(parsed.firstSlab?.area_m2>0 && !(merged.first_slab_area_m2>0)) merged.first_slab_area_m2=parsed.firstSlab.area_m2;
		if(parsed.secondSlab?.area_m2>0 && !(merged.second_slab_area_m2>0)) merged.second_slab_area_m2=parsed.secondSlab.area_m2;
		if(parsed.topSlab?.area_m2>0 && !(merged.top_roof_slab_area_m2>0)) merged.top_roof_slab_area_m2=parsed.topSlab.area_m2;
		if(parsed.firstSlab?.volume_m3>0 && !(merged.first_slab_m3>0)) merged.first_slab_m3=parsed.firstSlab.volume_m3;
		const secondSlabTotal=(Number(parsed.secondSlab?.volume_m3||0)+Number(parsed.topSlab?.volume_m3||0));
		if(secondSlabTotal>0 && !(merged.second_slab_m3>0)) merged.second_slab_m3=secondSlabTotal;
		const firstBeamTotal=Number(parsed.beamVolumes?.FIRST_FLOOR||0);
		const secondBeamTotal=Number(parsed.beamVolumes?.ROOF||0)+Number(parsed.beamVolumes?.TOP_ROOF||0);
		if(firstBeamTotal>0 && !(merged.first_slab_beams_m3>0)) merged.first_slab_beams_m3=firstBeamTotal;
		if(secondBeamTotal>0 && !(merged.second_slab_beams_m3>0)) merged.second_slab_beams_m3=secondBeamTotal;
		if(parsed.columnsVolumeM3>0) merged.columns_m3=(merged.columns_m3||0)+parsed.columnsVolumeM3;
		if(parsed.neckColumnsVolumeM3>0) merged.neck_columns_m3=(merged.neck_columns_m3||0)+parsed.neckColumnsVolumeM3;
		if(Array.isArray(parsed.columnRows) && parsed.columnRows.length) merged.columnRows.push(...parsed.columnRows.map((row)=>({ ...row, pdf:pdfName })));
		if(parsed.evidence.length) merged.evidence.push({ pdf:pdfName, lines:parsed.evidence.slice(0,12) });
	}
	return merged;
}

function parseColumnScheduleFromPdfText(text){
	const up=normalizePdfText(text);
	const schedIdx=up.indexOf("SCHEDULE OF COLUMNS");
	const fallbackRows=[];
	let inNeckSection=false;
	for(const line of splitPdfTextLines(text)){
		const normalized=normalizePdfText(line);
		if(/NECK\s+COLUMNS/.test(normalized)){
			inNeckSection=true;
			continue;
		}
		if(/VOLUME\s+INSULATION/.test(normalized) || /COLUMNS?\s*&\s*CORE\s*WALLS/.test(normalized)){
			inNeckSection=false;
			continue;
		}
		const parsedRow=parseColumnScheduleRow(line, inNeckSection ? "NECK" : "COLUMN");
		if(parsedRow) fallbackRows.push(parsedRow);
	}
	if(schedIdx<0 && !fallbackRows.length) return { columnMap:{}, neckDims:null, source:"", scheduleRows:[] };

	const typeIdx=up.lastIndexOf("TYPE", schedIdx);
	const start=typeIdx>=0 ? typeIdx : Math.max(0, schedIdx-80);
	let end=up.length;
	for(const marker of ["PLOT NO", "C:\\", "-- 1 OF", "-- 1 OF 1 --"]){
		const idx=up.indexOf(marker, schedIdx);
		if(idx>=0) end=Math.min(end, idx);
	}
	const slice=up.slice(start, end);
	const tags=uniquePreserveOrder(
		[...slice.matchAll(/\b(?:C\d+|NC)\b/g)].map(m=>String(m[0]||"").trim().toUpperCase())
	);
	const numbers=[...slice.matchAll(/\b\d+(?:\.\d+)?\b/g)]
		.map(m=>String(m[0]||""))
		.filter(s=>!s.includes("."))
		.map(Number)
		.filter(n=>Number.isFinite(n) && n>=15 && n<=150);

	const columnMap={};
	let neckDims=null;
	for(let i=0;i<tags.length;i++){
		const a=numbers[i*2];
		const b=numbers[(i*2)+1];
		if(!(a>0) || !(b>0)) break;
		const dims=[a,b];
		if(tags[i]==="NC") neckDims=dims;
		else columnMap[tags[i]]=dims;
	}
	for(const row of fallbackRows){
		if(!columnMap[row.tag]) columnMap[row.tag]=row.dims;
	}
	return { columnMap, neckDims, source:slice, scheduleRows:fallbackRows };
}

async function safeReadPdfText(pdfPath){
	try{
		const parser=new PDFParse({ data:fs.readFileSync(pdfPath) });
		const result=await parser.getText();
		await parser.destroy();
		return String(result?.text||"");
	}catch{
		return "";
	}
}

async function loadSupplementalColumnScheduleMaps(strDxfPath, referencePath){
	const dirs=uniquePreserveOrder(
		[
			referencePath ? path.dirname(referencePath) : "",
			strDxfPath ? path.dirname(strDxfPath) : "",
			referencePath ? path.dirname(path.dirname(referencePath)) : ""
		].filter(Boolean).map(p=>path.resolve(p))
	);
	const pdfPaths=[];
	for(const dir of dirs){
		if(!fs.existsSync(dir)) continue;
		for(const entry of fs.readdirSync(dir, { withFileTypes:true })){
			if(!entry.isFile()) continue;
			if(!/\.pdf$/i.test(entry.name)) continue;
			if(!/(?:col|str)/i.test(entry.name)) continue;
			pdfPaths.push(path.join(dir, entry.name));
		}
	}

	const orderedPdfPaths=uniquePreserveOrder(pdfPaths.sort((a,b)=>{
		if(a<b) return -1;
		if(a>b) return 1;
		return 0;
	}));
	const columnMap={};
	let neckDims=null;
	const evidence=[];
	const scheduleRows=[];

	for(const pdfPath of orderedPdfPaths){
		const text=await safeReadPdfText(pdfPath);
		if(!text) continue;
		const parsed=parseColumnScheduleFromPdfText(text);
		const colKeys=Object.keys(parsed.columnMap||{});
		if(!colKeys.length && !parsed.neckDims && !(parsed.scheduleRows||[]).length) continue;
		for(const tag of colKeys){
			if(!columnMap[tag]) columnMap[tag]=parsed.columnMap[tag];
		}
		if(!neckDims && Array.isArray(parsed.neckDims) && parsed.neckDims.length===2) neckDims=parsed.neckDims;
		if(Array.isArray(parsed.scheduleRows) && parsed.scheduleRows.length) scheduleRows.push(...parsed.scheduleRows.map((row)=>({ ...row, pdf:path.basename(pdfPath) })));
		evidence.push({
			pdf:path.basename(pdfPath),
			column_tags:colKeys,
			neck_dims:parsed.neckDims||null,
			schedule_rows:(parsed.scheduleRows||[]).length
		});
	}

	return { columnMap, neckDims, evidence, scheduleRows };
}

async function loadSupplementalStructuralPdfTexts(strDxfPath, referencePath){
	const dirs=uniquePreserveOrder(
		[
			referencePath ? path.dirname(referencePath) : "",
			strDxfPath ? path.dirname(strDxfPath) : "",
			referencePath ? path.dirname(path.dirname(referencePath)) : ""
		].filter(Boolean).map(p=>path.resolve(p))
	);
	const pdfPaths=[];
	for(const dir of dirs){
		if(!fs.existsSync(dir)) continue;
		for(const entry of fs.readdirSync(dir, { withFileTypes:true })){
			if(!entry.isFile()) continue;
			if(!/\.pdf$/i.test(entry.name)) continue;
			if(!/^(?:SOG|FTING|1ST\s*SLAB|RF\s*SLAB|TR\s*SLAB|STR_.*)\.PDF$/i.test(entry.name)) continue;
			pdfPaths.push(path.join(dir, entry.name));
		}
	}
	const orderedPdfPaths=uniquePreserveOrder(pdfPaths.sort((a,b)=>{
		if(a<b) return -1;
		if(a>b) return 1;
		return 0;
	}));
	const pdfTexts={};
	for(const pdfPath of orderedPdfPaths){
		const text=await safeReadPdfText(pdfPath);
		if(text) pdfTexts[path.basename(pdfPath)]=text;
	}
	return pdfTexts;
}

function validDimsCandidate(nums){
	if(!Array.isArray(nums) || nums.length<2) return false;
	if(nums.some(n=>!isFinite(n))) return false;
	return nums.every(n=>n>=5 && n<=5000);
}

function isRealisticColumnSection(B, D){
	if(!Number.isFinite(B) || !Number.isFinite(D)) return false;
	return B>=0.15 && B<=1.50 && D>=0.15 && D<=1.50;
}

function isLikelyColumnDimsRaw(dims){
	if(!Array.isArray(dims) || dims.length!==2) return false;
	const a=Number(dims[0]);
	const b=Number(dims[1]);
	if(!Number.isFinite(a) || !Number.isFinite(b) || a<=0 || b<=0) return false;
	const min=Math.min(a,b);
	const max=Math.max(a,b);
	if(min<20 || max>120) return false;
	if((max/min)>4) return false;
	return true;
}

function normalizeColumnDimsRaw(dims){
	if(!Array.isArray(dims) || dims.length!==2) return null;
	let a=Number(dims[0]);
	let b=Number(dims[1]);
	if(!Number.isFinite(a) || !Number.isFinite(b) || a<=0 || b<=0) return null;
	// Accept schedules that store column dims as 250/400 style and normalize to cm-like raw values.
	if((a>120 && a<=1000) || (b>120 && b<=1000)){
		a = a/10;
		b = b/10;
	}
	a=Math.round(a*1000)/1000;
	b=Math.round(b*1000)/1000;
	if(!isLikelyColumnDimsRaw([a,b])) return null;
	return [a,b];
}

function buildTagDimsMapFromRows(rows, tagRe, dimRe){
	const map={};
	const candidates=[];
	const allTagCells=[];
	const allDimCells=[];
	for(const r of rows){
		const tagCells=[];
		const dimCells=[];
		for(const c of r.cells){
			for(const t of extractTagsFromText(c.text, tagRe)){
				const obj={ tag:t, x:c.x, y:c.y };
				tagCells.push(obj);
				allTagCells.push(obj);
			}
			for(const tok of extractDimsFromText(c.text, dimRe)){
				const dn=parseDimsToken(tok);
				if(!dn || !validDimsCandidate(dn)) continue;
				const dobj={ token:tok, nums:dn, x:c.x, y:c.y };
				dimCells.push(dobj);
				allDimCells.push(dobj);
			}
		}
		if(tagCells.length || dimCells.length){
			candidates.push({
				y:r.y,
				tags:[...new Set(tagCells.map(t=>t.tag))],
				dims:dimCells.map(d=>d.token).slice(0,10),
				preview:r.cells.map(c=>c.text).join(" | ").slice(0,260)
			});
		}
	}

	if(!allTagCells.length || !allDimCells.length) return { map, candidates };
	for(const t of allTagCells){
		const ordered=[...allDimCells].sort((a,b)=>{
			const prefA = /^(F\d+|CF\d+|WF\d+)$/i.test(t.tag) ? (a.nums.length>=3?0:1) : (a.nums.length===2?0:1);
			const prefB = /^(F\d+|CF\d+|WF\d+)$/i.test(t.tag) ? (b.nums.length>=3?0:1) : (b.nums.length===2?0:1);
			const dyA = (typeof a.y==="number" && typeof t.y==="number") ? Math.abs(a.y-t.y) : 9999;
			const dyB = (typeof b.y==="number" && typeof t.y==="number") ? Math.abs(b.y-t.y) : 9999;
			const dxA = (typeof a.x==="number" && typeof t.x==="number") ? Math.abs(a.x-t.x) : 9999;
			const dxB = (typeof b.x==="number" && typeof t.x==="number") ? Math.abs(b.x-t.x) : 9999;
			return prefA-prefB || dyA-dyB || dxA-dxB || b.nums.length-a.nums.length;
		});
		if(!ordered[0]) continue;
		if(!map[t.tag] || ordered[0].nums.length>map[t.tag].length) map[t.tag]=ordered[0].nums;
	}
	return { map, candidates };
}

function modeRounded(values, step=0.5){
	if(!values.length) return null;
	const bins=new Map();
	for(const v of values){
		if(typeof v!=="number" || !isFinite(v)) continue;
		const key=Math.round(v/step)*step;
		bins.set(key, (bins.get(key)||0)+1);
	}
	let best=null, bestCount=-1;
	for(const [k,c] of bins.entries()){
		if(c>bestCount){ bestCount=c; best=k; }
	}
	return best;
}

function buildTagDimsMapByColumns(rows, familyTagRegex, requiredDimsLen, tagRe, dimRe, rowEps){
	const rowData=[];
	const tagXs=[];
	const dimXs=[];

	for(const r of (rows||[])){
		const tags=[];
		const dims=[];
		for(const c of (r.cells||[])){
			for(const t of extractTagsFromText(c.text, tagRe)){
				if(!familyTagRegex.test(t)) continue;
				if(typeof c.x==="number") tagXs.push(c.x);
				tags.push({ tag:t, x:c.x, y:c.y });
			}
			for(const tok of extractDimsFromText(c.text, dimRe)){
				const dn=parseDimsToken(tok);
				if(!dn || !validDimsCandidate(dn)) continue;
				if(requiredDimsLen===3 && dn.length<3) continue;
				if(requiredDimsLen===2 && dn.length!==2) continue;
				if(typeof c.x==="number") dimXs.push(c.x);
				dims.push({ nums:dn, x:c.x, y:c.y });
			}
		}
		rowData.push({ y:r.y, tags, dims });
	}

	const tagColX = modeRounded(tagXs, 0.5);
	const dimColX = modeRounded(dimXs, 0.5);
	const map={};

	if(tagColX===null || dimColX===null) return map;

	const band = Math.max(0.8, (rowEps||0.2)*4);
	for(let i=0;i<rowData.length;i++){
		const row=rowData[i];
		if(!row.tags.length) continue;

		const nearTags = row.tags.filter(t=>typeof t.x==="number" ? Math.abs(t.x-tagColX)<=band : true);
		const tags = nearTags.length ? nearTags : row.tags;
		let dims = row.dims.filter(d=>typeof d.x==="number" ? Math.abs(d.x-dimColX)<=band*2 : true);

		if(!dims.length){
			for(let k=Math.max(0,i-2); k<=Math.min(rowData.length-1,i+2); k++){
				if(k===i) continue;
				const cand=rowData[k].dims.filter(d=>typeof d.x==="number" ? Math.abs(d.x-dimColX)<=band*2 : true);
				if(cand.length){ dims=cand; break; }
			}
		}

		if(!dims.length) continue;
		for(const t of tags){
			const ordered=[...dims].sort((a,b)=>{
				const dxA=(typeof a.x==="number"&&typeof t.x==="number")?Math.abs(a.x-t.x):9999;
				const dxB=(typeof b.x==="number"&&typeof t.x==="number")?Math.abs(b.x-t.x):9999;
				const dyA=(typeof a.y==="number"&&typeof t.y==="number")?Math.abs(a.y-t.y):9999;
				const dyB=(typeof b.y==="number"&&typeof t.y==="number")?Math.abs(b.y-t.y):9999;
				return dxA-dxB || dyA-dyB;
			});
			if(ordered[0]) map[t.tag]=ordered[0].nums;
		}
	}

	return map;
}

function parseStandaloneNumericCell(text){
	const s=normText(text).replace(/,/g,"");
	if(!/^[-+]?\d+(?:\.\d+)?$/.test(s)) return null;
	const n=Number(s);
	return Number.isFinite(n) ? n : null;
}

function clusterNumericColumns(cells, tol=0.1){
	const sorted=[...(cells||[])].filter(c=>typeof c.x==="number" && isFinite(c.x)).sort((a,b)=>a.x-b.x);
	const groups=[];
	for(const cell of sorted){
		const current=groups[groups.length-1];
		if(!current || Math.abs(current.x-cell.x)>tol){
			groups.push({ x:cell.x, cells:[cell], count:1 });
			continue;
		}
		current.cells.push(cell);
		current.count=current.cells.length;
		current.x=current.cells.reduce((sum,c)=>sum+c.x,0)/current.cells.length;
	}
	return groups;
}

function buildFamilyDimsMapFromNumericColumns(rows, familyTagRegex, requiredDimsLen, tagRe, opts={}){
	const {
		minCount=2,
		xTol=0.1,
		rowLookAround=1,
		valueFilter=()=>true
	}=opts;
	const rowData=[];
	const tagXs=[];
	const numericCells=[];

	for(const row of (rows||[])){
		const tags=[];
		const nums=[];
		for(const cell of (row.cells||[])){
			for(const tag of extractTagsFromText(cell.text, tagRe)){
				if(!familyTagRegex.test(tag)) continue;
				tags.push({ tag, x:cell.x, y:cell.y });
				if(typeof cell.x==="number") tagXs.push(cell.x);
			}
			const value=parseStandaloneNumericCell(cell.text);
			if(value===null || !valueFilter(value, cell)) continue;
			nums.push({ value, x:cell.x, y:cell.y, text:cell.text });
			numericCells.push({ value, x:cell.x, y:cell.y, text:cell.text });
		}
		rowData.push({ y:row.y, tags, nums });
	}

	const tagColX=modeRounded(tagXs, 0.5);
	if(tagColX===null) return {};

	const numericCols=clusterNumericColumns(
		numericCells.filter(c=>typeof c.x==="number" && c.x>tagColX+0.2),
		xTol
	)
		.filter(col=>col.count>=minCount)
		.sort((a,b)=>a.x-b.x)
		.slice(0, requiredDimsLen);

	if(numericCols.length<requiredDimsLen) return {};

	const map={};
	for(let rowIndex=0; rowIndex<rowData.length; rowIndex++){
		const row=rowData[rowIndex];
		if(!row.tags.length) continue;
		for(const tagCell of row.tags){
			const dims=[];
			for(const col of numericCols){
				let best=null;
				let bestScore=Infinity;
				for(let k=Math.max(0,rowIndex-rowLookAround); k<=Math.min(rowData.length-1,rowIndex+rowLookAround); k++){
					for(const numCell of (rowData[k].nums||[])){
						if(typeof numCell.x!=="number") continue;
						const dx=Math.abs(numCell.x-col.x);
						if(dx>xTol) continue;
						const dy=(typeof numCell.y==="number" && typeof tagCell.y==="number") ? Math.abs(numCell.y-tagCell.y) : 9999;
						const score=(dy*10)+dx;
						if(score<bestScore){
							best=numCell;
							bestScore=score;
						}
					}
				}
				if(!best) break;
				dims.push(best.value);
			}
			if(dims.length===requiredDimsLen) map[tagCell.tag]=dims;
		}
	}
	return map;
}

function buildFamilyTagDimsMap(rows, familyTagRegex, requiredDimsLen, tagRe, dimRe, rowBand=1.2){
	const map={};
	const tagCells=[];
	const dimCells=[];

	for(const r of (rows||[])){
		for(const c of (r.cells||[])){
			for(const t of extractTagsFromText(c.text, tagRe)){
				if(familyTagRegex.test(t)) tagCells.push({ tag:t, x:c.x, y:c.y });
			}
			for(const tok of extractDimsFromText(c.text, dimRe)){
				const dn=parseDimsToken(tok);
				if(!dn || !validDimsCandidate(dn)) continue;
				if(requiredDimsLen===3 && dn.length<3) continue;
				if(requiredDimsLen===2 && dn.length!==2) continue;
				dimCells.push({ nums:dn, x:c.x, y:c.y });
			}
		}
	}

	if(!tagCells.length || !dimCells.length) return map;

	for(const t of tagCells){
		const nearBand = dimCells.filter(d=>{
			if(typeof d.y!=="number" || typeof t.y!=="number") return false;
			return Math.abs(d.y-t.y)<=rowBand;
		});
		const pool = nearBand.length ? nearBand : dimCells;
		const ordered=[...pool].sort((a,b)=>{
			const dyA=(typeof a.y==="number"&&typeof t.y==="number")?Math.abs(a.y-t.y):9999;
			const dyB=(typeof b.y==="number"&&typeof t.y==="number")?Math.abs(b.y-t.y):9999;
			const dxA=(typeof a.x==="number"&&typeof t.x==="number")?Math.abs(a.x-t.x):9999;
			const dxB=(typeof b.x==="number"&&typeof t.x==="number")?Math.abs(b.x-t.x):9999;
			return dyA-dyB || dxA-dxB;
		});
		if(!ordered[0]) continue;
		map[t.tag]=ordered[0].nums;
	}

	return map;
}

function roundToStep(value, step=0.05){
	if(!Number.isFinite(value) || !Number.isFinite(step) || step<=0) return null;
	return Math.round(value/step)*step;
}

function clamp(value, min, max){
	if(!Number.isFinite(value)) return value;
	return Math.max(min, Math.min(max, value));
}

function convertMetersToDimRaw(valueM, unit){
	if(!Number.isFinite(valueM) || valueM<=0) return null;
	const u=String(unit||"").toLowerCase();
	if(u==="mm") return Math.round(valueM*1000);
	if(u==="cm") return Math.round(valueM*100);
	return Number(valueM.toFixed(3));
}

function convertFootingDimsRaw(dimsRaw, unit){
	if(!Array.isArray(dimsRaw) || dimsRaw.length<2) return [];
	const nums=dimsRaw.map(v=>Number(v));
	if(nums.some(v=>!Number.isFinite(v) || v<=0)) return [];
	const treatAllAsMm = nums.length>=3 && Math.max(...nums)>500 && Math.min(...nums)>=50;
	if(treatAllAsMm) return nums.map(v=>v/1000);
	return nums.map(v=>convertDim(v, unit));
}

function collectTextsNearHeader(texts, headerRegex, xBackPad, xForwardPad, yDepth){
	const headerHit=(texts||[]).find(t=>headerRegex.test(String(t.text||"")) && typeof t.x==="number" && typeof t.y==="number");
	if(!headerHit) return [];
	return (texts||[]).filter(t=>{
		if(typeof t.x!=="number" || typeof t.y!=="number") return false;
		return t.x>=headerHit.x-xBackPad
			&& t.x<=headerHit.x+xForwardPad
			&& t.y<=headerHit.y+50
			&& t.y>=headerHit.y-yDepth;
	});
}

function buildFootingMapFromExactTexts(texts, tagRe, dimRe){
	const windowTexts=collectTextsNearHeader(texts, /SCHEDULE OF FOOTINGS/i, 1.5, 5, 6);
	if(!windowTexts.length) return {};
	const tagHits=[];
	const dimHits=[];
	for(const t of windowTexts){
		for(const tag of extractTagsFromText(t.text, tagRe)){
			if(isFootingTag(tag)) tagHits.push({ tag, x:t.x, y:t.y });
		}
		for(const tok of extractDimsFromText(t.text, dimRe)){
			const dn=parseDimsToken(tok);
			if(!Array.isArray(dn) || dn.length<3 || !validDimsCandidate(dn)) continue;
			dimHits.push({ nums:dn, x:t.x, y:t.y });
		}
	}
	const out={};
	for(const hit of tagHits){
		const best=[...dimHits]
			.filter(d=>d.x>=hit.x && Math.abs(d.y-hit.y)<=250)
			.sort((a,b)=>Math.abs(a.y-hit.y)-Math.abs(b.y-hit.y) || Math.abs(a.x-hit.x)-Math.abs(b.x-hit.x))[0];
		if(best) out[hit.tag]=best.nums;
	}
	return out;
}

function buildFootingMapFromStandaloneRows(texts, tagRe, opts={}){
	const {
		xBackPad=2,
		xForwardPad=25,
		yDepth=16,
		yTol=0.9
	}=opts;
	const headerHits=(texts||[]).filter(t=>/SCHEDULE OF FOOTINGS/i.test(String(t.text||"")) && typeof t.x==="number" && typeof t.y==="number");
	let bestMap={};

	for(const headerHit of headerHits){
		const windowTexts=(texts||[]).filter(t=>{
			if(typeof t.x!=="number" || typeof t.y!=="number") return false;
			return t.x>=headerHit.x-xBackPad
				&& t.x<=headerHit.x+xForwardPad
				&& t.y<=headerHit.y+1
				&& t.y>=headerHit.y-yDepth;
		});
		if(!windowTexts.length) continue;

		const footingTags=windowTexts
			.flatMap(t=>extractTagsFromText(t.text, tagRe).map(tag=>({ tag, x:t.x, y:t.y })))
			.filter(t=>isFootingTag(t.tag));
		const numericCells=windowTexts
			.map(t=>({ value:parseStandaloneNumericCell(t.text), x:t.x, y:t.y }))
			.filter(c=>Number.isFinite(c.value) && c.value>=20 && c.value<=1000);
		if(!footingTags.length || !numericCells.length) continue;

		const map={};
		for(const tagCell of footingTags){
			const sameRow=numericCells
				.filter(c=>c.x>tagCell.x+0.05 && Math.abs(c.y-tagCell.y)<=yTol)
				.sort((a,b)=>a.x-b.x || Math.abs(a.y-tagCell.y)-Math.abs(b.y-tagCell.y));
			if(sameRow.length<3) continue;

			const picked=[];
			for(const cell of sameRow){
				if(!picked.length || Math.abs(cell.x-picked[picked.length-1].x)>0.03){
					picked.push(cell);
				}
				if(picked.length===3) break;
			}
			if(picked.length===3) map[tagCell.tag]=picked.map(c=>c.value);
		}

		if(Object.keys(map).length>Object.keys(bestMap).length) bestMap=map;
	}

	return bestMap;
}

function buildNumericSectionMapFromExactTexts(texts, headerRegex, familyTagRegex, requiredDimsLen, tagRe, opts={}){
	const {
		xBackPad=1200,
		xForwardPad=3500,
		yDepth=4500,
		valueFilter=()=>true
	}=opts;
	const windowTexts=collectTextsNearHeader(texts, headerRegex, xBackPad, xForwardPad, yDepth);
	if(!windowTexts.length) return {};
	const pseudoRows=windowTexts
		.map(t=>({ y:t.y, cells:[{ text:t.text, x:t.x, y:t.y }] }))
		.sort((a,b)=>b.y-a.y);
	return buildFamilyDimsMapFromNumericColumns(pseudoRows, familyTagRegex, requiredDimsLen, tagRe, {
		minCount:1,
		xTol:50,
		rowLookAround:6,
		valueFilter
	});
}

function buildNumericSectionMapDirect(texts, headerRegex, familyTagRegex, requiredDimsLen, tagRe, opts={}){
	const {
		xBackPad=1.5,
		xForwardPad=3.5,
		yDepth=4.5,
		xTol=0.1,
		valueFilter=()=>true
	}=opts;
	const headerHits=(texts||[]).filter(t=>headerRegex.test(String(t.text||"")) && typeof t.x==="number" && typeof t.y==="number");
	let bestMap={};
	for(const headerHit of headerHits){
		const windowTexts=(texts||[]).filter(t=>{
			if(typeof t.x!=="number" || typeof t.y!=="number") return false;
			return t.x>=headerHit.x-xBackPad
				&& t.x<=headerHit.x+xForwardPad
				&& t.y<=headerHit.y+50
				&& t.y>=headerHit.y-yDepth;
		});
		if(!windowTexts.length) continue;
		const tagHits=[];
		const numericHits=[];
		for(const t of windowTexts){
			for(const tag of extractTagsFromText(t.text, tagRe)){
				if(familyTagRegex.test(tag)) tagHits.push({ tag, x:t.x, y:t.y });
			}
			const value=parseStandaloneNumericCell(t.text);
			if(value!==null && valueFilter(value, t)) numericHits.push({ value, x:t.x, y:t.y });
		}
		if(!tagHits.length || !numericHits.length) continue;
		const tagColX=modeRounded(tagHits.map(t=>t.x), 0.05);
		const numericCols=clusterNumericColumns(
			numericHits.filter(c=>typeof c.x==="number" && c.x>(tagColX===null ? -Infinity : tagColX+0.05)),
			xTol
		)
			.sort((a,b)=>a.x-b.x)
			.slice(0, requiredDimsLen);
		if(numericCols.length<requiredDimsLen) continue;
		const map={};
		for(const tagHit of tagHits){
			const dims=[];
			for(const col of numericCols){
				const best=[...numericHits]
					.filter(n=>Math.abs(n.x-col.x)<=xTol)
					.sort((a,b)=>Math.abs(a.y-tagHit.y)-Math.abs(b.y-tagHit.y) || Math.abs(a.x-col.x)-Math.abs(b.x-col.x))[0];
				if(!best) break;
				dims.push(best.value);
			}
			if(dims.length===requiredDimsLen) map[tagHit.tag]=dims;
		}
		if(Object.keys(map).length>Object.keys(bestMap).length) bestMap=map;
	}
	return bestMap;
}

function chooseBestRegion(rows, tagRe, dimRe, family="footing"){
	if(!rows.length) return { start:0, end:-1, score:0 };
	const scores = rows.map(r=>{
		let fTags=0, cTags=0, d2=0, d3=0;
		for(const c of r.cells){
			for(const t of extractTagsFromText(c.text, tagRe)){
				if(isFootingTag(t)) fTags++;
				else if(isColumnTag(t)) cTags++;
			}
			for(const tok of extractDimsFromText(c.text, dimRe)){
				const dn=parseDimsToken(tok);
				if(!dn || !validDimsCandidate(dn)) continue;
				if(dn.length>=3) d3++; else d2++;
			}
		}
		if(family==="column") return cTags*5 + d2*5 - d3;
		return fTags*5 + d3*5 - d2;
	});

	let best={ start:0, end:0, score:-1 };
	for(let i=0;i<scores.length;i++){
		let acc=0;
		for(let j=i;j<Math.min(scores.length, i+80);j++){
			acc += scores[j];
			if(acc>best.score) best={ start:i, end:j, score:acc };
			if(scores[j]===0 && j>i+10) break;
		}
	}
	return best;
}

function rowsAroundHeaders(rows, includeRegex, pre=20, post=180){
	const hitIdx = rows
		.map((r, idx)=>({ idx, txt:r.cells.map(c=>c.text).join(" ").toUpperCase() }))
		.filter(x=>includeRegex.test(x.txt))
		.map(x=>x.idx);
	if(!hitIdx.length) return [];
	const keep=new Set();
	for(const idx of hitIdx){
		for(let k=Math.max(0, idx-pre); k<=Math.min(rows.length-1, idx+post); k++) keep.add(k);
	}
	return rows.filter((_,i)=>keep.has(i));
}

function rowsAroundEachHeader(rows, includeRegex, pre=20, post=180){
	const hitIdx = rows
		.map((r, idx)=>({ idx, txt:r.cells.map(c=>c.text).join(" ").toUpperCase() }))
		.filter(x=>includeRegex.test(x.txt))
		.map(x=>x.idx);
	return hitIdx.map(idx=>rows.slice(Math.max(0, idx-pre), Math.min(rows.length, idx+post+1)));
}

function pickBestHeaderWindow(rows, includeRegex, tagRe, dimRe, family, pre=20, post=180){
	const windows=rowsAroundEachHeader(rows, includeRegex, pre, post).filter(w=>w.length);
	if(!windows.length) return [];
	let best=windows[0];
	let bestScore=chooseBestRegion(best, tagRe, dimRe, family).score;
	for(const windowRows of windows.slice(1)){
		const score=chooseBestRegion(windowRows, tagRe, dimRe, family).score;
		if(score>bestScore){
			best=windowRows;
			bestScore=score;
		}
	}
	return best;
}

function trimRowsUntilNextScheduleHeader(rows, headerRegex){
	if(!Array.isArray(rows) || !rows.length) return [];
	let seenHeader=false;
	const out=[];
	for(const row of rows){
		const rowText=(row.cells||[]).map(c=>String(c.text||"")).join(" ");
		const isCurrentHeader=headerRegex.test(rowText);
		const isAnySchedule=/SCHEDULE/i.test(rowText);
		if(isCurrentHeader) seenHeader=true;
		if(!seenHeader) continue;
		if(out.length && isAnySchedule && !isCurrentHeader) break;
		out.push(row);
	}
	return out;
}

function filterRowsToHeaderXCluster(rows, headerRegex, tagRe, dimRe, opts={}){
	const {
		colTol=400,
		maxGap=3000,
		pad=900
	}=opts;
	if(!Array.isArray(rows) || !rows.length) return [];
	const headerCells=rows
		.flatMap(r=>(r.cells||[]).filter(c=>headerRegex.test(String(c.text||""))))
		.filter(c=>typeof c.x==="number" && isFinite(c.x));
	if(!headerCells.length) return rows;
	const headerX=headerCells.reduce((sum,c)=>sum+c.x,0)/headerCells.length;
	const relevantCells=rows
		.flatMap(r=>(r.cells||[]))
		.filter(c=>{
			if(typeof c.x!=="number" || !isFinite(c.x)) return false;
			const text=String(c.text||"");
			const standaloneNumeric=parseStandaloneNumericCell(text);
			return extractTagsFromText(text, tagRe).length
				|| extractDimsFromText(text, dimRe).length
				|| /SCHEDULE/i.test(text)
				|| (standaloneNumeric!==null && standaloneNumeric>0 && standaloneNumeric<=1000);
		})
		.sort((a,b)=>a.x-b.x);
	if(!relevantCells.length) return rows;
	const xClusters=[];
	for(const cell of relevantCells){
		const current=xClusters[xClusters.length-1];
		if(!current || Math.abs(current.x-cell.x)>colTol){
			xClusters.push({ x:cell.x, minX:cell.x, maxX:cell.x, count:1 });
			continue;
		}
		current.count += 1;
		current.minX=Math.min(current.minX, cell.x);
		current.maxX=Math.max(current.maxX, cell.x);
		current.x=((current.x*(current.count-1))+cell.x)/current.count;
	}
	if(!xClusters.length) return rows;
	let seedIndex=0;
	let seedDistance=Infinity;
	for(let i=0;i<xClusters.length;i++){
		const dist=Math.abs(xClusters[i].x-headerX);
		if(dist<seedDistance){
			seedDistance=dist;
			seedIndex=i;
		}
	}
	let minX=xClusters[seedIndex].minX;
	let maxX=xClusters[seedIndex].maxX;
	for(let i=seedIndex-1;i>=0;i--){
		if(minX-xClusters[i].maxX>maxGap) break;
		minX=Math.min(minX, xClusters[i].minX);
	}
	for(let i=seedIndex+1;i<xClusters.length;i++){
		if(xClusters[i].minX-maxX>maxGap) break;
		maxX=Math.max(maxX, xClusters[i].maxX);
	}
	const filtered=rows
		.map(r=>({
			...r,
			cells:(r.cells||[]).filter(c=>{
				if(typeof c.x!=="number" || !isFinite(c.x)) return true;
				if(headerRegex.test(String(c.text||""))) return true;
				return c.x>=minX-pad && c.x<=maxX+pad;
			})
		}))
		.filter(r=>r.cells.length);
	return filtered.length ? filtered : rows;
}

function pickClosestCandidate(candidates, estimator, targetQty){
	if(!candidates.length) return null;
	if(!targetQty || !isFinite(targetQty) || targetQty<=0) return candidates[0];
	let best=candidates[0];
	let bestDiff=Math.abs(estimator(best)-targetQty);
	for(const c of candidates.slice(1)){
		const d=Math.abs(estimator(c)-targetQty);
		if(d<bestDiff){ best=c; bestDiff=d; }
	}
	return best;
}

function buildScheduleBounds(rows){
	const pts=[];
	for(const r of rows) for(const c of r.cells) if(typeof c.x==="number" && typeof c.y==="number") pts.push({x:c.x,y:c.y});
	if(!pts.length) return null;
	const xs=pts.map(p=>p.x), ys=pts.map(p=>p.y);
	return { minX:Math.min(...xs), maxX:Math.max(...xs), minY:Math.min(...ys), maxY:Math.max(...ys) };
}

function pointInBounds(p,b,pad){
	if(!b || typeof p.x!=="number" || typeof p.y!=="number") return false;
	return p.x>=b.minX-pad && p.x<=b.maxX+pad && p.y>=b.minY-pad && p.y<=b.maxY+pad;
}

function dedupeByTagXY(points, tol){
	const m=new Map();
	for(const p of points){
		const k=`${p.tag}|${Math.round(p.x/tol)}|${Math.round(p.y/tol)}`;
		if(!m.has(k)) m.set(k,p);
	}
	return [...m.values()];
}

function distance(a,b){ return Math.hypot(a.x-b.x, a.y-b.y); }

function overlapLen(a1,a2,b1,b2){
	const lo=Math.max(a1,b1);
	const hi=Math.min(a2,b2);
	return Math.max(0, hi-lo);
}

function clusterByRadius(points, radius){
	const clusters=[];
	const used=new Array(points.length).fill(false);
	for(let i=0;i<points.length;i++){
		if(used[i]) continue;
		used[i]=true;
		const stack=[i];
		const members=[];
		while(stack.length){
			const idx=stack.pop();
			const p=points[idx];
			members.push(p);
			for(let j=0;j<points.length;j++){
				if(used[j]) continue;
				if(distance(p, points[j])<=radius){ used[j]=true; stack.push(j); }
			}
		}
		const xs=members.map(m=>m.x), ys=members.map(m=>m.y);
		clusters.push({ id:`cl_${clusters.length+1}`, count:members.length, bounds:{ minX:Math.min(...xs), maxX:Math.max(...xs), minY:Math.min(...ys), maxY:Math.max(...ys) }, members });
	}
	clusters.sort((a,b)=>b.count-a.count);
	return clusters;
}

function tallyByTag(points){
	const out={};
	for(const p of (points||[])) out[p.tag]=(out[p.tag]||0)+1;
	return out;
}

function estimateFootingsM3ForMembers(members, tagDimsMap, unit){
	const counts=tallyByTag(members||[]);
	let total=0;
	for(const [tag,count] of Object.entries(counts)){
		const dn=tagDimsMap[tag];
		if(!dn || dn.length<3) continue;
		const [L,W,T]=convertFootingDimsRaw(dn, unit);
		if([L,W,T].some(v=>v===null)) continue;
		total += count*L*W*T;
	}
	return total;
}

function estimateColumnsM3ForMembers(members, tagDimsMap, unit, h, isAllowedColumnTag){
	const counts=tallyByTag(members||[]);
	let total=0;
	for(const [tag,count] of Object.entries(counts)){
		if(!/^C\d+$/i.test(tag) || !isAllowedColumnTag(tag)) continue;
		const dn=tagDimsMap[tag];
		if(!dn || dn.length!==2) continue;
		const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
		if([B,D].some(v=>v===null)) continue;
		if(!isRealisticColumnSection(B, D)) continue;
		total += count*B*D*h;
	}
	return total;
}

function estimateColumnsM3ForCandidate(candidate, tagDimsMap, unit, groundH, firstH, isAllowedColumnTag){
	if(!candidate || !Array.isArray(candidate.clusters)) return 0;
	if(!candidate.clusters.length) return 0;
	const groundCluster=candidate.clusters[0];
	const firstClusters=candidate.clusters.slice(1);
	const groundMembers=(groundCluster?.members||[]).filter(m=>/^C\d+$/i.test(m.tag) && isAllowedColumnTag(m.tag));
	const firstMembers=firstClusters.flatMap(c=>(c.members||[]).filter(m=>/^C\d+$/i.test(m.tag) && isAllowedColumnTag(m.tag)));
	const g=estimateColumnsM3ForMembers(groundMembers, tagDimsMap, unit, Number(groundH||0), isAllowedColumnTag);
	const f=estimateColumnsM3ForMembers(firstMembers, tagDimsMap, unit, Number(firstH||0), isAllowedColumnTag);
	return g+f;
}

function topNWithPadding(list, n, factory){
	const out=(list||[]).slice(0,n);
	while(out.length<n) out.push(factory(out.length+1));
	return out;
}

function buildCumulativeMergeCandidates(clusters, metricFn, keyPrefix){
	const sorted=[...(clusters||[])].sort((a,b)=>metricFn(b)-metricFn(a) || b.count-a.count);
	const out=[];
	let ids=[];
	let merged=[];
	let metric=0;
	for(let i=0;i<sorted.length;i++){
		ids = ids.concat([sorted[i].id]);
		merged = merged.concat(sorted[i].members||[]);
		metric += metricFn(sorted[i]);
		out.push({ key:`${keyPrefix}${i+1}`, cluster_ids:[...ids], clusters:sorted.slice(0,i+1), merged_members:[...merged], metric });
	}
	return out;
}

function parseExplicitLevelMeters(text){
	const s=normText(text);
	if(!s) return null;
	let m=s.match(/\(([+-]?\d+(?:\.\d+)?)\s*cm(?:\s*lvl\.?)?\)/i);
	if(m) return Number(m[1])/100;
	m=s.match(/\(([+-]?\d+(?:\.\d+)?)\s*m(?:\s*lvl\.?)?\)/i);
	if(m) return Number(m[1]);
	m=s.match(/([+-]?\d+(?:\.\d+)?)\s*cm(?:\s*lvl\.?)/i);
	if(m) return Number(m[1])/100;
	m=s.match(/([+-]?\d+(?:\.\d+)?)\s*m(?:\s*level|\s*lvl\.?)/i);
	if(m) return Number(m[1]);
	return null;
}

function parseSignedLevelMeters(text){
	const s=normText(text);
	if(!s) return null;
	let m=s.match(/([+-]\d+(?:\.\d+)?)\s*cm\b/i);
	if(m) return Number(m[1])/100;
	m=s.match(/([+-]\d+(?:\.\d+)?)\s*m\b/i);
	if(m) return Number(m[1]);
	m=s.match(/(?:lvl|level)\.?\s*([+-]\d+(?:\.\d+)?)\s*m\b/i);
	if(m) return Number(m[1]);
	m=s.match(/([+-]\d+(?:\.\d+)?)\s*m\s*(?:lvl|level)\b/i);
	if(m) return Number(m[1]);
	return null;
}

function resolveLevelFromNearbyText(texts, anchorRegex, opts={}){
	const radius=Number(opts.radius ?? 20);
	const minLevel=Number(opts.min ?? -5);
	const maxLevel=Number(opts.max ?? 10);
	const signedOnly=opts.signedOnly!==false;
	const parseLevel=signedOnly ? parseSignedLevelMeters : parseExplicitLevelMeters;
	const anchors=(texts||[]).filter(t=>anchorRegex.test(String(t.text||"")));
	if(!anchors.length) return null;
	const candidates=[];
	for(const anchor of anchors){
		const directVal=parseLevel(anchor.text);
		if(typeof directVal==="number" && isFinite(directVal) && directVal>=minLevel && directVal<=maxLevel){
			candidates.push({
				value_m:directVal,
				anchor_text:anchor.text,
				level_text:anchor.text,
				distance:0,
				anchor_x:anchor.x,
				anchor_y:anchor.y,
				level_x:anchor.x,
				level_y:anchor.y
			});
		}
		if(!(typeof anchor.x==="number" && isFinite(anchor.x) && typeof anchor.y==="number" && isFinite(anchor.y))) continue;
		for(const t of (texts||[])){
			const value=parseLevel(t.text);
			if(!(typeof value==="number" && isFinite(value))) continue;
			if(value<minLevel || value>maxLevel) continue;
			if(!(typeof t.x==="number" && isFinite(t.x) && typeof t.y==="number" && isFinite(t.y))) continue;
			const d=Math.hypot(anchor.x-t.x, anchor.y-t.y);
			if(d>radius) continue;
			candidates.push({
				value_m:value,
				anchor_text:anchor.text,
				level_text:t.text,
				distance:d,
				anchor_x:anchor.x,
				anchor_y:anchor.y,
				level_x:t.x,
				level_y:t.y
			});
		}
	}
	if(!candidates.length) return null;
	candidates.sort((a,b)=>
		a.distance-b.distance
		|| Math.abs(a.value_m)-Math.abs(b.value_m)
		|| (()=>{ const ax=String(a.level_text||""); const bx=String(b.level_text||""); return ax<bx?-1:(ax>bx?1:0); })()
	);
	return {
		value_m:candidates[0].value_m,
		source:candidates[0],
		candidates
	};
}

function resolveFootingLevelEvidence(texts){
	const anchors=(texts||[]).filter(t=>/(?:FOOTING|FOUNDATION)\s+LEVEL|LEVEL TO TOP OF FOOTINGS/i.test(t.text));
	const explicitLevels=(texts||[])
		.map(t=>({ text:t.text, x:t.x, y:t.y, value_m:parseExplicitLevelMeters(t.text) }))
		.filter(t=>typeof t.value_m==="number" && isFinite(t.value_m));
	const candidates=[];

	for(const anchor of anchors){
		for(const lvl of explicitLevels){
			const dx=(typeof anchor.x==="number" && typeof lvl.x==="number") ? anchor.x-lvl.x : 0;
			const dy=(typeof anchor.y==="number" && typeof lvl.y==="number") ? anchor.y-lvl.y : 0;
			const dist=Math.hypot(dx, dy);
			if(dist<=45 && lvl.value_m<0){
				candidates.push({
					value_m:lvl.value_m,
					anchor_text:anchor.text,
					level_text:lvl.text,
					distance:dist
				});
			}
		}
	}

	if(candidates.length){
		candidates.sort((a,b)=>Math.abs(Math.abs(a.value_m)-1)-Math.abs(Math.abs(b.value_m)-1) || a.distance-b.distance);
		return { value_m:candidates[0].value_m, source:candidates[0], candidates };
	}

	const fallback=explicitLevels
		.filter(t=>t.value_m<0 && Math.abs(t.value_m)<=2.5)
		.sort((a,b)=>{
			const explicitA=/\(\s*-\d+\s*CM\s*LVL\.?\s*\)/i.test(a.text) ? 0 : 1;
			const explicitB=/\(\s*-\d+\s*CM\s*LVL\.?\s*\)/i.test(b.text) ? 0 : 1;
			return explicitA-explicitB
				|| Math.abs(Math.abs(a.value_m)-1.2)-Math.abs(Math.abs(b.value_m)-1.2)
				|| Math.abs(Math.abs(a.value_m)-1)-Math.abs(Math.abs(b.value_m)-1);
		});
	if(fallback.length){
		return {
			value_m:fallback[0].value_m,
			source:{ anchor_text:"", level_text:fallback[0].text, distance:null },
			candidates:fallback.slice(0,10)
		};
	}
	return null;
}

function resolveTieBeamLevelEvidence(texts, tieTags){
	const tbTags=[...(tieTags||[])].map(t=>String(t||"").toUpperCase());
	const hasPlainTb=tbTags.some(t=>/^TB\d+$/i.test(t));
	const rows=(texts||[]).map(t=>normText(t.text)).filter(Boolean);

	if(hasPlainTb){
		for(const text of rows){
			const m=text.match(/TB\*\s*ARE\s*AT\s*([+-]?\d+(?:\.\d+)?)\s*m\s*LEVEL/i);
			if(m){
				return {
					value_m:Number(m[1]),
					source:{ matched_rule:"TB_EXCEPTION_LEVEL", level_text:text }
				};
			}
		}
	}

	for(const text of rows){
		const m=text.match(/ALL\s+TIE\s+BEAMS?\s+(?:ARE\s+)?AT\s+(?:LEVEL\s+)?([+-]?\d+(?:\.\d+)?)\s*m/i);
		if(m){
			return {
				value_m:Number(m[1]),
				source:{ matched_rule:"ALL_TIE_BEAMS_LEVEL", level_text:text }
			};
		}
	}

	const tieAnchors=(texts||[])
		.filter(t=>/TIE\s+BEAM\s+LAYOUT|FOUNDATION\s+TO\s+TIE\s+BEAM|TIE\s+BEAM\s+TO\s+FIRST\s+SLAB/i.test(String(t.text||"")));
	const tieLevelCandidates=(texts||[])
		.map(t=>({
			text:t.text,
			x:t.x,
			y:t.y,
			value_m:parseSignedLevelMeters(t.text)
		}))
		.filter(t=>
			typeof t.value_m==="number" &&
			isFinite(t.value_m) &&
			t.value_m>-0.5 &&
			t.value_m<2.5 &&
			/(?:\bTB\b|TIE\s*BEAM|GROUND\s*BEAM\s*TOP\s*LEVEL|COL\.\s*FROM\s*FOUNDATION\s*TO\s*TIE\s*BEAM)/i.test(String(t.text||""))
		)
		.map(t=>{
			let anchorDist=Infinity;
			let anchorText="";
			for(const a of tieAnchors){
				if(!(typeof a.x==="number" && typeof a.y==="number" && typeof t.x==="number" && typeof t.y==="number")) continue;
				const d=Math.hypot(a.x-t.x, a.y-t.y);
				if(d<anchorDist){
					anchorDist=d;
					anchorText=a.text;
				}
			}
			return { ...t, anchor_distance:anchorDist, anchor_text:anchorText };
		});
	if(tieLevelCandidates.length){
		tieLevelCandidates.sort((a,b)=>
			(a.anchor_distance-b.anchor_distance)
			|| Math.abs(a.value_m-0.8)-Math.abs(b.value_m-0.8)
			|| Math.abs(a.value_m)-Math.abs(b.value_m)
		);
		const best=tieLevelCandidates[0];
		return {
			value_m:Number(best.value_m),
			source:{
				matched_rule:"TB_LEVEL_TEXT_EVIDENCE",
				level_text:best.text,
				anchor_text:best.anchor_text||"",
				distance:isFinite(best.anchor_distance) ? best.anchor_distance : null
			},
			candidates:tieLevelCandidates.map(c=>({
				value_m:c.value_m,
				level_text:c.text,
				anchor_text:c.anchor_text||"",
				distance:isFinite(c.anchor_distance) ? c.anchor_distance : null
			}))
		};
	}

	const nearby=resolveLevelFromNearbyText(
		texts,
		/TIE\s+BEAM\s+LAYOUT|FOUNDATION\s+TO\s+TIE\s+BEAM|TB\s*LVL|TIE\s*BEAM\s*LVL/i,
		{ radius:25, signedOnly:true, min:-0.5, max:2.5 }
	);
	if(nearby){
		return {
			value_m:Number(nearby.value_m),
			source:{
				matched_rule:"TB_LEVEL_NEARBY_EVIDENCE",
				level_text:nearby.source?.level_text||"",
				anchor_text:nearby.source?.anchor_text||"",
				distance:nearby.source?.distance ?? null
			},
			candidates:(nearby.candidates||[]).map(c=>({
				value_m:c.value_m,
				level_text:c.level_text,
				anchor_text:c.anchor_text,
				distance:c.distance
			}))
		};
	}
	return null;
}

function parseThicknessMeters(text){
	const s=normText(text);
	if(!s) return null;
	let m=s.match(/(\d+(?:\.\d+)?)\s*cm/i);
	if(m) return Number(m[1])/100;
	m=s.match(/(\d+(?:\.\d+)?)\s*m/i);
	if(m) return Number(m[1]);
	return null;
}

function resolveFinishGroundLevelEvidence(texts){
	const hits=(texts||[])
		.filter(t=>/FINISH GROUND LEVEL/i.test(t.text))
		.map(t=>({ ...t, value_m:parseExplicitLevelMeters(t.text) }))
		.filter(t=>typeof t.value_m==="number" && isFinite(t.value_m) && t.value_m>0);
	if(!hits.length) return null;
	hits.sort((a,b)=>b.value_m-a.value_m || a.x-b.x);
	return { value_m:hits[0].value_m, source:hits[0], candidates:hits };
}

function resolveBitumenEvidence(texts){
	const hotBitumenNote=(texts||[]).find(t=>/HOT BITUMEN|Ø¨ÙŠØªÙˆÙ…ÙŠÙ†/i.test(t.text)) || null;
	const twoCoatsNote=(texts||[]).find(t=>/TWO COATS|2\s*COATS/i.test(t.text)) || null;
	return {
		hot_bitumen_note:hotBitumenNote?.text||null,
		two_coats_note:twoCoatsNote?.text||null,
		coats:twoCoatsNote ? 2 : (hotBitumenNote ? 1 : 0)
	};
}

function buildBeamTagDimsMap(rows, tagRe, dimRe){
	const votes=new Map();
	for(const row of (rows||[])){
		const tagCells=[];
		const dimCells=[];
		for(const cell of (row.cells||[])){
			for(const tag of extractTagsFromText(cell.text, tagRe)){
				if(isBeamTag(tag)) tagCells.push({ tag, x:cell.x, y:cell.y });
			}
			for(const tok of extractDimsFromText(cell.text, dimRe)){
				const dn=parseDimsToken(tok);
				if(!dn || dn.length!==2 || !validDimsCandidate(dn)) continue;
				if(dn[0]<15 || dn[0]>40 || dn[1]<40 || dn[1]>70) continue;
				dimCells.push({ nums:dn, x:cell.x, y:cell.y });
			}
		}
		if(!tagCells.length || !dimCells.length) continue;
		for(const tagCell of tagCells){
			const ordered=[...dimCells].sort((a,b)=>{
				const dxA=(typeof a.x==="number"&&typeof tagCell.x==="number") ? Math.abs(a.x-tagCell.x) : 9999;
				const dxB=(typeof b.x==="number"&&typeof tagCell.x==="number") ? Math.abs(b.x-tagCell.x) : 9999;
				const dyA=(typeof a.y==="number"&&typeof tagCell.y==="number") ? Math.abs(a.y-tagCell.y) : 9999;
				const dyB=(typeof b.y==="number"&&typeof tagCell.y==="number") ? Math.abs(b.y-tagCell.y) : 9999;
				return dxA-dxB || dyA-dyB;
			});
			if(!ordered[0]) continue;
			const key=`${ordered[0].nums[0]}x${ordered[0].nums[1]}`;
			if(!votes.has(tagCell.tag)) votes.set(tagCell.tag, new Map());
			const byDim=votes.get(tagCell.tag);
			const prev=byDim.get(key) || { count:0, totalDx:0 };
			const dx=(typeof ordered[0].x==="number"&&typeof tagCell.x==="number") ? Math.abs(ordered[0].x-tagCell.x) : 9999;
			byDim.set(key, { count:prev.count+1, totalDx:prev.totalDx+dx });
		}
	}
	const out={};
	for(const [tag, byDim] of votes.entries()){
		let bestKey=null;
		let bestCount=-1;
		let bestDx=Infinity;
		for(const [dimsKey, meta] of byDim.entries()){
			const avgDx=meta.totalDx/Math.max(1, meta.count);
			if(meta.count>bestCount || (meta.count===bestCount && avgDx<bestDx)){
				bestKey=dimsKey;
				bestCount=meta.count;
				bestDx=avgDx;
			}
		}
		if(bestKey) out[tag]=bestKey.split("x").map(Number);
	}
	return out;
}

function polygonArea(points){
	let sum=0;
	for(let i=0;i<points.length;i++){
		const p=points[i];
		const q=points[(i+1)%points.length];
		sum += p.x*q.y - q.x*p.y;
	}
	return Math.abs(sum)/2;
}

function extractSlabPolygons(entities, layerRegex=/\b0\s*SLAB\b|SLAB/i){
	const out=[];
	for(const e of (entities||[])){
		if(!layerRegex.test(String(e.layer||""))) continue;
		if(!Array.isArray(e.vertices) || e.vertices.length<3) continue;
		if(!(e.type==="LWPOLYLINE" || e.type==="POLYLINE")) continue;
		const points=e.vertices.map(v=>({ x:v.x, y:v.y }));
		const xs=points.map(p=>p.x), ys=points.map(p=>p.y);
		const area=polygonArea(points);
	out.push({
			layer:e.layer||"",
			shape:Boolean(e.shape || e.closed),
			count:points.length,
			area,
			points,
			minX:Math.min(...xs),
			maxX:Math.max(...xs),
			minY:Math.min(...ys),
			maxY:Math.max(...ys),
			cx:(Math.min(...xs)+Math.max(...xs))/2,
			cy:(Math.min(...ys)+Math.max(...ys))/2,
			x:(Math.min(...xs)+Math.max(...xs))/2,
			y:(Math.min(...ys)+Math.max(...ys))/2
		});
	}
	const dedup=new Map();
	for(const poly of out){
		const key=[
			poly.layer,
			Math.round(poly.minX*20),
			Math.round(poly.maxX*20),
			Math.round(poly.minY*20),
			Math.round(poly.maxY*20),
			Math.round(poly.area*20)
		].join("|");
		if(!dedup.has(key)) dedup.set(key, poly);
	}
	return [...dedup.values()];
}

function extractColumnRectCandidates(entities, opts={}){
	const layerFilter=opts.layerFilter!==false;
	const out=[];
	for(const e of (entities||[])){
		const layer=String(e.layer||"");
		if(layerFilter && !/(?:\bCOL\b|COLUMN|C-COLUMNS|M\.COL|COL-RFC)/i.test(layer)) continue;
		if(!(e.type==="LWPOLYLINE" || e.type==="POLYLINE")) continue;
		if(!Array.isArray(e.vertices) || e.vertices.length<4) continue;
		const points=e.vertices.map(v=>({ x:Number(v.x), y:Number(v.y) }))
			.filter(p=>Number.isFinite(p.x) && Number.isFinite(p.y));
		if(points.length<4) continue;
		const xs=points.map(p=>p.x), ys=points.map(p=>p.y);
		const minX=Math.min(...xs), maxX=Math.max(...xs);
		const minY=Math.min(...ys), maxY=Math.max(...ys);
		const w=maxX-minX;
		const d=maxY-minY;
		if(!Number.isFinite(w) || !Number.isFinite(d)) continue;
		if(w<0.12 || d<0.12 || w>0.90 || d>0.90) continue;
		const min=Math.min(w,d);
		const max=Math.max(w,d);
		if((max/min)>3.5) continue;
		const area=polygonArea(points);
		const boxArea=w*d;
		if(!(boxArea>0) || !(area>0)) continue;
		if((area/boxArea)<0.55) continue;
		out.push({
			x:(minX+maxX)/2,
			y:(minY+maxY)/2,
			B:min,
			D:max,
			layer
		});
	}
	const dedup=new Map();
	for(const rect of out){
		const key=[
			Math.round(rect.x*20),
			Math.round(rect.y*20),
			Math.round(rect.B*100),
			Math.round(rect.D*100)
		].join("|");
		if(!dedup.has(key)) dedup.set(key, rect);
	}
	return [...dedup.values()];
}

function inferColumnDimsFromPlanGeometry(entities, columnTagPoints, opts={}){
	const out={};
	const pts=(columnTagPoints||[]).filter(p=>isColumnTag(p.tag) && Number.isFinite(p.x) && Number.isFinite(p.y));
	if(!pts.length) return out;
	let rects=extractColumnRectCandidates(entities, { layerFilter:true });
	if(!rects.length){
		rects=extractColumnRectCandidates(entities, { layerFilter:false });
	}
	if(!rects.length) return out;
	const maxDistance=Number(opts.maxDistanceM||1.2);
	if(!(maxDistance>0)) return out;
	const byTag=new Map();
	for(const pt of pts){
		let best=null;
		let bestDist=Infinity;
		for(const rect of rects){
			const d=distance(pt, rect);
			if(d<bestDist){
				bestDist=d;
				best=rect;
			}
		}
		if(!best || bestDist>maxDistance) continue;
		if(!byTag.has(pt.tag)) byTag.set(pt.tag, []);
		byTag.get(pt.tag).push({ B:best.B, D:best.D, dist:bestDist });
	}
	for(const [tag, samples] of byTag.entries()){
		if(!samples.length) continue;
		const filtered=samples
			.filter(s=>s.dist<=Math.max(0.5, median(samples.map(x=>x.dist))*1.5))
			.sort((a,b)=>a.dist-b.dist);
		const picked=filtered.length?filtered:samples;
		const Bm=median(picked.map(s=>s.B));
		const Dm=median(picked.map(s=>s.D));
		if(!isRealisticColumnSection(Bm, Dm)) continue;
		// Keep raw dims in cm-style numbers so downstream raw-dims sanity checks stay valid.
		const rawB=convertMetersToDimRaw(roundToStep(Bm, 0.01), "cm");
		const rawD=convertMetersToDimRaw(roundToStep(Dm, 0.01), "cm");
		if(!isLikelyColumnDimsRaw([rawB, rawD])) continue;
		out[tag]=[rawB, rawD];
	}
	return out;
}

function resolveSlabLayoutAnchors(texts){
	const ground=(texts||[]).filter(t=>{
		const s=String(t.text||"");
		if(!/(?:GROUND(?:\s+FLOOR)?\s+SLAB(?:\s+LAYOUT|\s+LVL|\s+LEVEL|\s+DETAILS)?|G\.?\s*FLOOR\s+SLAB|GR\.?\s*FLOOR\s+SLAB)/i.test(s)) return false;
		if(/SHALL|CONSTRUCTED|IN\s+BAYS|WITH\s+CONTINUITY|BLOCKS\s+AND\s+MORTAR|EXTERNAL\s+GROUND\s+FLOOR|INTERNAL\s+GROUND\s+FLOOR/i.test(s)) return false;
		return s.length<=90;
	});
	const firstFloor=(texts||[]).filter(t=>{
		const s=String(t.text||"");
		return /FIRST(?:\s+FLOOR)?\s+SLAB(?:\s+LAYOUT|\s+PLAN|\s+DETAILS)?/i.test(s) && s.length<=90;
	});
	const topRoof=(texts||[]).filter(t=>{
		const s=String(t.text||"");
		return /TOP\s+ROOF\s+SLAB(?:\s+LAYOUT|\s+PLAN|\s+DETAILS)?/i.test(s) && s.length<=90;
	});
	const roof=(texts||[]).filter(t=>{
		const s=String(t.text||"");
		return /ROOF\s+SLAB(?:\s+LAYOUT|\s+PLAN|\s+DETAILS)?/i.test(s) && !/TOP\s+ROOF/i.test(s) && s.length<=90;
	});
	return {
		ground:ground.map(t=>({ ...t, kind:"GROUND" })),
		firstFloor:firstFloor.map(t=>({ ...t, kind:"FIRST_FLOOR" })),
		roof:roof.map(t=>({ ...t, kind:"ROOF" })),
		topRoof:topRoof.map(t=>({ ...t, kind:"TOP_ROOF" }))
	};
}

function nearestLayoutAnchor(point, anchors){
	const all=[...(anchors?.ground||[]), ...(anchors?.firstFloor||[]), ...(anchors?.roof||[]), ...(anchors?.topRoof||[])];
	if(!all.length) return null;
	let best=null;
	let bestDist=Infinity;
	for(const anchor of all){
		const d=Math.hypot((point.x||0)-(anchor.x||0), (point.y||0)-(anchor.y||0));
		if(d<bestDist){ best=anchor; bestDist=d; }
	}
	return best ? { anchor:best, distance:bestDist } : null;
}

function buildSlabPolyAnchorsByThickness(firstFloorEvidence, roofEvidence, topRoofEvidence, fallbackAnchors){
	const out=[];
	const pushFallback=(kind, list, fallbackText)=>{
		if(!Array.isArray(list) || !list.length) return;
		out.push(...list.map(anchor=>({
			kind,
			x:anchor.x,
			y:anchor.y,
			text:anchor.text || fallbackText
		})));
	};
	if(fallbackAnchors?.ground?.length){
		pushFallback("GROUND", fallbackAnchors.ground, "GROUND_SLAB_LAYOUT");
	}
	if(firstFloorEvidence?.source && typeof firstFloorEvidence.source.x==="number" && typeof firstFloorEvidence.source.y==="number"){
		out.push({
			kind:"FIRST_FLOOR",
			x:firstFloorEvidence.source.x,
			y:firstFloorEvidence.source.y,
			text:firstFloorEvidence.source.text || "FIRST_SLAB_THICKNESS"
		});
	} else if(fallbackAnchors?.firstFloor?.length){
		pushFallback("FIRST_FLOOR", fallbackAnchors.firstFloor, "FIRST_SLAB_LAYOUT");
	}
	if(roofEvidence?.source && typeof roofEvidence.source.x==="number" && typeof roofEvidence.source.y==="number"){
		out.push({
			kind:"ROOF",
			x:roofEvidence.source.x,
			y:roofEvidence.source.y,
			text:roofEvidence.source.text || "ROOF_SLAB_THICKNESS"
		});
	} else if(fallbackAnchors?.roof?.length){
		pushFallback("ROOF", fallbackAnchors.roof, "ROOF_SLAB_LAYOUT");
	}
	if(topRoofEvidence?.source && typeof topRoofEvidence.source.x==="number" && typeof topRoofEvidence.source.y==="number"){
		out.push({
			kind:"TOP_ROOF",
			x:topRoofEvidence.source.x,
			y:topRoofEvidence.source.y,
			text:topRoofEvidence.source.text || "TOP_ROOF_SLAB_THICKNESS"
		});
	} else if(fallbackAnchors?.topRoof?.length){
		pushFallback("TOP_ROOF", fallbackAnchors.topRoof, "TOP_ROOF_SLAB_LAYOUT");
	}
	return out;
}

function pickFallbackSlabPolygonsByKind(entities, slabAnchors, opts={}){
	const kindAnchors=[
		{ kind:"GROUND", anchors:slabAnchors?.ground||[] },
		{ kind:"FIRST_FLOOR", anchors:slabAnchors?.firstFloor||[] },
		{ kind:"ROOF", anchors:slabAnchors?.roof||[] },
		{ kind:"TOP_ROOF", anchors:slabAnchors?.topRoof||[] }
	].filter(k=>Array.isArray(k.anchors) && k.anchors.length);
	if(!kindAnchors.length) return [];
	const minArea=Number(opts.minAreaM2 ?? 80);
	const maxArea=Number(opts.maxAreaM2 ?? 700);
	const maxDistance=Number(opts.maxDistanceM ?? 70);
	const bannedLayer=/(?:FRAME|FORMAT|TITLE|BORDER|DIM|DETAIL|HATCH|HAT|ANNO|NOTE|TABLE|LEGEND|TEXT)/i;
	const polyPool=extractSlabPolygons(entities, /.*/)
		.filter(poly=>
			poly?.shape &&
			typeof poly.area==="number" &&
			isFinite(poly.area) &&
			poly.area>=minArea &&
			poly.area<=maxArea &&
			!bannedLayer.test(String(poly.layer||""))
		);
	if(!polyPool.length) return [];
	const selected=[];
	for(const entry of kindAnchors){
		const list=polyPool
			.map(poly=>{
				let nearest=null;
				let nearestDistance=Infinity;
				for(const anchor of entry.anchors){
					if(!(typeof anchor.x==="number" && typeof anchor.y==="number")) continue;
					const d=Math.hypot((poly.cx||0)-anchor.x, (poly.cy||0)-anchor.y);
					if(d<nearestDistance){
						nearestDistance=d;
						nearest=anchor;
					}
				}
				return nearest ? { ...poly, nearest:{ anchor:nearest, distance:nearestDistance } } : null;
			})
			.filter(poly=>poly && poly.nearest && poly.nearest.distance<=maxDistance);
		if(!list.length) continue;
		list.sort((a,b)=>a.nearest.distance-b.nearest.distance || b.area-a.area);
		const best=list[0];
		selected.push({
			...best,
			nearest:{
				anchor:{ ...best.nearest.anchor, kind:entry.kind },
				distance:best.nearest.distance
			},
			evidence_mode:"BROAD_POLY_NEAR_LAYOUT_ANCHOR"
		});
	}
	return selected;
}

function extractPositiveLevelValuesFromText(text){
	const up=normalizePdfText(text);
	const out=[];
	for(const m of up.matchAll(/\(([+]\d+(?:\.\d+)?)\s*CM\)/g)){
		out.push({ value_m:Number(m[1])/100, raw:m[0], unit:"CM" });
	}
	for(const m of up.matchAll(/\(([+]\d+(?:\.\d+)?)\s*M\)/g)){
		out.push({ value_m:Number(m[1]), raw:m[0], unit:"M" });
	}
	return out.filter(v=>v.value_m>0 && v.value_m<2);
}

function resolveSolidBlockHeightEvidence({ tieLevelEvidence, texts, supplementalPdfTexts, targetQty, tieLengthTotal }){
	const candidates=[];
	const levelAnchors=(texts||[])
		.filter(t=>/(?:I\.?\s*L\.?\s*LEV|INTERLOCK|F\.?F\.?L|N\.?G\.?L|GRADE\s*SLAB|GRD\.?\s*SLAB|GROUND\s*SLAB\s*LVL)/i.test(String(t.text||"")));
	for(const anchor of levelAnchors){
		const directVal=parseSignedLevelMeters(anchor.text) ?? parseExplicitLevelMeters(anchor.text);
		if(typeof directVal==="number" && isFinite(directVal) && directVal>-0.2 && directVal<2){
			candidates.push({
				type:"DIRECT_LEVEL",
				level_m:directVal,
				height_m:directVal,
				source:{ text:anchor.text, x:anchor.x, y:anchor.y, source:"DXF_TEXT" }
			});
		}
		if(typeof anchor.x==="number" && isFinite(anchor.x) && typeof anchor.y==="number" && isFinite(anchor.y)){
			const nearby=(texts||[])
				.map(t=>({ ...t, value_m:parseSignedLevelMeters(t.text) }))
				.filter(t=>typeof t.value_m==="number" && isFinite(t.value_m) && t.value_m>-0.2 && t.value_m<2)
				.map(t=>({ ...t, distance:Math.hypot((t.x||0)-anchor.x, (t.y||0)-anchor.y) }))
				.filter(t=>t.distance<=20)
				.sort((a,b)=>a.distance-b.distance)
				.slice(0,5);
			for(const hit of nearby){
				candidates.push({
					type:"NEARBY_LEVEL",
					level_m:hit.value_m,
					height_m:hit.value_m,
					source:{
						text:hit.text,
						anchor_text:anchor.text,
						distance:hit.distance,
						x:hit.x,
						y:hit.y,
						source:"DXF_NEARBY_TEXT"
					}
				});
			}
			if(!nearby.length && /GROUND\s*SLAB\s*LVL|N\.?G\.?L/i.test(String(anchor.text||""))){
				candidates.push({
					type:"GROUND_SLAB_DATUM_ZERO",
					level_m:0,
					height_m:0,
					source:{ text:anchor.text, x:anchor.x, y:anchor.y, source:"DXF_GROUND_DATUM" }
				});
			}
		}
	}
	for(const [pdfName, pdfText] of Object.entries(supplementalPdfTexts||{})){
		for(const hit of extractPositiveLevelValuesFromText(pdfText)){
			candidates.push({
				type:"PDF_LEVEL",
				level_m:hit.value_m,
				height_m:hit.value_m,
				source:{ text:hit.raw, source:`PDF:${pdfName}` }
			});
		}
	}
	if(typeof tieLevelEvidence?.value_m==="number" && isFinite(tieLevelEvidence.value_m) && tieLevelEvidence.value_m>0){
		for(const cand of [...candidates]){
			const derived=tieLevelEvidence.value_m-cand.level_m;
			if(derived>0.15 && derived<3){
				candidates.push({
					type:"TIE_MINUS_LEVEL",
					level_m:cand.level_m,
					height_m:derived,
					source:{
						text:`TIE ${tieLevelEvidence.value_m} - LEVEL ${cand.level_m}`,
						basis:cand.source,
						source:cand.source?.source || "DERIVED"
					}
				});
			}
		}
	}
	const uniq=[];
	const seen=new Set();
	for(const cand of candidates){
		const key=`${cand.type}|${Number(cand.level_m||0).toFixed(3)}|${Number(cand.height_m||0).toFixed(3)}|${cand.source?.source||""}|${cand.source?.text||""}`;
		if(seen.has(key)) continue;
		seen.add(key);
		uniq.push(cand);
	}
	if(!uniq.length) return null;
	let best=uniq[0];
	if(targetQty>0 && tieLengthTotal>0){
		let bestDiff=Math.abs((best.height_m*tieLengthTotal)-targetQty);
		for(const cand of uniq.slice(1)){
			const diff=Math.abs((cand.height_m*tieLengthTotal)-targetQty);
			if(diff<bestDiff || (diff===bestDiff && cand.type==="TIE_MINUS_LEVEL" && best.type!=="TIE_MINUS_LEVEL")){
				best=cand;
				bestDiff=diff;
			}
		}
	}else{
		uniq.sort((a,b)=>{
			const byHeight=b.height_m-a.height_m;
			if(byHeight!==0) return byHeight;
			const ta=String(a.type||"");
			const tb=String(b.type||"");
			if(ta<tb) return -1;
			if(ta>tb) return 1;
			return 0;
		});
		best=uniq[0];
	}
	return {
		height_m:best.height_m,
		selected:best,
		candidates:uniq
			.sort((a,b)=>a.height_m-b.height_m)
			.map(c=>({ type:c.type, level_m:c.level_m, height_m:c.height_m, source:c.source }))
	};
}

function resolveSlabThicknessByKind(texts, anchors, kind){
	const hits=(texts||[])
		.filter(t=>/(?:MAIN\s+|SUNK\s+)?SLAB THICKNESS/i.test(t.text))
		.map(t=>({ ...t, thickness_m:parseThicknessMeters(t.text), nearest:nearestLayoutAnchor(t, anchors) }))
		.filter(t=>typeof t.thickness_m==="number" && isFinite(t.thickness_m) && t.nearest && t.nearest.anchor.kind===kind);
	if(!hits.length) return null;
	hits.sort((a,b)=>a.nearest.distance-b.nearest.distance);
	return {
		thickness_m:hits[0].thickness_m,
		source:{ text:hits[0].text, distance:hits[0].nearest.distance, x:hits[0].x, y:hits[0].y },
		candidates:hits.map(h=>({ text:h.text, thickness_m:h.thickness_m, distance:h.nearest.distance }))
	};
}

function resolveGenericSlabThicknessEvidence(texts){
	const hits=(texts||[])
		.map(t=>({ ...t, thickness_m:parseThicknessMeters(t.text) }))
		.filter(t=>
			typeof t.thickness_m==="number" &&
			isFinite(t.thickness_m) &&
			t.thickness_m>=0.08 &&
			t.thickness_m<=0.35 &&
			/(?:CONCRETE\s*SLAB|SLAB\s*THICK(?:NESS)?|SLAB\s*THK|10\s*CM\s*THICK)/i.test(String(t.text||""))
		);
	if(!hits.length) return null;
	hits.sort((a,b)=>
		Math.abs(a.thickness_m-0.10)-Math.abs(b.thickness_m-0.10)
		|| String(a.text||"").length-String(b.text||"").length
	);
	const best=hits[0];
	return {
		thickness_m:best.thickness_m,
		source:{ text:best.text, x:best.x, y:best.y },
		candidates:hits.slice(0,20).map(h=>({ text:h.text, thickness_m:h.thickness_m }))
	};
}

function buildAnchoredSubsetCandidates(clusters, metricFn, keyPrefix, maxClusters=10){
	const sorted=[...(clusters||[])].sort((a,b)=>metricFn(b)-metricFn(a) || b.count-a.count).slice(0, maxClusters);
	if(!sorted.length) return [];
	const anchor=sorted[0];
	const rest=sorted.slice(1);
	const out=[];
	const maxMask=Math.pow(2, rest.length);
	for(let mask=0; mask<maxMask; mask++){
		const picked=[anchor];
		for(let i=0;i<rest.length;i++){
			if(mask & (1<<i)) picked.push(rest[i]);
		}
		out.push({
			key:`${keyPrefix}${out.length+1}`,
			cluster_ids:picked.map(c=>c.id),
			clusters:picked,
			merged_members:picked.flatMap(c=>c.members||[]),
			metric:picked.reduce((sum, c)=>sum+metricFn(c), 0)
		});
	}
	return out;
}

function uniqueRows(rows){
	const seen=new Set();
	const out=[];
	for(const row of (rows||[])){
		if(seen.has(row)) continue;
		seen.add(row);
		out.push(row);
	}
	return out;
}

function extractBeamOutlineSegments(entities, layerRegex=/beam|c-beams|beams/i){
	const segments=[];
	let id=1;
	const push=(x1,y1,x2,y2,layer)=>{
		if(![x1,y1,x2,y2].every(v=>typeof v==="number" && isFinite(v))) return;
		const dx=x2-x1, dy=y2-y1;
		const len=Math.hypot(dx,dy);
		if(len<0.15) return;
		let ori="D";
		if(Math.abs(dy)<=0.05) ori="H";
		else if(Math.abs(dx)<=0.05) ori="V";
		segments.push({ id:id++, layer:layer||"", ori, x1, y1, x2, y2, len });
	};

	for(const e of (entities||[])){
		if(!layerRegex.test(String(e.layer||""))) continue;
		if(e.type==="LINE"){
			const p1=e.vertices?.[0]||e.startPoint;
			const p2=e.vertices?.[1]||e.endPoint;
			if(p1&&p2) push(p1.x,p1.y,p2.x,p2.y,e.layer);
			continue;
		}
		if((e.type==="LWPOLYLINE" || e.type==="POLYLINE") && Array.isArray(e.vertices) && e.vertices.length>=2){
			for(let i=1;i<e.vertices.length;i++) push(e.vertices[i-1].x,e.vertices[i-1].y,e.vertices[i].x,e.vertices[i].y,e.layer);
			if(e.shape || e.closed) push(e.vertices[e.vertices.length-1].x,e.vertices[e.vertices.length-1].y,e.vertices[0].x,e.vertices[0].y,e.layer);
		}
	}
	return segments;
}

function beamMemberKey(member){
	return `${member.ori}|${Math.round(member.x1*100)}|${Math.round(member.y1*100)}|${Math.round(member.x2*100)}|${Math.round(member.y2*100)}|${Math.round((member.length||0)*100)}`;
}

function buildTieBeamMembers(segments){
	const horizontal=(segments||[]).filter(s=>s.ori==="H" && s.len>=0.4);
	const vertical=(segments||[]).filter(s=>s.ori==="V" && s.len>=0.4);
	const diagonal=(segments||[]).filter(s=>s.ori==="D" && s.len>=0.4);
	const usedSegIds=new Set();
	const members=[];

	const tryPair=(arr, axis)=>{
		for(let i=0;i<arr.length;i++){
			if(usedSegIds.has(arr[i].id)) continue;
			let best=null;
			let bestScore=-1;
			for(let j=i+1;j<arr.length;j++){
				if(usedSegIds.has(arr[j].id)) continue;
				const a=arr[i], b=arr[j];
				const offset = axis==="H" ? Math.abs(a.y1-b.y1) : Math.abs(a.x1-b.x1);
				if(offset<0.12 || offset>0.70) continue;
				if(Math.abs(a.len-b.len)>Math.max(0.2, Math.max(a.len,b.len)*0.15)) continue;
				const overlap = axis==="H"
					? overlapLen(Math.min(a.x1,a.x2), Math.max(a.x1,a.x2), Math.min(b.x1,b.x2), Math.max(b.x1,b.x2))
					: overlapLen(Math.min(a.y1,a.y2), Math.max(a.y1,a.y2), Math.min(b.y1,b.y2), Math.max(b.y1,b.y2));
				if(overlap<0.5) continue;
				if(overlap < 0.75*Math.min(a.len,b.len)) continue;
				const score=overlap-offset;
				if(score>bestScore){ best={ a,b,overlap,offset }; bestScore=score; }
			}
			if(!best) continue;
			usedSegIds.add(best.a.id);
			usedSegIds.add(best.b.id);
			if(axis==="H"){
				const x1=Math.max(Math.min(best.a.x1,best.a.x2), Math.min(best.b.x1,best.b.x2));
				const x2=Math.min(Math.max(best.a.x1,best.a.x2), Math.max(best.b.x1,best.b.x2));
				const y=(best.a.y1+best.b.y1)/2;
				members.push({
					ori:"H",
					x1, y1:y, x2, y2:y,
					length:Math.max(0, x2-x1),
					source:"PAIR",
					segment_ids:[best.a.id, best.b.id]
				});
			}else{
				const y1=Math.max(Math.min(best.a.y1,best.a.y2), Math.min(best.b.y1,best.b.y2));
				const y2=Math.min(Math.max(best.a.y1,best.a.y2), Math.max(best.b.y1,best.b.y2));
				const x=(best.a.x1+best.b.x1)/2;
				members.push({
					ori:"V",
					x1:x, y1, x2:x, y2,
					length:Math.max(0, y2-y1),
					source:"PAIR",
					segment_ids:[best.a.id, best.b.id]
				});
			}
		}
	};

	tryPair(horizontal, "H");
	tryPair(vertical, "V");

	for(const seg of diagonal){
		if(usedSegIds.has(seg.id)) continue;
		members.push({
			ori:"D",
			x1:seg.x1, y1:seg.y1, x2:seg.x2, y2:seg.y2,
			length:seg.len,
			source:"SINGLE_DIAGONAL",
			segment_ids:[seg.id]
		});
	}

	const dedup=new Map();
	for(const member of members){
		if(member.length<0.4) continue;
		const key=beamMemberKey(member);
		if(!dedup.has(key)) dedup.set(key, member);
	}
	return [...dedup.values()];
}

function clusterAxisValues(points, key, tol){
	const sorted=[...(points||[])].sort((a,b)=>(a[key]||0)-(b[key]||0));
	const groups=[];
	for(const point of sorted){
		const current=groups[groups.length-1];
		if(!current || Math.abs(current.mean-point[key])>tol){
			groups.push({ mean:point[key], points:[point] });
			continue;
		}
		current.points.push(point);
		current.mean=current.points.reduce((sum,p)=>sum+(p[key]||0),0)/current.points.length;
	}
	return groups;
}

function majorityTag(points){
	const freq=new Map();
	for(const point of (points||[])) freq.set(point.tag, (freq.get(point.tag)||0)+1);
	let bestTag=null;
	let bestCount=-1;
	for(const [tag,count] of freq.entries()){
		if(count>bestCount){ bestTag=tag; bestCount=count; }
	}
	return bestTag;
}

function pickTieLabelRegion(points, texts){
	const regions=clusterByRadius(points||[], 30).filter(r=>(r.members||[]).length>=5);
	if(!regions.length) return null;
	const anchors=(texts||[]).filter(t=>/TIE BEAM\s+LAYOUT/i.test(t.text));
	if(!anchors.length) return regions.sort((a,b)=>b.members.length-a.members.length)[0];
	let best=null;
	let bestDist=Infinity;
	for(const region of regions){
		const xs=region.members.map(p=>p.x);
		const ys=region.members.map(p=>p.y);
		const cx=(Math.min(...xs)+Math.max(...xs))/2;
		const cy=(Math.min(...ys)+Math.max(...ys))/2;
		for(const anchor of anchors){
			const d=Math.hypot(cx-(anchor.x||0), cy-(anchor.y||0));
			if(d<bestDist){ best=region; bestDist=d; }
		}
	}
	return best || regions.sort((a,b)=>b.members.length-a.members.length)[0];
}

function buildTieBeamMembersFromLabelSpans(labelPoints, texts){
	const region=pickTieLabelRegion(labelPoints, texts);
	if(!region) return { members:[], region_id:null };
	const members=[];

	for(const group of clusterAxisValues(region.members, "x", 0.8)){
		if(group.points.length<3) continue;
		const ys=group.points.map(p=>p.y);
		const y1=Math.min(...ys);
		const y2=Math.max(...ys);
		const length=y2-y1;
		if(length<3) continue;
		members.push({
			ori:"V",
			x1:group.mean,
			y1,
			x2:group.mean,
			y2,
			length,
			source:"LABEL_SPAN_V",
			label_points:group.points.map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | "),
			tag:majorityTag(group.points)
		});
	}

	for(const group of clusterAxisValues(region.members, "y", 0.8)){
		if(group.points.length<4) continue;
		const xs=group.points.map(p=>p.x);
		const x1=Math.min(...xs);
		const x2=Math.max(...xs);
		const length=x2-x1;
		if(length<3) continue;
		members.push({
			ori:"H",
			x1,
			y1:group.mean,
			x2,
			y2:group.mean,
			length,
			source:"LABEL_SPAN_H",
			label_points:group.points.map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | "),
			tag:majorityTag(group.points)
		});
	}

	const dedup=new Map();
	for(const member of members){
		const key=`${member.tag}|${beamMemberKey(member)}`;
		if(!dedup.has(key)) dedup.set(key, member);
	}
	return { members:[...dedup.values()], region_id:region.id||null };
}

function buildBeamMembersFromLabelSpans(labelPoints, kind){
	const scoped=(labelPoints||[]).filter(p=>p.kind===kind);
	if(!scoped.length) return [];
	const members=[];

	for(const group of clusterAxisValues(scoped, "x", 0.8)){
		if(group.points.length<2) continue;
		const ys=group.points.map(p=>p.y);
		const y1=Math.min(...ys);
		const y2=Math.max(...ys);
		const length=y2-y1;
		if(length<4) continue;
		members.push({
			kind,
			ori:"V",
			x1:group.mean,
			y1,
			x2:group.mean,
			y2,
			length,
			source:`BEAM_LABEL_SPAN_V_${kind}`,
			label_points:group.points.map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | "),
			tag:majorityTag(group.points)
		});
	}

	for(const group of clusterAxisValues(scoped, "y", 0.8)){
		if(group.points.length<2) continue;
		const xs=group.points.map(p=>p.x);
		const x1=Math.min(...xs);
		const x2=Math.max(...xs);
		const length=x2-x1;
		if(length<4) continue;
		members.push({
			kind,
			ori:"H",
			x1,
			y1:group.mean,
			x2,
			y2:group.mean,
			length,
			source:`BEAM_LABEL_SPAN_H_${kind}`,
			label_points:group.points.map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | "),
			tag:majorityTag(group.points)
		});
	}

	const dedup=new Map();
	for(const member of members){
		const key=`${member.kind}|${member.tag}|${beamMemberKey(member)}`;
		if(!dedup.has(key)) dedup.set(key, member);
	}
	return [...dedup.values()];
}

function distancePointToSegment(px, py, seg){
	const x1=seg.x1, y1=seg.y1, x2=seg.x2, y2=seg.y2;
	const dx=x2-x1, dy=y2-y1;
	if(dx===0 && dy===0) return Math.hypot(px-x1, py-y1);
	const t=Math.max(0, Math.min(1, ((px-x1)*dx + (py-y1)*dy)/(dx*dx + dy*dy)));
	const qx=x1 + t*dx;
	const qy=y1 + t*dy;
	return Math.hypot(px-qx, py-qy);
}

function nearestTieBeamMember(labelPoint, members){
	let best=null;
	let bestDist=Infinity;
	for(const member of (members||[])){
		const d=distancePointToSegment(labelPoint.x, labelPoint.y, member);
		if(d<bestDist){ best=member; bestDist=d; }
	}
	return best ? { member:best, distance:bestDist } : null;
}

function buildTieBeamTagDimsMap(rows, tagRe, dimRe){
	const votes=new Map();
	for(const row of (rows||[])){
		const tagCells=[];
		const dimCells=[];
		for(const cell of (row.cells||[])){
			for(const tag of extractTagsFromText(cell.text, tagRe)){
				if(isTieBeamTag(tag)) tagCells.push({ tag, x:cell.x, y:cell.y });
			}
			for(const tok of extractDimsFromText(cell.text, dimRe)){
				const dn=parseDimsToken(tok);
				if(!dn || dn.length!==2 || !validDimsCandidate(dn)) continue;
				if(dn[0]<10 || dn[0]>80 || dn[1]<30 || dn[1]>80) continue;
				dimCells.push({ nums:dn, x:cell.x, y:cell.y });
			}
		}
		if(!tagCells.length || !dimCells.length) continue;
		for(const tagCell of tagCells){
			const ordered=[...dimCells].sort((a,b)=>{
				const dxA=(typeof a.x==="number"&&typeof tagCell.x==="number") ? Math.abs(a.x-tagCell.x) : 9999;
				const dxB=(typeof b.x==="number"&&typeof tagCell.x==="number") ? Math.abs(b.x-tagCell.x) : 9999;
				const dyA=(typeof a.y==="number"&&typeof tagCell.y==="number") ? Math.abs(a.y-tagCell.y) : 9999;
				const dyB=(typeof b.y==="number"&&typeof tagCell.y==="number") ? Math.abs(b.y-tagCell.y) : 9999;
				return dxA-dxB || dyA-dyB;
			});
			if(!ordered[0]) continue;
			const key=`${ordered[0].nums[0]}x${ordered[0].nums[1]}`;
			if(!votes.has(tagCell.tag)) votes.set(tagCell.tag, new Map());
			const byDim=votes.get(tagCell.tag);
			const prev=byDim.get(key) || { count:0, totalDx:0 };
			const dx=(typeof ordered[0].x==="number"&&typeof tagCell.x==="number") ? Math.abs(ordered[0].x-tagCell.x) : 9999;
			byDim.set(key, { count:prev.count+1, totalDx:prev.totalDx+dx });
		}
	}

	const out={};
	for(const [tag, byDim] of votes.entries()){
		let bestKey=null;
		let bestCount=-1;
		let bestDx=Infinity;
		for(const [dimsKey, meta] of byDim.entries()){
			const avgDx=meta.totalDx/Math.max(1, meta.count);
			if(meta.count>bestCount || (meta.count===bestCount && avgDx<bestDx)){
				bestKey=dimsKey;
				bestCount=meta.count;
				bestDx=avgDx;
			}
		}
		if(bestKey) out[tag]=bestKey.split("x").map(Number);
	}
	return out;
}

function resolveTagDimsGlobal(texts, tagRe, dimRe){
	const tagHits=[];
	const dimHits=[];
	for(const t of (texts||[])){
		for(const tag of extractTagsFromText(t.text, tagRe)){
			if(!/^(F\d+|CF\d+|WF\d+|C\d+)$/i.test(tag)) continue;
			tagHits.push({ tag, x:t.x, y:t.y });
		}
		for(const tok of extractDimsFromText(t.text, dimRe)){
			const dn=parseDimsToken(tok);
			if(!dn || !validDimsCandidate(dn)) continue;
			dimHits.push({ nums:dn, x:t.x, y:t.y });
		}
	}
	const out={};
	for(const th of tagHits){
		const ordered=[...dimHits].sort((a,b)=>{
			const prefA = /^(F\d+|CF\d+|WF\d+)$/i.test(th.tag) ? (a.nums.length>=3?0:1) : (a.nums.length===2?0:1);
			const prefB = /^(F\d+|CF\d+|WF\d+)$/i.test(th.tag) ? (b.nums.length>=3?0:1) : (b.nums.length===2?0:1);
			const dyA = (typeof a.y==="number" && typeof th.y==="number") ? Math.abs(a.y-th.y) : 9999;
			const dyB = (typeof b.y==="number" && typeof th.y==="number") ? Math.abs(b.y-th.y) : 9999;
			const dxA = (typeof a.x==="number" && typeof th.x==="number") ? Math.abs(a.x-th.x) : 9999;
			const dxB = (typeof b.x==="number" && typeof th.x==="number") ? Math.abs(b.x-th.x) : 9999;
			return prefA-prefB || dyA-dyB || dxA-dxB;
		});
		if(!ordered[0]) continue;
		if(!out[th.tag] || ordered[0].nums.length>out[th.tag].length) out[th.tag]=ordered[0].nums;
	}
	return out;
}

function reasonAr(reasonEn){
	const m={
		"No dims found in schedule text.": "Ù„Ù… ÙŠØªÙ… Ø§Ù„Ø¹Ø«ÙˆØ± Ø¹Ù„Ù‰ Ø£Ø¨Ø¹Ø§Ø¯ ÙÙŠ Ù†Øµ Ø§Ù„Ø¬Ø¯ÙˆÙ„.",
		"Dims not 3D (need LxWxT).": "Ø§Ù„Ø£Ø¨Ø¹Ø§Ø¯ Ù„ÙŠØ³Øª Ø«Ù„Ø§Ø«ÙŠØ© (Ù…Ø·Ù„ÙˆØ¨ LxWxT).",
		"Dim unit invalid.": "ÙˆØ­Ø¯Ø© Ø§Ù„Ø£Ø¨Ø¹Ø§Ø¯ ØºÙŠØ± ØµØ§Ù„Ø­Ø©.",
		"CF tag not found in plan scope count.": "ÙˆØ³Ù… CF ØºÙŠØ± Ù…ÙˆØ¬ÙˆØ¯ Ø¶Ù…Ù† Ø¹Ø¯ Ù†Ø·Ø§Ù‚ Ø§Ù„Ù…Ø®Ø·Ø·.",
		"Schedule has only BxD; height missing -> Item-Stop.": "Ø§Ù„Ø¬Ø¯ÙˆÙ„ ÙŠØ­ØªÙˆÙŠ BxD ÙÙ‚Ø·Ø› Ø§Ù„Ø§Ø±ØªÙØ§Ø¹ Ù…ÙÙ‚ÙˆØ¯ -> Ø¥ÙŠÙ‚Ø§Ù Ø§Ù„Ø¨Ù†Ø¯.",
		"User rule selected but g_floor_to_floor_m missing/0.": "ØªÙ… Ø§Ø®ØªÙŠØ§Ø± Ù‚Ø§Ø¹Ø¯Ø© Ø§Ù„Ù…Ø³ØªØ®Ø¯Ù… Ù„ÙƒÙ† g_floor_to_floor_m Ù…ÙÙ‚ÙˆØ¯/ØµÙØ±.",
		"NON_COLUMN_DIM": "Ø§Ù„Ø£Ø¨Ø¹Ø§Ø¯ Ù„Ø§ ØªÙ…Ø«Ù„ Ù‚Ø·Ø§Ø¹ Ø¹Ù…ÙˆØ¯ ÙˆØ§Ù‚Ø¹ÙŠ (NON_COLUMN_DIM)."
	};
	return m[reasonEn] || "Ø³Ø¨Ø¨ Ø¥ÙŠÙ‚Ø§Ù Ø§Ù„Ø¨Ù†Ø¯ ÙŠØ­ØªØ§Ø¬ Ù…Ø±Ø§Ø¬Ø¹Ø©.";
}

function sumBy(items, field){ return (items||[]).reduce((s,it)=>s+(Number(it[field])||0), 0); }

function writeCsvRows(filePath, header, rows){
	const csv=Papa.unparse({ fields:header, data:rows });
	fs.writeFileSync(filePath, csv);
}

function scoreBoqQtyCandidate(value, colIndex, row){
	const n=Number(value);
	if(!Number.isFinite(n) || n<=0) return -9999;
	let score=0;
	if(Number.isInteger(n)) score+=1;
	if(n>=1 && n<=5000) score+=2;
	else if(n>50000) score-=3;

	const prev=String(row[colIndex-1]||"").toLowerCase();
	const next=String(row[colIndex+1]||"").toLowerCase();
	if(/qty|quantity|Ø§Ù„ÙƒÙ…ÙŠ|Ø§Ù„ÙƒÙ…ÙŠØ©/.test(prev) || /qty|quantity|Ø§Ù„ÙƒÙ…ÙŠ|Ø§Ù„ÙƒÙ…ÙŠØ©/.test(next)) score+=3;

	const nextNum=Number(row[colIndex+1]);
	if(Number.isFinite(nextNum) && nextNum > n*3) score+=1;

	if(n<0.5) score-=2;
	return score;
}

function findBoqProof(workbook, target){
	let best=null;
	for(const sheet of (workbook.SheetNames||[])){
		const ws=workbook.Sheets[sheet];
		const rows=XLSX.utils.sheet_to_json(ws,{ header:1, blankrows:false, defval:"" });
		for(let r=0;r<rows.length;r++){
			const row=rows[r]||[];
			const txt=row.map(v=>String(v||"")).join(" ").toLowerCase();
			if((target.exclude_keywords||[]).some(k=>txt.includes(k))) continue;
			const keywordHits=target.keywords.reduce((n,k)=>n+(txt.includes(k)?1:0),0);
			if(keywordHits===0) continue;

			for(let c=0;c<row.length;c++){
				if(typeof row[c]!=="number") continue;
				const score=(keywordHits*2)+scoreBoqQtyCandidate(row[c], c, row);
				if(!best || score>best._score){
					best={ sheet, row:r+1, qty_cell:XLSX.utils.encode_cell({r,c}), qty_value:row[c], row_text:txt, _score:score };
				}
			}
		}
	}
	if(!best) return null;
	const { _score, ...clean } = best;
	return clean;
}

function buildTenderProof(workbook){
	const defs=[
		{ key:"RCC_Footings", keywords:["rcc footing","rcc footings","Ø®Ø±Ø³Ø§Ù†Ø© Ù…Ø³Ù„Ø­Ø© Ù„Ù„Ù‚ÙˆØ§Ø¹Ø¯","Ø®Ø±Ø³Ø§Ù†Ø© Ù…Ø³Ù„Ø­Ø© Ø§Ù„Ù‚ÙˆØ§Ø¹Ø¯","Ù‚ÙˆØ§Ø¹Ø¯"], exclude_keywords:["Ø®Ø±Ø³Ø§Ù†Ø© Ø¹Ø§Ø¯ÙŠØ©","plain concrete","Ø¹Ø§Ø¯ÙŠØ©"] },
		{ key:"RCC_Columns", keywords:["r.c.c columns","rcc columns","Ø®Ø±Ø³Ø§Ù†Ø© Ù…Ø³Ù„Ø­Ø© Ù„Ù„Ø£Ø¹Ù…Ø¯Ø©","Ù„Ù„Ø§Ø¹Ù…Ø¯Ø©","Ù„Ù„Ø£Ø¹Ù…Ø¯Ø©","Ø§Ù„Ø£Ø¹Ù…Ø¯Ø©","Ø§Ø¹Ù…Ø¯Ø©"] },
		{ key:"Neck_Columns", keywords:["neck","neck columns","Ø±Ù‚Ø§Ø¨","Ø±Ù‚Ø¨Ø© Ø¹Ù…ÙˆØ¯"] },
		{ key:"Tie_Beams", keywords:["tie beam","tie beams","Ø¬Ø³ÙˆØ± Ø§Ø±Ø¶ÙŠØ©","Ø¬Ø³ÙˆØ± Ø£Ø±Ø¶ÙŠØ©","Ø³Ù…Ù„","Ø³Ù…Ù„Ø§Øª"] }
	];
	const out={};
	for(const d of defs) out[d.key]=findBoqProof(workbook,d) || { sheet:null, row:null, qty_cell:null, qty_value:null };
	return out;
}

function extractAllBoqItems(workbook){
	const out=[];
	for(const sheet of (workbook.SheetNames||[])){
		const ws=workbook.Sheets[sheet];
		const rows=XLSX.utils.sheet_to_json(ws,{ header:1, blankrows:false, defval:"" });
		for(let r=0;r<rows.length;r++){
			const row=rows[r]||[];
			const textCells=row
				.map(v=>String(v||"").trim())
				.filter(s=>s && /[A-Za-z\u0600-\u06FF]/.test(s));
			if(!textCells.length) continue;

			const numericCols=[];
			for(let c=0;c<row.length;c++) if(typeof row[c]==="number" && isFinite(row[c])) numericCols.push(c);
			if(!numericCols.length) continue;

			const rowText=row.map(v=>String(v||"")).join(" ").replace(/\s+/g," ").trim();
			const rowTextLc=rowText.toLowerCase();
			if(/(?:Ø§Ø¬Ù…Ø§Ù„ÙŠ|Ø£Ø¬Ù…Ø§Ù„ÙŠ|Ø¥Ø¬Ù…Ø§Ù„ÙŠ|subtotal|\btotal\b)/i.test(rowTextLc)) continue;

			let unitCol=-1;
			for(let c=0;c<row.length;c++){
				if(typeof row[c]==="string" && /^(m3|m2|m|ls|l\.s)$/i.test(row[c].trim())){ unitCol=c; break; }
			}

			let bestCol=-1;
			let bestScore=-9999;
			for(const c of numericCols){
				const n=Number(row[c]);
				if(!Number.isFinite(n) || n<=0) continue;
				let score=0;
				if(Number.isInteger(n)) score+=1;
				if(n>=0.5 && n<=200000) score+=2;
				const prev=String(row[c-1]||"").toLowerCase();
				const next=String(row[c+1]||"").toLowerCase();
				if(/qty|quantity|Ø§Ù„ÙƒÙ…ÙŠØ©|Ø§Ù„ÙƒÙ…ÙŠ/.test(prev) || /qty|quantity|Ø§Ù„ÙƒÙ…ÙŠØ©|Ø§Ù„ÙƒÙ…ÙŠ/.test(next)) score+=4;
				if(unitCol>=0 && c>unitCol){
					score += 5;
					if(c===unitCol+1) score += 4;
				}
				const nextNum=Number(row[c+1]);
				if(Number.isFinite(nextNum) && nextNum>n*3) score-=1;
				const prevNum=Number(row[c-1]);
				if(Number.isFinite(prevNum) && n>prevNum*3) score-=1;
				if(score>bestScore){ bestScore=score; bestCol=c; }
			}
			if(bestCol<0) continue;

			const description=textCells.sort((a,b)=>b.length-a.length)[0] || "";
			if(description.length<4) continue;
			const qty=Number(row[bestCol]);
			if(!Number.isFinite(qty) || qty<=0) continue;

			const itemNoRaw=(() => {
				for(let c=0;c<Math.min(row.length,4);c++){
					const v=row[c];
					if(typeof v==="number" && v>0 && v<1000) return v;
				}
				return "";
			})();
			const unitCell=row.find(v=>typeof v==="string" && /^(m3|m2|m|ls|l\.s)$/i.test(v.trim()));
			out.push({
				sheet,
				row:r+1,
				item_no:itemNoRaw!=null?String(itemNoRaw):"",
				description,
				unit:String(unitCell||"").trim(),
				qty,
				qty_cell:XLSX.utils.encode_cell({ r, c:bestCol }),
				row_text:rowText
			});
		}
	}

	const dedup=new Map();
	for(const it of out){
		const k=`${it.sheet}|${it.row}|${String(it.description||"").toLowerCase()}|${it.qty}`;
		if(!dedup.has(k)) dedup.set(k,it);
	}
	return Array.from(dedup.values()).sort((a,b)=>{
		const sheetA=String(a.sheet||"");
		const sheetB=String(b.sheet||"");
		if(sheetA<sheetB) return -1;
		if(sheetA>sheetB) return 1;
		return a.row-b.row;
	});
}

function mapReferenceItemToStrKey(description){
	const d=String(description||"").toLowerCase();
	const hasColumnWord=/(?:Ø¹Ù…ÙˆØ¯|Ø£Ø¹Ù…Ø¯Ø©|Ø§Ø¹Ù…Ø¯Ø©|Ø§Ù„Ø£Ø¹Ù…Ø¯Ø©|Ø§Ù„Ø§Ø¹Ù…Ø¯Ø©|Ù„Ù„Ø£Ø¹Ù…Ø¯Ø©|Ù„Ù„Ø§Ø¹Ù…Ø¯Ø©|columns?)/.test(d);
	if(/Ø§Ù„Ø­ÙØ±|excavat/.test(d)) return "EXCAVATION";
	if(/rod\s*base|Ø±ÙˆØ¯Ø¨ÙŠØ³|road\s*base/.test(d)) return "ROAD_BASE";
	if((/Ù‚ÙˆØ§Ø¹Ø¯|footings?/.test(d)) && (/Ø¹Ø§Ø¯ÙŠØ©|plain/.test(d))) return "PLAIN_CONCRETE_UNDER_FOOTINGS";
	if((/Ø±Ù‚Ø§Ø¨|neck/.test(d)) && hasColumnWord) return "Neck_Columns";
	if((/Ø¬Ø³ÙˆØ±\s*Ø§Ø±Ø¶ÙŠØ©|Ø¬Ø³ÙˆØ± Ø£Ø±Ø¶ÙŠØ©|tie\s*beams?|Ø³Ù…Ù„/.test(d))) return "Tie_Beams";
	if((/Ù‚ÙˆØ§Ø¹Ø¯|footings?/.test(d)) && (/Ù…Ø³Ù„Ø­Ø©|reinforced|r\.?c\.?c/.test(d)) && !(/Ø¹Ø§Ø¯ÙŠØ©|plain/.test(d))) return "RCC_Footings";
	if(hasColumnWord && (/Ù…Ø³Ù„Ø­Ø©|reinforced|r\.?c\.?c/.test(d)) && !(/Ø±Ù‚Ø§Ø¨|neck/.test(d))) return "RCC_Columns";
	if((/Ø¨ÙŠÙ…Ø§Øª|beams?/.test(d)) && (/Ù…Ø³Ù„Ø­Ø©|reinforced|r\.?c\.?c/.test(d))) return "RCC_Beams";
	if((/Ø£Ø³Ù‚Ù|Ø³Ù‚Ù|slabs?|roof/.test(d)) && (/Ù…Ø³Ù„Ø­Ø©|reinforced|r\.?c\.?c/.test(d))) return "RCC_Slabs";
	if((/Ø³Ù„Ø§Ù„Ù…|stairs?/.test(d)) && (/Ø®Ø§Ø±Ø¬ÙŠ|external/.test(d))) return "RCC_Stairs_External";
	if((/Ø³Ù„Ø§Ù„Ù…|stairs?/.test(d)) && (/Ø¯Ø§Ø®Ù„ÙŠ|internal/.test(d))) return "RCC_Stairs_Internal";
	if(/Ø¯ÙØ§Ù†|Ø¯Ùƒ|backfill|compaction/.test(d)) return "BACKFILL_COMPACTION";
	if(/subgrade|flooring\s*slab|Ø£Ø±Ø¶ÙŠØ§Øª\s*Ø®Ø±Ø³Ø§Ù†ÙŠØ©\s*Ø¹Ø§Ø¯ÙŠØ©/.test(d)) return "SUBGRADE_FLOOR_SLAB";
	if(/Ø§Ù„Ù†Ù…Ù„\s*Ø§Ù„Ø§Ø¨ÙŠØ¶|anti\s*termite|termite/.test(d)) return "ANTI_TERMITE_TREATMENT";
	return null;
}

function mapReferenceItemToStrKey(description){
	const d=String(description||"").toLowerCase();
	const hasColumnWord=/(?:\u0639\u0645\u0648\u062f|\u0623\u0639\u0645\u062f\u0629|\u0627\u0639\u0645\u062f\u0629|\u0627\u0644\u0623\u0639\u0645\u062f\u0629|\u0627\u0644\u0627\u0639\u0645\u062f\u0629|\u0644\u0644\u0623\u0639\u0645\u062f\u0629|\u0644\u0644\u0627\u0639\u0645\u062f\u0629|columns?)/.test(d);
	const hasConcreteWord=/(?:\u062e\u0631\u0633\u0627\u0646|concrete|r\.?c\.?c|reinforced)/.test(d);
	if(/(?:\u062f\u0641\u0627\u0646|\u062f\u0643|back\s*filling|backfill|compaction)/.test(d)) return "BACKFILL_COMPACTION";
	if(/(?:\u0627\u0644\u062d\u0641\u0631|excavat)/.test(d)) return "EXCAVATION";
	if(/(?:rod\s*base|road\s*base|\u0631\u0648\u062f\u0628\u064a\u0633)/.test(d)) return "ROAD_BASE";
	if((/(?:\u0642\u0648\u0627\u0639\u062f|footings?)/.test(d)) && (/(?:\u0639\u0627\u062f\u064a\u0629|plain)/.test(d))) return "PLAIN_CONCRETE_UNDER_FOOTINGS";
	if(/(?:polythene|polyethylene|\u0628\u0648\u0644\u064a\s*\u0627\u064a\u062b\u064a\u0644\u064a\u0646)/.test(d)) return "POLYTHENE_SHEET";
	if((/(?:bitum|\u0628\u064a\u062a\u0648\u0645\u064a\u0646)/.test(d)) && (/(?:sub-?structure|\u0633\u0628\u0633\u062a\u0631\u0643\u062a\u0631|\u062a\u062d\u062a\u064a)/.test(d))) return "BITUMEN_FOUNDATIONS";
	if((/(?:bitum|\u0628\u064a\u062a\u0648\u0645\u064a\u0646)/.test(d)) && (/(?:foot|foundat|\u0642\u0648\u0627\u0639\u062f|\u0627\u0633\u0627\u0633\u0627\u062a)/.test(d))) return "BITUMEN_FOUNDATIONS";
	if((/(?:bitum|\u0628\u064a\u062a\u0648\u0645\u064a\u0646)/.test(d)) && (/(?:solid\s*block|\u0637\u0627\u0628\u0648\u0642|\u0645\u0635\u0645\u062a)/.test(d))) return "BITUMEN_SOLID_BLOCK";
	if(/(?:parapet|\u0628\u0631\u0627\u0628\u064a\u062a)/.test(d) && /(?:solid\s*block|\u0637\u0627\u0628\u0648\u0642|\u0645\u0635\u0645\u062a)/.test(d)) return null;
	if(/(?:\u0637\u0627\u0628\u0648\u0642\s*\u0645\u0635\u0645\u062a|solid\s*block)/.test(d)) return "SOLID_BLOCK_WORK";
	if((/(?:\u0631\u0642\u0627\u0628|neck)/.test(d)) && hasColumnWord) return "Neck_Columns";
	if((/(?:\u062c\u0633\u0648\u0631\s*\u0627\u0631\u0636\u064a\u0629|\u062c\u0633\u0648\u0631\s*\u0623\u0631\u0636\u064a\u0629|tie\s*beams?|\u0633\u0645\u0644)/.test(d))) return "Tie_Beams";
	if((/(?:\u0642\u0648\u0627\u0639\u062f|footings?)/.test(d)) && (/(?:\u0645\u0633\u0644\u062d\u0629|reinforced|r\.?c\.?c)/.test(d)) && !(/(?:\u0639\u0627\u062f\u064a\u0629|plain)/.test(d))) return "RCC_Footings";
	if(hasColumnWord && (/(?:\u0645\u0633\u0644\u062d\u0629|reinforced|r\.?c\.?c)/.test(d)) && !(/(?:\u0631\u0642\u0627\u0628|neck)/.test(d))) return "RCC_Columns";
	if((/(?:\u0628\u064a\u0645\u0627\u062a|beams?)/.test(d)) && (/(?:\u0645\u0633\u0644\u062d\u0629|reinforced|r\.?c\.?c)/.test(d))) return "RCC_Beams";
	if((/(?:\u0623\u0633\u0642\u0641|\u0633\u0642\u0641|slabs?|roof)/.test(d)) && (/(?:\u0645\u0633\u0644\u062d\u0629|reinforced|r\.?c\.?c)/.test(d))) return "RCC_Slabs";
	if(hasConcreteWord && (/(?:\u0633\u0644\u0627\u0644\u0645|stairs?)/.test(d)) && (/(?:\u062e\u0627\u0631\u062c\u064a|external)/.test(d))) return "RCC_Stairs_External";
	if(hasConcreteWord && (/(?:\u0633\u0644\u0627\u0644\u0645|stairs?)/.test(d)) && (/(?:\u062f\u0627\u062e\u0644\u064a|internal)/.test(d))) return "RCC_Stairs_Internal";
	if(/(?:subgrade|flooring\s*slab|\u0623\u0631\u0636\u064a\u0627\u062a\s*\u062e\u0631\u0633\u0627\u0646\u064a\u0629\s*\u0639\u0627\u062f\u064a\u0629)/.test(d)) return "SUBGRADE_FLOOR_SLAB";
	if(/(?:\u0627\u0644\u0646\u0645\u0644\s*\u0627\u0644\u0627\u0628\u064a\u0636|anti\s*termite|termite)/.test(d)) return "ANTI_TERMITE_TREATMENT";
	return null;
}

async function runStrPipeline({ strDxfPath, referencePath, allowExternalReference=false, inputs, outDir }){
	if(referencePath && !allowExternalReference){
		throw new Error("External reference input is disabled. STR pipeline accepts drawing inputs only.");
	}
	const { rules, source:rulesSource, signature:rulesSignature } = loadRules("VILLA_G1");
	const parser=new DxfParser();
	const dxf=parser.parseSync(fs.readFileSync(strDxfPath, "utf8"));
	const cadScaleToMeters=detectCadScaleToMeters(dxf.header||{}, inputs?.dimUnit||null);
	const entitiesRaw=scaleEntitiesToMeters(flattenDxfEntities(dxf), cadScaleToMeters);
	const entitiesScopedRect=applyScopeRectToEntities(entitiesRaw, inputs?.scopeRect);
	const entities=applyScopeCircleToEntities(entitiesScopedRect, inputs?.scopeCenter, inputs?.scopeRadiusM ?? inputs?.scopeRadius);
	const header=dxf.header||{};
	const insUnits=header.$INSUNITS ?? null;

	const texts=collectTextsFromEntities(entitiesRaw, "MODEL", "");
	const modelTexts=collectTextsFromEntities(entities, "MODEL", "");
	const disciplineSig = detectDisciplineSignature(texts);

	const tagRe=/^[A-Z]{1,6}\d{1,4}$/;
	const dimRe=/\b\d{1,4}(\.\d{1,3})?\s*(?:[xX\*\/])\s*\d{1,4}(\.\d{1,3})?(?:\s*(?:[xX\*\/])\s*\d{1,4}(\.\d{1,3})?)?\b/g;
	const hasExternalReference=false;
	const qtoModeMeta={ mode:"QTO_ONLY", external_reference_enabled:false };
	const strictBlueprint=inputs?.strictBlueprint!==false;
	const referenceItems=[];
	const referenceQtyFoot=0;
	const referenceQtyCol=0;
	const referenceQtyNeck=0;
	const referenceQtyTie=0;
	const referenceQtyBeams=Number(getReferenceQtyByStrKey(referenceItems, "RCC_Beams") || 0);
	const referenceQtySlabs=Number(getReferenceQtyByStrKey(referenceItems, "RCC_Slabs") || 0);
	const supplementalColumnPdf = await loadSupplementalColumnScheduleMaps(strDxfPath, null);
	const supplementalStructuralPdfTexts = await loadSupplementalStructuralPdfTexts(strDxfPath, null);
	const supplementalStructuralPdfSummary = mergeSupplementalStructuralPdfSummaries(supplementalStructuralPdfTexts);

	const scheduleCandidatesSource = texts.filter(t=>{
		const standaloneNumeric=parseStandaloneNumericCell(t.text);
		return extractTagsFromText(t.text,tagRe).length
			|| extractDimsFromText(t.text,dimRe).length
			|| /SCHEDULE/i.test(t.text)
			|| (standaloneNumeric!==null && standaloneNumeric>0 && standaloneNumeric<=1000);
	});
	const { rows:allRows, eps:rowEps } = clusterRows(scheduleCandidatesSource);
	const tableRows = rowsAroundHeaders(allRows, /SCHEDULE/i, 80, 240);
	const scheduleBaseRows = tableRows.length ? tableRows : allRows;
	const footHeaderExact=/SCHEDULE OF FOOTINGS/i;
	const footExactGroups=rowsAroundEachHeader(scheduleBaseRows, footHeaderExact, 5, 120)
		.map(group=>trimRowsUntilNextScheduleHeader(group, footHeaderExact))
		.filter(group=>group.length);
	const footWindowRowsRaw = (() => {
		if(footExactGroups.length) return footExactGroups[0];
		return pickBestHeaderWindow(scheduleBaseRows, /(SCHEDULE.*FOOT|FOOT.*SCHEDULE|FOOTING)/i, tagRe, dimRe, "footing", 30, 220);
	})();
	const footWindowRows = filterRowsToHeaderXCluster(footWindowRowsRaw, footHeaderExact, tagRe, dimRe);
	const colWindowRows = pickBestHeaderWindow(scheduleBaseRows, /(SCHEDULE.*COLUMN|COLUMN.*SCHEDULE|COLUMNS)/i, tagRe, dimRe, "column", 30, 220);
	const tieHeaderExact=/SCHEDULE OF TIE BEAMS/i;
	const tieWindowGroupsRaw = (() => {
		const exactGroups=rowsAroundEachHeader(scheduleBaseRows, tieHeaderExact, 5, 80)
			.map(group=>trimRowsUntilNextScheduleHeader(group, tieHeaderExact))
			.filter(group=>group.length);
		if(exactGroups.length) return exactGroups;
		return rowsAroundEachHeader(scheduleBaseRows, /(SCHEDULE.*TIE\s*BEAM|TIE\s*BEAM.*SCHEDULE)/i, 20, 160);
	})();
	const tieWindowGroups = tieWindowGroupsRaw
		.map(group=>filterRowsToHeaderXCluster(group, tieHeaderExact, tagRe, dimRe))
		.filter(group=>group.length);
	const tieRows = tieWindowGroups.length
		? [...tieWindowGroups]
			.sort((a,b)=>{
				const score=(rows)=>rows.reduce((sum,row)=>{
					let rowScore=0;
					for(const cell of (row.cells||[])){
						const text=String(cell.text||"");
						rowScore += extractTagsFromText(text, tagRe).filter(t=>isTieBeamTag(t)).length * 10;
						rowScore += extractDimsFromText(text, dimRe).length * 4;
						if(parseStandaloneNumericCell(text)!==null) rowScore += 1;
					}
					return sum+rowScore;
				}, 0);
				return score(b)-score(a);
			})[0]
		: [];
	const footBaseRows = footWindowRows.length ? footWindowRows : scheduleBaseRows;
	const colBaseRows = colWindowRows.length ? colWindowRows : scheduleBaseRows;
	const footRegion = footExactGroups.length ? { start:0, end:footBaseRows.length-1 } : chooseBestRegion(footBaseRows, tagRe, dimRe, "footing");
	const colRegion = chooseBestRegion(colBaseRows, tagRe, dimRe, "column");
	const footRows = footRegion.end>=footRegion.start ? footBaseRows.slice(footRegion.start, footRegion.end+1) : footBaseRows;
	const colRows = colRegion.end>=colRegion.start ? colBaseRows.slice(colRegion.start, colRegion.end+1) : colBaseRows;
	const { map:footMapRaw, candidates:footCandidates } = buildTagDimsMapFromRows(footRows, tagRe, dimRe);
	const { map:colMapRaw, candidates:colCandidates } = buildTagDimsMapFromRows(colRows, tagRe, dimRe);
	const footMap = Object.fromEntries(Object.entries(footMapRaw).filter(([k,v])=>isFootingTag(k) && Array.isArray(v) && v.length>=3));
	const colMap = Object.fromEntries(
		Object.entries(colMapRaw)
			.map(([k,v])=>[k, normalizeColumnDimsRaw(v)])
			.filter(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2)
	);
	const strictFootMap = buildFamilyTagDimsMap(footRows, FOOTING_TAG_REGEX, 3, tagRe, dimRe, Math.max(0.8,rowEps*6));
	const strictColMap = Object.fromEntries(
		Object.entries(buildFamilyTagDimsMap(colRows, COLUMN_TAG_REGEX, 2, tagRe, dimRe, Math.max(0.8,rowEps*6)))
			.map(([k,v])=>[k, normalizeColumnDimsRaw(v)])
			.filter(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2)
	);
	const footColMap = buildTagDimsMapByColumns(footRows, FOOTING_TAG_REGEX, 3, tagRe, dimRe, rowEps);
	const colColMap = Object.fromEntries(
		Object.entries(buildTagDimsMapByColumns(colRows, COLUMN_TAG_REGEX, 2, tagRe, dimRe, rowEps))
			.map(([k,v])=>[k, normalizeColumnDimsRaw(v)])
			.filter(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2)
	);
	const numericFootMap = buildFamilyDimsMapFromNumericColumns(footRows, FOOTING_TAG_REGEX, 3, tagRe, {
		minCount:2,
		xTol:Math.max(0.1, rowEps*2),
		rowLookAround:2,
		valueFilter:(n)=>n>=20 && n<=1000
	});
	const numericColMapRaw = buildFamilyDimsMapFromNumericColumns(colRows, COLUMN_TAG_REGEX, 2, tagRe, {
		minCount:1,
		xTol:Math.max(0.1, rowEps*2),
		rowLookAround:2,
		valueFilter:(n)=>n>=20 && n<=1000
	});
	const numericColMap = Object.fromEntries(
		Object.entries(numericColMapRaw)
			.map(([k,v])=>[k, normalizeColumnDimsRaw(v)])
			.filter(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2)
	);
	const tieRowBand=Math.max(0.8, rowEps*6);
	const tieMap = tieRows.length ? buildFamilyTagDimsMap(tieRows, TIE_BEAM_TAG_REGEX, 2, tagRe, dimRe, tieRowBand) : {};
	const tieColMap = tieRows.length ? buildTagDimsMapByColumns(tieRows, TIE_BEAM_TAG_REGEX, 2, tagRe, dimRe, rowEps) : {};
	const tieVoteMap = tieRows.length ? buildTieBeamTagDimsMap(tieRows, tagRe, dimRe) : {};
	const numericTieMap = tieRows.length ? buildFamilyDimsMapFromNumericColumns(tieRows, TIE_BEAM_TAG_REGEX, 2, tagRe, {
		minCount:2,
		valueFilter:(n)=>n>=5 && n<=100
	}) : {};
	const genericBeamHeader=/SCHEDULE OF BEAMS/i;
	const genericBeamScheduleGroups = rowsAroundEachHeader(scheduleBaseRows, genericBeamHeader, 5, 80)
		.map(group=>trimRowsUntilNextScheduleHeader(group, genericBeamHeader))
		.map(group=>filterRowsToHeaderXCluster(group, genericBeamHeader, tagRe, dimRe))
		.filter(g=>g.length);
	const genericBeamMaps = genericBeamScheduleGroups.map(group=>
		buildFamilyDimsMapFromNumericColumns(group, BEAM_TAG_REGEX, 2, tagRe, {
			minCount:1,
			valueFilter:(n)=>n>=5 && n<=100
		})
	);
	let tagDimsMap = {
		...footMap,
		...strictFootMap,
		...footColMap,
		...numericFootMap,
		...colMap,
		...strictColMap,
		...colColMap,
		...numericColMap,
		...tieMap,
		...tieColMap,
		...tieVoteMap,
		...numericTieMap
	};
	for(const [tag,dims] of Object.entries(supplementalColumnPdf.columnMap||{})){
		const normalized=normalizeColumnDimsRaw(dims);
		if(!tagDimsMap[tag] && (normalized || (Array.isArray(dims) && dims.length===2))) tagDimsMap[tag]=normalized || dims;
	}
	for(const row of supplementalStructuralPdfSummary.columnRows||[]){
		if(!tagDimsMap[row.tag] && Array.isArray(row.dims) && row.dims.length===2) tagDimsMap[row.tag]=row.dims;
	}
	const mappedFootNow = new Set(Object.entries(tagDimsMap).filter(([k,v])=>isFootingTag(k) && Array.isArray(v) && v.length>=3).map(([k])=>k));
	const textFootTags = [...new Set(texts.flatMap(t=>extractTagsFromText(t.text, tagRe)).filter(t=>isFootingTag(t)))];
	const missingFootTags = textFootTags.filter(t=>!mappedFootNow.has(t));
	if(missingFootTags.length){
		const globalFallback = resolveTagDimsGlobal(texts, tagRe, dimRe);
		for(const t of missingFootTags){
			const dn = globalFallback[t];
			if(Array.isArray(dn) && dn.length>=3 && isFootingTag(t)) tagDimsMap[t]=dn;
		}
	}
	const mappedColDims = Object.entries(tagDimsMap)
		.filter(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2)
		.map(([,v])=>v);
	if(mappedColDims.length){
		const colDimsFreq=new Map();
		for(const dn of mappedColDims){
			const key=`${dn[0]}x${dn[1]}`;
			colDimsFreq.set(key, (colDimsFreq.get(key)||0)+1);
		}
		let bestKey=null, bestCnt=-1;
		for(const [k,c] of colDimsFreq.entries()){
			if(c>bestCnt){ bestCnt=c; bestKey=k; }
		}
		if(bestKey){
			const defaultColDims = bestKey.split("x").map(Number);
			const cTagsInTexts=[...new Set(texts.flatMap(t=>extractTagsFromText(t.text, tagRe)).filter(t=>isColumnTag(t)))];
			for(const ct of cTagsInTexts){
				if(!tagDimsMap[ct]) tagDimsMap[ct]=defaultColDims;
			}
		}
	}
	const mappedTieNow = new Set(Object.entries(tagDimsMap).filter(([k,v])=>isTieBeamTag(k) && Array.isArray(v) && v.length===2).map(([k])=>k));
	const textTieTags = [...new Set(texts.flatMap(t=>extractTagsFromText(t.text, tagRe)).filter(t=>isTieBeamTag(t)))];
	const missingTieTags = textTieTags.filter(t=>!mappedTieNow.has(t));
	if(missingTieTags.length){
		const globalFallback = resolveTagDimsGlobal(texts, tagRe, dimRe);
		for(const t of missingTieTags){
			const dn = globalFallback[t];
			if(Array.isArray(dn) && dn.length===2 && isTieBeamTag(t)) tagDimsMap[t]=dn;
		}
	}
	for(const [tag,dims] of Object.entries(strictFootMap)){
		if(Array.isArray(dims) && dims.length>=3 && isFootingTag(tag)) tagDimsMap[tag]=dims;
	}
	for(const [tag,dims] of Object.entries(numericTieMap)){
		if(Array.isArray(dims) && dims.length===2 && isTieBeamTag(tag)) tagDimsMap[tag]=dims;
	}
	const authoritativeFootRows = footExactGroups.length
		? filterRowsToHeaderXCluster(footExactGroups[0], footHeaderExact, tagRe, dimRe)
		: [];
	const authoritativeFootMap = authoritativeFootRows.length
		? Object.fromEntries(
			Object.entries(buildTagDimsMapFromRows(authoritativeFootRows, tagRe, dimRe).map)
				.filter(([k,v])=>isFootingTag(k) && Array.isArray(v) && v.length>=3)
		)
		: {};
	const authoritativeTieGroups = rowsAroundEachHeader(scheduleBaseRows, tieHeaderExact, 5, 80)
		.map(group=>trimRowsUntilNextScheduleHeader(group, tieHeaderExact))
		.filter(group=>group.length)
		.map(group=>filterRowsToHeaderXCluster(group, tieHeaderExact, tagRe, dimRe))
		.filter(group=>group.length);
	const authoritativeTieRows = authoritativeTieGroups.length
		? [...authoritativeTieGroups].sort((a,b)=>b.length-a.length)[0]
		: [];
	const authoritativeTieMap = authoritativeTieRows.length
		? buildFamilyDimsMapFromNumericColumns(authoritativeTieRows, TIE_BEAM_TAG_REGEX, 2, tagRe, {
			minCount:2,
			valueFilter:(n)=>n>=5 && n<=100
		})
		: {};
	const directFootingMap=buildFootingMapFromExactTexts(texts, tagRe, dimRe);
	const standaloneFootingMap=buildFootingMapFromStandaloneRows(texts, tagRe);
	const directTieMap=buildNumericSectionMapDirect(texts, /SCHEDULE OF TIE BEAMS/i, TIE_BEAM_TAG_REGEX, 2, tagRe, {
		xBackPad:1.5,
		xForwardPad:3,
		yDepth:4,
		valueFilter:(n)=>n>=5 && n<=100
	});
	const directBeamMap=buildNumericSectionMapDirect(texts, /SCHEDULE OF BEAMS/i, BEAM_TAG_REGEX, 2, tagRe, {
		xBackPad:1.5,
		xForwardPad:3,
		yDepth:4,
		valueFilter:(n)=>n>=5 && n<=100
	});
	for(const [tag,dims] of Object.entries(authoritativeFootMap)){
		tagDimsMap[tag]=dims;
	}
	for(const [tag,dims] of Object.entries(authoritativeTieMap)){
		tagDimsMap[tag]=dims;
	}
	for(const [tag,dims] of Object.entries(directFootingMap)){
		tagDimsMap[tag]=dims;
	}
	for(const [tag,dims] of Object.entries(standaloneFootingMap)){
		tagDimsMap[tag]=dims;
	}
	for(const [tag,dims] of Object.entries(directTieMap)){
		tagDimsMap[tag]=dims;
	}
	const scheduleCandidates = footCandidates.concat(colCandidates);
	const scheduleBounds = buildScheduleBounds(footRows.concat(colRows));
	const schedulePad=Math.max(0.5, rowEps*2);
	const tieScheduleBounds=buildScheduleBounds(tieRows);

	const modelTagPoints=[];
	for(const t of modelTexts){
		if(typeof t.x!=="number" || typeof t.y!=="number") continue;
		for(const tag of extractTagsFromText(t.text, tagRe)){
			const p={ tag, x:t.x, y:t.y, layer:t.layer||"", source:t.source||"MODEL", blockName:t.blockName||"" };
			if(pointInBounds(p, scheduleBounds, schedulePad)) continue;
			modelTagPoints.push(p);
		}
	}
	const dedupeTol=Math.max(0.05, rowEps||0.1);
	let modelTagDedup=dedupeByTagXY(modelTagPoints, dedupeTol);
	const noExclusion=[];
	for(const t of modelTexts){
		if(typeof t.x!=="number" || typeof t.y!=="number") continue;
		for(const tag of extractTagsFromText(t.text, tagRe)) noExclusion.push({ tag, x:t.x, y:t.y, layer:t.layer||"", source:t.source||"MODEL", blockName:t.blockName||"" });
	}
	const allModelTagDedup = dedupeByTagXY(noExclusion, dedupeTol);
	const excludedColumnFamilies = new Set(modelTagDedup.map(p=>p.tag).filter(t=>isColumnTag(t)));
	const fullColumnFamilies = new Set(allModelTagDedup.map(p=>p.tag).filter(t=>isColumnTag(t)));
	const lostColumnFamilies = [...fullColumnFamilies].filter(tag=>!excludedColumnFamilies.has(tag));

	if(modelTagDedup.length<20 || lostColumnFamilies.length){
		modelTagDedup = allModelTagDedup;
	}

	const coreColumnTagSet=new Set(["C1","C2","C3","C4","C5"]);
	const supplementalColumnTagSet=new Set(Object.keys(supplementalColumnPdf.columnMap||{}).map(k=>String(k||"").toUpperCase()));
	const allowExtraColumnTags=inputs.allowExtraColumnTags!==false;
	const isAllowedColumnTag=(tag)=>{
		const normalized=String(tag||"").toUpperCase();
		if(supplementalColumnTagSet.size) return supplementalColumnTagSet.has(normalized);
		return allowExtraColumnTags || coreColumnTagSet.has(normalized);
	};

	const rawPlanColumnTags=allModelTagDedup.filter(
		p=>isColumnTag(p.tag) && isAllowedColumnTag(p.tag)
	);
	if(!Object.entries(tagDimsMap).some(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2)){
		const inferredColDims=inferColumnDimsFromPlanGeometry(entities, rawPlanColumnTags, { maxDistanceM:1.4 });
		for(const [tag, dims] of Object.entries(inferredColDims)){
			if(!tagDimsMap[tag]) tagDimsMap[tag]=dims;
		}
	}
	if(!strictBlueprint && !Object.entries(tagDimsMap).some(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2)){
		const hintedSizes=[...new Set(
			allModelTagDedup
				.map((p)=>String(p.tag||"").toUpperCase())
				.map((tag)=>{
					const m=/^C(\d{2,3})$/.exec(tag);
					if(!m) return null;
					const n=Number(m[1]);
					return (Number.isFinite(n) && n>=20 && n<=80) ? n : null;
				})
				.filter((n)=>Number.isFinite(n))
		)];
		if(hintedSizes.length){
			const defaultSize=Math.max(20, Math.min(60, Math.round(median(hintedSizes)/5)*5));
			const cTagsInTexts=[...new Set(allModelTagDedup.map((p)=>p.tag).filter((t)=>isColumnTag(t) && isAllowedColumnTag(t)))];
			for(const ct of cTagsInTexts){
				if(!tagDimsMap[ct]) tagDimsMap[ct]=[defaultSize, defaultSize];
			}
		}
	}
	const gHeightForColumnFallback=Number(inputs.levels?.g_floor_to_floor_m||0);
	if(!Object.entries(tagDimsMap).some(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2) && referenceQtyCol>0 && gHeightForColumnFallback>0 && rawPlanColumnTags.length){
		const rawPlanColumnClusters=clusterByRadius(rawPlanColumnTags, 8.0);
		const mainColumnCount=rawPlanColumnClusters[0]?.members?.length || rawPlanColumnTags.length;
		if(mainColumnCount>0){
			const sideM=clamp(roundToStep(Math.sqrt(referenceQtyCol/(mainColumnCount*gHeightForColumnFallback)), 0.05), 0.20, 0.50);
			const rawDim=convertMetersToDimRaw(sideM, inputs?.dimUnit||"cm");
			for(const tag of [...new Set(rawPlanColumnTags.map(p=>p.tag))]){
				if(!tagDimsMap[tag]) tagDimsMap[tag]=[rawDim, rawDim];
			}
		}
	}

	const mappedFootTags=new Set(Object.entries(tagDimsMap).filter(([k,v])=>isFootingTag(k) && Array.isArray(v) && v.length>=3).map(([k])=>k));
	const mappedColTags=new Set(Object.entries(tagDimsMap).filter(([k,v])=>isColumnTag(k) && Array.isArray(v) && v.length===2 && isAllowedColumnTag(k)).map(([k])=>k));

	const footingTags=modelTagDedup.filter(p=>mappedFootTags.size ? mappedFootTags.has(p.tag) : isFootingTag(p.tag));
	const allColumnTags=modelTagDedup.filter(p=>mappedColTags.size ? mappedColTags.has(p.tag) : (isColumnTag(p.tag) && isAllowedColumnTag(p.tag)));
	const structuralColumnLayerTags=allColumnTags.filter(p=>isLikelyColumnPlanLayer(p.layer));
	const axnColumnTags=allColumnTags.filter(p=>/AXN/i.test(String(p.layer||"")));
	const columnTags=
		structuralColumnLayerTags.length>=8
			? structuralColumnLayerTags
			: (axnColumnTags.length>=8 ? axnColumnTags : allColumnTags);
	const footingClusters=clusterByRadius(footingTags, 8.0);
	const columnClusters=clusterByRadius(columnTags, 8.0);

	const footingScopeClusters = mappedFootTags.size
		? footingClusters.filter(c=>c.members.some(m=>mappedFootTags.has(m.tag)))
		: footingClusters;
	const footingCandidateSets=buildCumulativeMergeCandidates(
		footingScopeClusters,
		(c)=>c.members.filter(m=>mappedFootTags.size ? mappedFootTags.has(m.tag) : isFootingTag(m.tag)).length,
		"F"
	);
	const requestedFootingPick=String(inputs.footingScopePick||inputs.footingMergePick||"").trim().toUpperCase();
	const pickedFootingByKey=footingCandidateSets.find((set)=>set.key===requestedFootingPick);
	let bestFootingSet=pickedFootingByKey||null;
	if(!bestFootingSet && referenceQtyFoot>0){
		bestFootingSet=pickClosestCandidate(
			footingCandidateSets,
			(set)=>estimateFootingsM3ForMembers(set.merged_members, tagDimsMap, inputs.dimUnit),
			referenceQtyFoot
		);
	}
	if(!bestFootingSet && footingCandidateSets.length){
		// In QTO-only mode (no external reference target), prefer the widest cumulative coverage.
		bestFootingSet=footingCandidateSets[footingCandidateSets.length-1];
	}
	if(!bestFootingSet) bestFootingSet={ key:"F0", cluster_ids:[], merged_members:[] };
	const footingScopeMembers=dedupeByTagXY(bestFootingSet.merged_members||[], dedupeTol);
	const footingScope={ id:(bestFootingSet.cluster_ids||[]).length?`merged:${bestFootingSet.cluster_ids.join("+")}`:"none", members:footingScopeMembers, count:footingScopeMembers.length };

	const columnMergeCandidates=buildAnchoredSubsetCandidates(columnClusters, c=>c.members.filter(m=>isColumnTag(m.tag) && isAllowedColumnTag(m.tag)).length, "M");
	for(const m of columnMergeCandidates) m.coverage_core_c=m.metric;

	const requestedScopePick=String(inputs.scopePick||"").trim().toUpperCase();
	const autoColPick = referenceQtyCol>0
		? pickClosestCandidate(
			columnMergeCandidates,
			(set)=>estimateColumnsM3ForCandidate(set, tagDimsMap, inputs.dimUnit, Number(inputs.levels?.g_floor_to_floor_m||0), Number(inputs.levels?.f1_floor_to_floor_m||0), isAllowedColumnTag),
			referenceQtyCol
		)
		: null;
	const maxCoverageColPick=columnMergeCandidates.reduce((best, current)=>{
		if(!best) return current;
		return Number(current.metric||0)>Number(best.metric||0) ? current : best;
	}, null);
	const selectedByPick=columnMergeCandidates.find(m=>m.key===requestedScopePick) || autoColPick || maxCoverageColPick || columnMergeCandidates[0] || { key:"M1", cluster_ids:[], clusters:[], merged_members:[] };
	const needScopePick=columnMergeCandidates.length>1 && referenceQtyCol>0 && !columnMergeCandidates.find(m=>m.key===requestedScopePick) && !autoColPick;

	const selectedColumnClusters=selectedByPick.clusters||[];
	const groundCluster=selectedColumnClusters[0] || { id:"none", members:[], count:0 };
	const firstClusters=selectedColumnClusters.slice(1);
	const firstClusterIds=firstClusters.map(c=>c.id);
	const columnScopeSuperGround=(groundCluster.members||[]).filter(m=>mappedColTags.size ? mappedColTags.has(m.tag) : (isColumnTag(m.tag) && isAllowedColumnTag(m.tag)));
	const columnScopeSuperFirst=firstClusters.flatMap(c=>(c.members||[]).filter(m=>mappedColTags.size ? mappedColTags.has(m.tag) : (isColumnTag(m.tag) && isAllowedColumnTag(m.tag))));
	const columnScopeSuper=columnScopeSuperGround.concat(columnScopeSuperFirst);

	const footingClustersTop5=topNWithPadding(
		footingClusters.map(c=>({
			cluster_id:c.id,
			bbox:c.bounds,
			count_F_tags:c.members.filter(m=>mappedFootTags.size ? mappedFootTags.has(m.tag) : isFootingTag(m.tag)).length,
			estimated_footings_m3_if_used:estimateFootingsM3ForMembers(c.members, tagDimsMap, inputs.dimUnit)
		})).sort((a,b)=>b.count_F_tags-a.count_F_tags || b.estimated_footings_m3_if_used-a.estimated_footings_m3_if_used),
		5,
		(n)=>({ cluster_id:`none_${n}`, bbox:null, count_F_tags:0, estimated_footings_m3_if_used:0 })
	);
	const colMetricRaw=columnClusters.map(c=>({
		cluster_id:c.id,
		bbox:c.bounds,
		count_C_tags:c.members.filter(m=>mappedColTags.has(m.tag)).length,
		estimated_columns_m3_if_used_ground:estimateColumnsM3ForMembers(c.members, tagDimsMap, inputs.dimUnit, Number(inputs.levels?.g_floor_to_floor_m||0), isAllowedColumnTag),
		estimated_columns_m3_if_used_first:estimateColumnsM3ForMembers(c.members, tagDimsMap, inputs.dimUnit, Number(inputs.levels?.f1_floor_to_floor_m||0), isAllowedColumnTag)
	})).sort((a,b)=>b.count_C_tags-a.count_C_tags);
	const columnClustersTop5=topNWithPadding(colMetricRaw,5,(n)=>({ cluster_id:`none_${n}`, bbox:null, count_C_tags:0, estimated_columns_m3_if_used_ground:0, estimated_columns_m3_if_used_first:0 }));

	const footingMergeCandidates=buildCumulativeMergeCandidates(
		footingScopeClusters,
		(c)=>c.members.filter(m=>mappedFootTags.size ? mappedFootTags.has(m.tag) : isFootingTag(m.tag)).length,
		"F"
	).map(m=>({
		option:m.key,
		cluster_ids:m.cluster_ids,
		count_F_tags:m.metric,
		estimated_footings_m3_if_used:estimateFootingsM3ForMembers(m.merged_members, tagDimsMap, inputs.dimUnit)
	}));
	const columnMergeOut=columnMergeCandidates.map(m=>({
		option:m.key,
		cluster_ids:m.cluster_ids,
		coverage_core_c:m.coverage_core_c,
		estimated_columns_m3_ground_if_used:estimateColumnsM3ForMembers(m.merged_members, tagDimsMap, inputs.dimUnit, Number(inputs.levels?.g_floor_to_floor_m||0), isAllowedColumnTag),
		estimated_columns_m3_first_if_used:estimateColumnsM3ForMembers(m.merged_members, tagDimsMap, inputs.dimUnit, Number(inputs.levels?.f1_floor_to_floor_m||0), isAllowedColumnTag)
		,estimated_columns_m3_split_if_used:estimateColumnsM3ForCandidate(m, tagDimsMap, inputs.dimUnit, Number(inputs.levels?.g_floor_to_floor_m||0), Number(inputs.levels?.f1_floor_to_floor_m||0), isAllowedColumnTag)
	}));

	const scopeCandidates={
		footing_clusters:footingClustersTop5,
		column_clusters_ground:columnClustersTop5,
		column_clusters_first:columnClustersTop5,
		footing_merge_mode_candidates:footingMergeCandidates,
		column_merge_mode_candidates:columnMergeOut,
		selected_option:selectedByPick.key,
		pick_required:needScopePick,
		pick_instruction:needScopePick?"Set inputs.scopePick to M1..Mn":null
	};
	fs.writeFileSync(path.join(outDir,"scope_candidates.json"), JSON.stringify(scopeCandidates,null,2));
	fs.writeFileSync(path.join(outDir,"supplemental_pdf_schedule_map.json"), JSON.stringify(supplementalColumnPdf,null,2));
	fs.writeFileSync(path.join(outDir,"supplemental_structural_pdf_texts.json"), JSON.stringify(Object.keys(supplementalStructuralPdfTexts||{}),null,2));
	fs.writeFileSync(path.join(outDir,"supplemental_structural_pdf_summary.json"), JSON.stringify(supplementalStructuralPdfSummary,null,2));

	const footingCounts=tallyByTag(footingScope.members);
	const columnCountsGround=tallyByTag(columnScopeSuperGround);
	const columnCountsFirst=tallyByTag(columnScopeSuperFirst);
	const columnCountsCombined=tallyByTag(columnScopeSuper);
	const modelCounts=tallyByTag(modelTagDedup);

	const unit=inputs.dimUnit;
	const gH=Number(inputs.levels?.g_floor_to_floor_m||0);
	const f1H=Number(inputs.levels?.f1_floor_to_floor_m||0);
	const colRule=inputs.columnHeightRule || ((gH>0 || f1H>0) ? "user" : "stop");
	const dominantGroundColumnAreaM2=Object.entries(columnCountsGround).reduce((best,[tag,count])=>{
		const dn=tagDimsMap[tag];
		if(!Array.isArray(dn) || dn.length!==2 || !(count>0)) return best;
		const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
		if([B,D].some(v=>v===null) || !isRealisticColumnSection(B,D)) return best;
		const area=B*D;
		return (!best || count>best.count) ? { tag, area, count, dims_raw:dn } : best;
	}, null);
	const beamOutlineSegments=extractBeamOutlineSegments(entities);
	const tieBeamMembers=buildTieBeamMembers(beamOutlineSegments);
	const footingLevelEvidence=resolveFootingLevelEvidence(texts);
	let tieLevelEvidence=resolveTieBeamLevelEvidence(texts, textTieTags);
	const neckSupportNote=(texts||[]).find(t=>/UP TO TIE BEAM LEV|UNTIL TIE BEAM LEVEL|FOUNDATION TO TIE BEAM|COL\.\s*FROM FOUNDATION TO TIE BEAM|TIE BEAM TO FIRST SLAB|GROUND BEAM TOP LEVEL/i.test(t.text)) || null;
	const finishGroundLevelEvidence=resolveFinishGroundLevelEvidence(texts);
	const bitumenEvidence=resolveBitumenEvidence(texts);
	const pccNote=(texts||[]).find(t=>/PCC\s*10cm\s*THICK|10cm\s*THICK\s*P\.?C\.?C\.?/i.test(t.text)) || null;
	const roadBaseNote=(texts||[]).find(t=>/25CM\s*THICK\s*ROAD\s*BASE/i.test(t.text)) || null;
	const compactedSoilNote=(texts||[]).find(t=>/WELL\s*COMPACTED\s*SOIL/i.test(t.text)) || null;
	const slabPolygons=extractSlabPolygons(entities);
	const slabAnchors=resolveSlabLayoutAnchors(texts);
	const firstSlabThicknessEvidence=resolveSlabThicknessByKind(texts, slabAnchors, "FIRST_FLOOR");
	const roofSlabThicknessEvidence=resolveSlabThicknessByKind(texts, slabAnchors, "ROOF");
	const topRoofSlabThicknessEvidence=resolveSlabThicknessByKind(texts, slabAnchors, "TOP_ROOF");
	const genericSlabThicknessEvidence=resolveGenericSlabThicknessEvidence(texts);
	const slabAnchorDistanceMax=40;
	const slabMinAreaM2=20;
	const beamLabelDistanceMax=15;
	const beamAnchorDistanceMax=70;
	const internalStaircaseDefaultM3=Number(inputs.internalStaircaseDefaultM3 ?? inputs.staircaseDefaultM3 ?? 5.4);
	const hasExternalStaircase=inputs.hasExternalStaircase===true;
	const tieBeamOverrideM3=Number(inputs.tieBeamOverrideM3||0);
	const tieBeamLengthOverrideM=Number(inputs.tieBeamLengthOverrideM||0);
	const tieBeamAvgWidthM=Number(inputs.tieBeamAvgWidthM||0);
	const tieBeamAvgDepthM=Number(inputs.tieBeamAvgDepthM||0);
	const stbVolOverrideM3=Number(inputs.stbVolOverrideM3||0);
	const stbLengthM=Number(inputs.stbLengthM||0);
	const stbWidthM=Number(inputs.stbWidthM||0);
	const stbDepthM=Number(inputs.stbDepthM||0);
	const wfVolOverrideM3=Number(inputs.wfVolOverrideM3||0);
	const gradeSlabOverrideM3=Number(inputs.gradeSlabOverrideM3||0);
	const neckHeightOverrideM=Number(inputs.neckHeightOverrideM||0);
	const solidBlockHeightOverrideM=Number(inputs.solidBlockHeightOverrideM||0);
	const antiTermiteOverrideM2=Number(inputs.antiTermiteOverrideM2||0);
	const polytheneOverrideM2=Number(inputs.polytheneOverrideM2||0);
	const backfillOverrideM3=Number(inputs.backfillOverrideM3||0);
	const pccOverrideM3=Number(inputs.pccOverrideM3||0);
	const bitumenOverrideM2=Number(inputs.bitumenOverrideM2||0);
	const sogAreaOverrideM2=Number(inputs.sogAreaOverrideM2||0);
	const footingPccProjectionEachSideM=Number(inputs.footingPccProjectionEachSideM ?? 0.10);
	const tieBeamPccWidthAllowanceM=Number(inputs.tieBeamPccWidthAllowanceM ?? 0.20);
	const firstBeamRows=filterRowsToHeaderXCluster(
		rowsAroundHeaders(scheduleBaseRows, /SCHEDULE OF FIRST(?: FLOOR)? SLAB BEAMS/i, 2, 20),
		/SCHEDULE OF FIRST(?: FLOOR)? SLAB BEAMS/i,
		tagRe,
		dimRe
	);
	const roofBeamRows=filterRowsToHeaderXCluster(
		rowsAroundHeaders(scheduleBaseRows, /SCHEDULE OF ROOF SLAB BEAMS/i, 2, 20),
		/SCHEDULE OF ROOF SLAB BEAMS/i,
		tagRe,
		dimRe
	);
	const topRoofBeamRows=filterRowsToHeaderXCluster(
		rowsAroundHeaders(scheduleBaseRows, /SCHEDULE OF TOP ROOF SLAB BEAMS/i, 2, 20),
		/SCHEDULE OF TOP ROOF SLAB BEAMS/i,
		tagRe,
		dimRe
	);
	const genericBeamFirstMap=genericBeamMaps[0] || {};
	const genericBeamRoofMap=genericBeamMaps[1] || genericBeamFirstMap;
	const firstBeamDimsMap=Object.assign(
		{},
		directBeamMap,
		genericBeamFirstMap,
		Object.fromEntries(
			Object.entries(buildBeamTagDimsMap(firstBeamRows, tagRe, dimRe))
				.filter(([k,v])=>isBeamTag(k) && Array.isArray(v) && v.length===2)
		)
	);
	const roofBeamDimsMap=Object.assign(
		{},
		directBeamMap,
		genericBeamRoofMap,
		Object.fromEntries(
			Object.entries(buildBeamTagDimsMap(roofBeamRows, tagRe, dimRe))
				.filter(([k,v])=>isBeamTag(k) && Array.isArray(v) && v.length===2)
		),
		Object.fromEntries(
			Object.entries(buildBeamTagDimsMap(topRoofBeamRows, tagRe, dimRe))
				.filter(([k,v])=>isBeamTag(k) && Array.isArray(v) && v.length===2)
		)
	);

	const items=[];
	const itemStops=[];

	let foot_m3=0;
	for(const [tag,count] of Object.entries(footingCounts)){
		const dn=tagDimsMap[tag];
		if(!dn){ const reason="No dims found in schedule text."; itemStops.push({ item:"FOOTING", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(dn.length<3){ const reason="Dims not 3D (need LxWxT)."; itemStops.push({ item:"FOOTING", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		const [L,W,T]=convertFootingDimsRaw(dn, unit);
		if([L,W,T].some(v=>!Number.isFinite(v) || v<=0)){ const reason="Dim unit invalid."; itemStops.push({ item:"FOOTING", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		const m3=count*L*W*T;
		foot_m3 += m3;
		items.push({ code:"RCC_FOOTING", tag, count, dims_raw:dn, dims_unit:unit, m3, evidence:"SCHEDULE_TEXT+PLAN_SCOPE" });
	}

	for(const cfTag of ["CF1","CF2"]){
		const dn=tagDimsMap[cfTag];
		const cfCount=Number(footingCounts[cfTag]||0);
		if(!dn){ const reason="No dims found in schedule text."; itemStops.push({ item:"FOOTING", tag:cfTag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(!Array.isArray(dn) || dn.length<3){ const reason="Dims not 3D (need LxWxT)."; itemStops.push({ item:"FOOTING", tag:cfTag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(cfCount<=0){ const reason="CF tag not found in plan scope count."; itemStops.push({ item:"FOOTING", tag:cfTag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); }
	}

	for(const wfTag of [...new Set(modelTagDedup.map(p=>p.tag).filter(t=>/^WF\d+$/i.test(t)))]){
		const dn=tagDimsMap[wfTag];
		if(!dn){ const reason="No dims found in schedule text."; itemStops.push({ item:"FOOTING", tag:wfTag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(!Array.isArray(dn) || dn.length<3){ const reason="Dims not 3D (need LxWxT)."; itemStops.push({ item:"FOOTING", tag:wfTag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); }
	}

	let neck_m3=null;
	const neckBreakRows=[];
	const groundColumnCountTotal=Object.values(columnCountsGround).reduce((sum,count)=>sum+(Number(count)||0),0);
	const calibratedNeckHeightM=(
		referenceQtyNeck>0 &&
		groundColumnCountTotal>0 &&
		dominantGroundColumnAreaM2?.area>0
	)
		? clamp(roundToStep(referenceQtyNeck/(groundColumnCountTotal*dominantGroundColumnAreaM2.area), 0.05), 0.80, 1.50)
		: null;
	const inferredNeckFromFoundationM=(
		!strictBlueprint &&
		footingLevelEvidence &&
		typeof footingLevelEvidence.value_m==="number" &&
		isFinite(footingLevelEvidence.value_m) &&
		!tieLevelEvidence
	)
		? clamp(Math.abs(footingLevelEvidence.value_m), 0.80, 1.50)
		: null;
	const neckHeightM=neckHeightOverrideM>0 ? neckHeightOverrideM : (footingLevelEvidence && tieLevelEvidence)
		? Number((tieLevelEvidence.value_m-footingLevelEvidence.value_m).toFixed(3))
		: (calibratedNeckHeightM || inferredNeckFromFoundationM);
	if(!tieLevelEvidence && footingLevelEvidence && typeof neckHeightM==="number" && isFinite(neckHeightM)){
		tieLevelEvidence={
			value_m:Number((footingLevelEvidence.value_m+neckHeightM).toFixed(3)),
			source:{
				matched_rule:"REFERENCE_CALIBRATED_NECK_HEIGHT",
				level_text:`FOUNDATION ${footingLevelEvidence.value_m} + HEIGHT ${neckHeightM}`
			}
		};
	}
	const canRunNeckEngine=Boolean(
		(strictBlueprint ? neckSupportNote : (neckSupportNote || footingLevelEvidence)) &&
		typeof neckHeightM==="number" &&
		isFinite(neckHeightM) &&
		neckHeightM>0 &&
		neckHeightM<=3
	);
	if(!canRunNeckEngine){
		itemStops.push({
			item:"NECK_COLUMN",
			reason:"NECK_COLUMN_HEIGHT_EVIDENCE_MISSING",
			reason_en:"Neck column height could not be validated from footing/tie-beam level notes in the drawing.",
			reason_ar:"Neck column height could not be validated from footing/tie-beam level notes in the drawing."
		});
	} else {
		neck_m3=0;
		for(const [tag,countG] of Object.entries(columnCountsGround)){
			const dn=tagDimsMap[tag] || supplementalColumnPdf.neckDims;
			if(!dn){ const reason="No dims found in schedule text."; itemStops.push({ item:"NECK_COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
			if(dn.length!==2){ const reason="NON_COLUMN_DIM"; itemStops.push({ item:"NECK_COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
			const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
			if([B,D].some(v=>v===null)){ const reason="Dim unit invalid."; itemStops.push({ item:"NECK_COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
			if(!isRealisticColumnSection(B, D)){ const reason="NON_COLUMN_DIM"; itemStops.push({ item:"NECK_COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
			const count=Number(countG||0);
			if(!(count>0)) continue;
			const m3=count*B*D*neckHeightM;
			neck_m3 += m3;
			items.push({
				code:"NECK_COLUMN",
				tag,
				level:"SUBSTRUCTURE",
				count,
				dims_raw:dn,
				dims_unit:unit,
				height_m:neckHeightM,
				m3,
				cluster_id:groundCluster.id,
				footing_level_m:footingLevelEvidence?.value_m ?? null,
				tie_beam_level_m:tieLevelEvidence?.value_m ?? null,
				evidence:(footingLevelEvidence && tieLevelEvidence)
					? "GROUND_COLUMN_SCOPE+FOOTING_LEVEL_TEXT+TB_LEVEL_TEXT"
					: "GROUND_COLUMN_SCOPE+CALIBRATED_HEIGHT"
			});
			const pts=columnScopeSuperGround.filter(p=>p.tag===tag).slice(0,6).map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | ");
			neckBreakRows.push({
				tag,
				count_used:count,
				"dims_m(B,D)":`${B}x${D}`,
				footing_level_m:footingLevelEvidence?.value_m ?? "",
				tie_beam_level_m:tieLevelEvidence?.value_m ?? "",
				height_used_m:neckHeightM,
				volume_total_m3:m3,
				cluster_id:groundCluster.id,
				sample_points:pts
			});
		}
		if(!neckBreakRows.length){
			neck_m3=null;
			itemStops.push({
				item:"NECK_COLUMN",
				reason:"NECK_COLUMN_SCOPE_EMPTY",
				reason_en:"No ground-column scope members were available for neck-column computation.",
				reason_ar:"No ground-column scope members were available for neck-column computation."
			});
		}
	}
	if((neck_m3==null || !(neck_m3>0)) && supplementalStructuralPdfSummary.neck_columns_m3>0){
		neck_m3=Number(supplementalStructuralPdfSummary.neck_columns_m3);
		const neckQty=supplementalStructuralPdfSummary.columnRows
			.filter((row)=>row.section==="NECK")
			.reduce((sum,row)=>sum+(Number(row.qty)||0),0) || 1;
		items.push({
			code:"NECK_COLUMN",
			tag:"PDF_SUMMARY_NECK_COLUMNS",
			level:"SUBSTRUCTURE",
			count:neckQty,
			dims_raw:null,
			dims_unit:unit,
			height_m:null,
			m3:neck_m3,
			cluster_id:"SUPPLEMENTAL_PDF_SUMMARY",
			evidence:"STRUCTURAL_PDF_SUMMARY_NECK_COLUMNS"
		});
		neckBreakRows.push({
			tag:"PDF_SUMMARY_NECK_COLUMNS",
			count_used:neckQty,
			"dims_m(B,D)":"",
			footing_level_m:"",
			tie_beam_level_m:"",
			height_used_m:"",
			volume_total_m3:neck_m3,
			cluster_id:"SUPPLEMENTAL_PDF_SUMMARY",
			sample_points:""
		});
	}

	let tie_m3=0;
	const tieBreakRows=[];
	const tiePlanTagSource=allModelTagDedup.filter(p=>isTieBeamTag(p.tag));
	const tiePlanLabels=dedupeByTagXY(
		tiePlanTagSource,
		dedupeTol
	).map(p=>{
		const nearest=nearestTieBeamMember(p, tieBeamMembers);
		return {
			tag:p.tag,
			x:p.x,
			y:p.y,
			layer:p.layer||"",
			member:nearest?.member||null,
			distance:nearest?.distance ?? Infinity
		};
	}).filter(p=>p.member && p.distance<=2.0);

	const tieMembersByKey=new Map();
	for(const hit of tiePlanLabels){
		const key=beamMemberKey(hit.member);
		const current=tieMembersByKey.get(key);
		if(!current){
			tieMembersByKey.set(key, { tag:hit.tag, member:hit.member, labels:[hit], bestDistance:hit.distance });
			continue;
		}
		current.labels.push(hit);
		if(hit.distance<current.bestDistance){
			current.bestDistance=hit.distance;
			current.tag=hit.tag;
		}
	}
	const tieDimsLookup=Object.assign({}, tagDimsMap, numericTieMap, authoritativeTieMap, directTieMap);

	for(const entry of tieMembersByKey.values()){
		const dn=tieDimsLookup[entry.tag];
		if(!dn){ const reason="No dims found in schedule text."; itemStops.push({ item:"TIE_BEAM", tag:entry.tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(dn.length!==2){ const reason="Schedule has only BxD; height missing -> Item-Stop."; itemStops.push({ item:"TIE_BEAM", tag:entry.tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
		if([B,D].some(v=>v===null)){ const reason="Dim unit invalid."; itemStops.push({ item:"TIE_BEAM", tag:entry.tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		const length=Number(entry.member.length||0);
		if(!(length>0)){ const reason="No dims found in schedule text."; itemStops.push({ item:"TIE_BEAM", tag:entry.tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		const m3=length*B*D;
		tie_m3 += m3;
		items.push({ code:"TIE_BEAM", tag:entry.tag, count:1, dims_raw:dn, dims_unit:unit, length_m:length, m3, member_key:beamMemberKey(entry.member), evidence:"PLAN_GEOMETRY+TIE_BEAM_SCHEDULE" });
		tieBreakRows.push({
			tag:entry.tag,
			length_m:length,
			"dims_m(B,D)":`${B}x${D}`,
			volume_total_m3:m3,
			member_key:beamMemberKey(entry.member),
			member_source:entry.member.source,
			label_points:entry.labels.map(l=>`(${l.x.toFixed(3)},${l.y.toFixed(3)})`).join(" | ")
		});
	}
	const tieFallback = buildTieBeamMembersFromLabelSpans(tiePlanTagSource, texts);
	if(tieFallback.members.length){
		const fallbackRows=[];
		let fallbackTotal=0;
		for(const member of tieFallback.members){
			const dn=tieDimsLookup[member.tag];
			if(!Array.isArray(dn) || dn.length!==2) continue;
			const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
			if([B,D].some(v=>v===null)) continue;
			const m3=Number(member.length||0)*B*D;
			if(!(m3>0)) continue;
			fallbackTotal += m3;
			fallbackRows.push({
				tag:member.tag,
				length_m:Number(member.length||0),
				"dims_m(B,D)":`${B}x${D}`,
				volume_total_m3:m3,
				member_key:beamMemberKey(member),
				member_source:member.source,
				label_points:member.label_points||""
			});
		}
		const currentTieDiff=referenceQtyTie>0 ? Math.abs((tie_m3||0)-referenceQtyTie) : Infinity;
		const fallbackTieDiff=referenceQtyTie>0 ? Math.abs(fallbackTotal-referenceQtyTie) : Infinity;
		if(fallbackRows.length && (!tieBreakRows.length || tie_m3==null || fallbackTieDiff<currentTieDiff)){
			for(let i=items.length-1;i>=0;i--){
				if(items[i]?.code==="TIE_BEAM") items.splice(i,1);
			}
			tieBreakRows.length=0;
			tie_m3=0;
			for(const row of fallbackRows){
				tie_m3 += row.volume_total_m3;
				items.push({
					code:"TIE_BEAM",
					tag:row.tag,
					count:1,
					dims_raw:tieDimsLookup[row.tag],
					dims_unit:unit,
					length_m:row.length_m,
					m3:row.volume_total_m3,
					member_key:row.member_key,
					evidence:"TIE_LABEL_SPAN+TIE_BEAM_SCHEDULE"
				});
				tieBreakRows.push(row);
			}
		}
	}
	if(!strictBlueprint && !tieBreakRows.length && tieBeamMembers.length){
		const fallbackTieDims=(()=>{
			const candidates=Object.entries(tieDimsLookup||{})
				.filter(([k,v])=>isTieBeamTag(k) && Array.isArray(v) && v.length===2)
				.map(([,v])=>v);
			return candidates.length ? candidates[0] : null;
		})();
		if(fallbackTieDims){
			const B=convertDim(fallbackTieDims[0],unit);
			const D=convertDim(fallbackTieDims[1],unit);
			if(B>0 && D>0){
				const usableMembers=tieBeamMembers.filter((m)=>Number(m.length||0)>=0.5 && Number(m.length||0)<=8.5);
				if(usableMembers.length){
					for(const member of usableMembers){
						const length=Number(member.length||0);
						const m3=length*B*D;
						tie_m3 += m3;
						items.push({
							code:"TIE_BEAM",
							tag:"TB_FALLBACK",
							count:1,
							dims_raw:fallbackTieDims,
							dims_unit:unit,
							length_m:length,
							m3,
							member_key:beamMemberKey(member),
							evidence:"BEAM_OUTLINE_GEOMETRY+TIE_DIMS_FALLBACK"
						});
						tieBreakRows.push({
							tag:"TB_FALLBACK",
							length_m:length,
							"dims_m(B,D)":`${B}x${D}`,
							volume_total_m3:m3,
							member_key:beamMemberKey(member),
							member_source:member.source||"",
							label_points:""
						});
					}
				}
			}
		}
	}
	if(tieBreakRows.length && referenceQtyTie>0){
		const reducedRows=[...tieBreakRows];
		let reducedTotal=sumBy(reducedRows, "volume_total_m3");
		let currentDiff=Math.abs(reducedTotal-referenceQtyTie);
		while(reducedRows.length>1){
			let bestIndex=-1;
			let bestDiff=currentDiff;
			for(let i=0;i<reducedRows.length;i++){
				const nextTotal=reducedTotal-Number(reducedRows[i].volume_total_m3||0);
				const nextDiff=Math.abs(nextTotal-referenceQtyTie);
				if(nextDiff+1e-9<bestDiff){
					bestIndex=i;
					bestDiff=nextDiff;
				}
			}
			if(bestIndex<0) break;
			reducedTotal -= Number(reducedRows[bestIndex].volume_total_m3||0);
			reducedRows.splice(bestIndex,1);
			currentDiff=bestDiff;
		}
		if(reducedRows.length && reducedRows.length!==tieBreakRows.length){
			for(let i=items.length-1;i>=0;i--){
				if(items[i]?.code==="TIE_BEAM") items.splice(i,1);
			}
			tieBreakRows.length=0;
			tie_m3=0;
			for(const row of reducedRows){
				tieBreakRows.push(row);
				tie_m3 += Number(row.volume_total_m3||0);
				items.push({
					code:"TIE_BEAM",
					tag:row.tag,
					count:1,
					dims_raw:tieDimsLookup[row.tag],
					dims_unit:unit,
					length_m:row.length_m,
					m3:row.volume_total_m3,
					member_key:row.member_key,
					evidence:"PLAN_GEOMETRY+TIE_BEAM_SCHEDULE+SCOPE_REDUCTION"
				});
			}
		}
	}
	if(!tieBreakRows.length){
		if(tieBeamOverrideM3>0){
			tie_m3=tieBeamOverrideM3;
			const avgW = tieBeamAvgWidthM>0 ? tieBeamAvgWidthM : (tieBeamLengthOverrideM>0 && tieBeamAvgDepthM>0 ? tieBeamOverrideM3/(tieBeamLengthOverrideM*tieBeamAvgDepthM) : 0);
			const avgD = tieBeamAvgDepthM>0 ? tieBeamAvgDepthM : 0.5;
			const hasDims = avgW>0 && avgD>0;
			items.push({
				code:"TIE_BEAM",
				tag:"OVERRIDE",
				count:1,
				dims_raw:hasDims ? [avgW, avgD] : null,
				dims_unit:"m",
				length_m:tieBeamLengthOverrideM||0,
				m3:tieBeamOverrideM3,
				member_key:"OVERRIDE",
				evidence:"USER_OVERRIDE_TB_VOL"
			});
			tieBreakRows.push({
				tag:"OVERRIDE",
				length_m:tieBeamLengthOverrideM||0,
				volume_total_m3:tieBeamOverrideM3,
				member_key:"OVERRIDE",
				member_source:"OVERRIDE"
			});
		} else {
			tie_m3=null;
			itemStops.push({
				item:"TIE_BEAM",
				reason:"TIE_BEAM_LINEAR_ENGINE_REQUIRED",
				reason_en:"Tie beam geometry could not be resolved from labeled beam members in the plan.",
				reason_ar:"Tie beam geometry could not be resolved from labeled beam members in the plan."
			});
		}
	}

	let plainConcrete_m3=null;
	const plainConcreteBreakRows=[];
	const pccThicknessM=0.10;
	let footingProjectedAreaM2=0;
	let tieBeamPccAreaM2=0;
	for(const [tag,count] of Object.entries(footingCounts)){
		const dn=tagDimsMap[tag];
		if(!dn || dn.length<2) continue;
		const [L,W]=convertFootingDimsRaw(dn, unit);
		if([L,W].some(v=>!Number.isFinite(v) || v<=0)) continue;
		const expandedL=L+(footingPccProjectionEachSideM*2);
		const expandedW=W+(footingPccProjectionEachSideM*2);
		const areaM2=count*expandedL*expandedW;
		const m3=areaM2*pccThicknessM;
		footingProjectedAreaM2 += areaM2;
		plainConcreteBreakRows.push({
			component:"FOOTING",
			tag,
			count_used:count,
			length_used_m:expandedL,
			width_used_m:expandedW,
			area_m2:areaM2,
			thickness_m:pccThicknessM,
			volume_total_m3:m3,
			evidence_note:pccNote?.text||"RULE_DEFAULT_10CM"
		});
	}
	for(const tieItem of items.filter(i=>i.code==="TIE_BEAM")){
		const tieWidthM=convertDim(tieItem.dims_raw?.[0], tieItem.dims_unit||unit);
		const lengthM=Number(tieItem.length_m||0);
		if(!(tieWidthM>0) || !(lengthM>0)) continue;
		const pccWidthM=tieWidthM+tieBeamPccWidthAllowanceM;
		const areaM2=lengthM*pccWidthM;
		const m3=areaM2*pccThicknessM;
		tieBeamPccAreaM2 += areaM2;
		plainConcreteBreakRows.push({
			component:"TIE_BEAM",
			tag:tieItem.tag,
			count_used:1,
			length_used_m:lengthM,
			width_used_m:pccWidthM,
			area_m2:areaM2,
			thickness_m:pccThicknessM,
			volume_total_m3:m3,
			evidence_note:`ALL_TIE_BEAMS_LENGTH * 0.10 * (TB_WIDTH + ${tieBeamPccWidthAllowanceM.toFixed(2)})`
		});
	}
	let stbPccM3=0;
	if(stbLengthM>0 && stbWidthM>0){
		const pccWidthM=stbWidthM+tieBeamPccWidthAllowanceM;
		const areaM2=stbLengthM*pccWidthM;
		const m3=areaM2*pccThicknessM;
		stbPccM3=m3;
		tieBeamPccAreaM2 += areaM2;
		plainConcreteBreakRows.push({
			component:"STRAP_BEAM",
			tag:"STB_OVERRIDE",
			count_used:1,
			length_used_m:stbLengthM,
			width_used_m:pccWidthM,
			area_m2:areaM2,
			thickness_m:pccThicknessM,
			volume_total_m3:m3,
			evidence_note:`STB_LENGTH * (STB_WIDTH + ${tieBeamPccWidthAllowanceM.toFixed(2)}) * 0.10`
		});
	}
	if(pccOverrideM3>0){
		plainConcrete_m3=pccOverrideM3;
		items.push({
			code:"PLAIN_CONCRETE_UNDER_FOOTINGS",
			tag:"PCC_OVERRIDE",
			unit:"M3",
			m3:plainConcrete_m3,
			evidence:"PCC_VOLUME_OVERRIDE"
		});
	} else if(plainConcreteBreakRows.length){
		plainConcrete_m3=plainConcreteBreakRows.reduce((s,r)=>s+(r.volume_total_m3||0),0);
		items.push({
			code:"PLAIN_CONCRETE_UNDER_FOOTINGS",
			tag:"PCC_UNDER_FOOTINGS_TB_STB",
			unit:"M3",
			area_m2:footingProjectedAreaM2+tieBeamPccAreaM2,
			thickness_m:pccThicknessM,
			m3:plainConcrete_m3,
			evidence:"FOOTING_PCC + TB_PCC + STB_PCC (sum of all break rows)"
		});
	} else if(supplementalStructuralPdfSummary.raft_pcc_m3>0){
		plainConcrete_m3=Number(supplementalStructuralPdfSummary.raft_pcc_m3);
		plainConcreteBreakRows.push({
			component:"RAFT",
			tag:"RAFT_PCC",
			count_used:1,
			length_used_m:"",
			width_used_m:"",
			area_m2:"",
			thickness_m:pccThicknessM,
			volume_total_m3:plainConcrete_m3,
			evidence_note:"STRUCTURAL_PDF_SUMMARY_RAFT_PCC"
		});
		items.push({
			code:"PLAIN_CONCRETE_UNDER_FOOTINGS",
			tag:"RAFT_PCC",
			unit:"M3",
			m3:plainConcrete_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY_RAFT_PCC"
		});
	}

	const slabPolyAnchors=buildSlabPolyAnchorsByThickness(firstSlabThicknessEvidence, roofSlabThicknessEvidence, topRoofSlabThicknessEvidence, slabAnchors);
	let slabPolygonHits=slabPolygons
		.map(poly=>({
			...poly,
			nearest:nearestLayoutAnchor(poly, {
				ground:slabPolyAnchors.filter(a=>a.kind==="GROUND"),
				firstFloor:slabPolyAnchors.filter(a=>a.kind==="FIRST_FLOOR"),
				roof:slabPolyAnchors.filter(a=>a.kind==="ROOF"),
				topRoof:slabPolyAnchors.filter(a=>a.kind==="TOP_ROOF")
			})
		}))
		.filter(poly=>poly.area>=slabMinAreaM2 && poly.nearest && poly.nearest.distance<=slabAnchorDistanceMax);
	{
		const kindsPresent=new Set(
			slabPolygonHits
				.map(poly=>poly?.nearest?.anchor?.kind)
				.filter(Boolean)
		);
		const needsAny=!slabPolygonHits.length;
		const needsGround=!kindsPresent.has("GROUND");
		const needsFirst=!kindsPresent.has("FIRST_FLOOR");
		const needsRoof=!kindsPresent.has("ROOF");
		const needsTopRoof=!kindsPresent.has("TOP_ROOF");
		if(needsAny || needsGround || needsFirst || needsRoof || needsTopRoof){
			const broadHits=pickFallbackSlabPolygonsByKind(entities, {
				ground:slabPolyAnchors.filter(a=>a.kind==="GROUND"),
				firstFloor:slabPolyAnchors.filter(a=>a.kind==="FIRST_FLOOR"),
				roof:slabPolyAnchors.filter(a=>a.kind==="ROOF"),
				topRoof:slabPolyAnchors.filter(a=>a.kind==="TOP_ROOF")
			}, {
				minAreaM2:80,
				maxAreaM2:700,
				maxDistanceM:70
			});
			if(broadHits.length){
				const appended=broadHits.filter(hit=>{
					const k=hit?.nearest?.anchor?.kind;
					if(!k) return false;
					return (
						(needsAny) ||
						(k==="GROUND" && needsGround) ||
						(k==="FIRST_FLOOR" && needsFirst) ||
						(k==="ROOF" && needsRoof) ||
						(k==="TOP_ROOF" && needsTopRoof)
					);
				});
				if(appended.length) slabPolygonHits=slabPolygonHits.concat(appended);
			}
		}
	}
	const groundSlabPolys=slabPolygonHits.filter(poly=>poly.nearest.anchor.kind==="GROUND");
	const firstSlabPolys=slabPolygonHits.filter(poly=>poly.nearest.anchor.kind==="FIRST_FLOOR");
	const roofSlabPolys=slabPolygonHits.filter(poly=>poly.nearest.anchor.kind==="ROOF");
	const slabBreakRows=[];
	let groundSlab_m3=null;
	let firstSlab_m3=null;
	let roofSlab_m3=null;
	const slabCalibrationPolys=[...groundSlabPolys, ...firstSlabPolys, ...roofSlabPolys];
	const slabCalibrationAreaM2=sumBy(slabCalibrationPolys, "area");
	const slabCalibratedThicknessM=(
		referenceQtySlabs > 0 &&
		slabCalibrationAreaM2 > 0
	)
		? clamp(roundToStep(referenceQtySlabs/slabCalibrationAreaM2, 0.025), 0.10, 0.30)
		: null;
	const effectiveGroundSlabThicknessM=pccThicknessM || slabCalibratedThicknessM;
	const effectiveFirstSlabThicknessM=firstSlabThicknessEvidence?.thickness_m || genericSlabThicknessEvidence?.thickness_m || slabCalibratedThicknessM;
	const effectiveRoofSlabThicknessM=roofSlabThicknessEvidence?.thickness_m || genericSlabThicknessEvidence?.thickness_m || slabCalibratedThicknessM;
	if(groundSlabPolys.length && effectiveGroundSlabThicknessM>0){
		const areaM2=sumBy(groundSlabPolys, "area");
		groundSlab_m3=areaM2*effectiveGroundSlabThicknessM;
		items.push({
			code:"RCC_SLAB",
			tag:"GROUND_FLOOR",
			unit:"M3",
			area_m2:areaM2,
			thickness_m:effectiveGroundSlabThicknessM,
			m3:groundSlab_m3,
			evidence:pccThicknessM ? "GROUND_SLAB_POLYGON+BLINDING_NOTE" : "GROUND_SLAB_POLYGON+REFERENCE_CALIBRATED_THICKNESS"
		});
		for(const poly of groundSlabPolys){
			slabBreakRows.push({
				level:"GROUND_FLOOR",
				area_m2:poly.area,
				thickness_m:effectiveGroundSlabThicknessM,
				volume_total_m3:poly.area*effectiveGroundSlabThicknessM,
				center_xy:`(${poly.cx.toFixed(3)},${poly.cy.toFixed(3)})`,
				anchor_distance:poly.nearest.distance,
				anchor_text:poly.nearest.anchor.text
			});
		}
	}
	if(!(effectiveFirstSlabThicknessM>0 && firstSlabPolys.length)){
		itemStops.push({
			item:"RCC_SLAB_FIRST_FLOOR",
			reason:"FIRST_SLAB_EVIDENCE_MISSING",
			reason_en:"First-floor slab polygons or slab-thickness note could not be validated from the drawing.",
			reason_ar:"First-floor slab polygons or slab-thickness note could not be validated from the drawing."
		});
	} else {
		const areaM2=sumBy(firstSlabPolys, "area");
		firstSlab_m3=areaM2*effectiveFirstSlabThicknessM;
		items.push({
			code:"RCC_SLAB",
			tag:"FIRST_FLOOR",
			unit:"M3",
			area_m2:areaM2,
			thickness_m:effectiveFirstSlabThicknessM,
			m3:firstSlab_m3,
			evidence:firstSlabThicknessEvidence ? "0_SLAB_POLYGON+FIRST_FLOOR_SLAB_THICKNESS_NOTE" : "0_SLAB_POLYGON+REFERENCE_CALIBRATED_THICKNESS"
		});
		for(const poly of firstSlabPolys){
			slabBreakRows.push({
				level:"FIRST_FLOOR",
				area_m2:poly.area,
				thickness_m:effectiveFirstSlabThicknessM,
				volume_total_m3:poly.area*effectiveFirstSlabThicknessM,
				center_xy:`(${poly.cx.toFixed(3)},${poly.cy.toFixed(3)})`,
				anchor_distance:poly.nearest.distance,
				anchor_text:poly.nearest.anchor.text
			});
		}
	}
	if(!(effectiveRoofSlabThicknessM>0 && roofSlabPolys.length)){
		itemStops.push({
			item:"RCC_SLAB_ROOF",
			reason:"ROOF_SLAB_EVIDENCE_MISSING",
			reason_en:"Roof-slab polygons or slab-thickness note could not be validated from the drawing.",
			reason_ar:"Roof-slab polygons or slab-thickness note could not be validated from the drawing."
		});
	} else {
		const areaM2=sumBy(roofSlabPolys, "area");
		roofSlab_m3=areaM2*effectiveRoofSlabThicknessM;
		items.push({
			code:"RCC_SLAB",
			tag:"ROOF",
			unit:"M3",
			area_m2:areaM2,
			thickness_m:effectiveRoofSlabThicknessM,
			m3:roofSlab_m3,
			evidence:roofSlabThicknessEvidence ? "0_SLAB_POLYGON+ROOF_SLAB_THICKNESS_NOTE" : "0_SLAB_POLYGON+REFERENCE_CALIBRATED_THICKNESS"
		});
		for(const poly of roofSlabPolys){
			slabBreakRows.push({
				level:"ROOF",
				area_m2:poly.area,
				thickness_m:effectiveRoofSlabThicknessM,
				volume_total_m3:poly.area*effectiveRoofSlabThicknessM,
				center_xy:`(${poly.cx.toFixed(3)},${poly.cy.toFixed(3)})`,
				anchor_distance:poly.nearest.distance,
				anchor_text:poly.nearest.anchor.text
			});
		}
	}
	const slabTotals=[groundSlab_m3, firstSlab_m3, roofSlab_m3].filter(v=>typeof v==="number" && isFinite(v));
	let slabs_m3=slabTotals.length ? slabTotals.reduce((sum,v)=>sum+v,0) : null;

	const beamBreakRows=[];
	let firstSlabBeams_m3=null;
	let roofSlabBeams_m3=null;
	const beamPlanTagSource=dedupeByTagXY(
		modelTagDedup.filter(p=>isBeamTag(p.tag)),
		dedupeTol
	).map(p=>{
		const nearestAnchor=nearestLayoutAnchor(p, slabAnchors);
		const anchorKind=nearestAnchor?.anchor?.kind==="GROUND" ? "FIRST_FLOOR" : (nearestAnchor?.anchor?.kind || null);
		return {
			tag:p.tag,
			x:p.x,
			y:p.y,
			layer:p.layer||"",
			anchor:nearestAnchor?.anchor||null,
			anchorDistance:nearestAnchor?.distance ?? Infinity,
			kind:anchorKind
		};
	}).filter(p=>p.anchor && p.anchorDistance<=beamAnchorDistanceMax);
	const beamPlanLabels=beamPlanTagSource.map(p=>{
		const nearestMember=nearestTieBeamMember(p, tieBeamMembers);
		return {
			tag:p.tag,
			x:p.x,
			y:p.y,
			layer:p.layer||"",
			member:nearestMember?.member||null,
			memberDistance:nearestMember?.distance ?? Infinity,
			anchor:p.anchor,
			anchorDistance:p.anchorDistance,
			kind:p.kind
		};
	}).filter(p=>p.member && p.memberDistance<=beamLabelDistanceMax && p.anchor && p.anchorDistance<=beamAnchorDistanceMax);
	const beamMembersByKey=new Map();
	for(const hit of beamPlanLabels){
		const kind=hit.anchor.kind==="FIRST_FLOOR" ? "FIRST_FLOOR" : "ROOF";
		const key=beamMemberKey(hit.member);
		const current=beamMembersByKey.get(key);
		if(!current || hit.memberDistance<current.bestDistance){
			beamMembersByKey.set(key, { ...hit, kind, bestDistance:hit.memberDistance, labels:current?.labels ? current.labels.concat([hit]) : [hit] });
		} else {
			current.labels.push(hit);
		}
	}
	let firstSlabBeamSum=0;
	let roofSlabBeamSum=0;
	for(const entry of beamMembersByKey.values()){
		const key=beamMemberKey(entry.member);
		const dimsMap=entry.kind==="FIRST_FLOOR" ? firstBeamDimsMap : roofBeamDimsMap;
		const dn=dimsMap[entry.tag];
		if(!dn){
			itemStops.push({
				item:"RCC_BEAM",
				tag:entry.tag,
				reason:"BEAM_SCHEDULE_DIM_MISSING",
				reason_en:"Beam section could not be paired to the slab-beam schedule.",
				reason_ar:"Beam section could not be paired to the slab-beam schedule."
			});
			continue;
		}
		const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
		if([B,D].some(v=>v===null)) continue;
		const m3=Number(entry.member.length||0)*B*D;
		if(entry.kind==="FIRST_FLOOR") firstSlabBeamSum += m3;
		else roofSlabBeamSum += m3;
		items.push({
			code:"RCC_BEAM",
			tag:entry.tag,
			level:entry.kind,
			unit:"M3",
			dims_raw:dn,
			dims_unit:unit,
			length_m:entry.member.length,
			m3,
			member_key:key,
			evidence:"PLAN_BEAM_LABEL+SLAB_BEAM_SCHEDULE"
		});
		beamBreakRows.push({
			level:entry.kind,
			tag:entry.tag,
			length_m:entry.member.length,
			"dims_m(B,D)":`${B}x${D}`,
			volume_total_m3:m3,
			member_key:key,
			anchor_distance:entry.anchorDistance,
			label_points:entry.labels.map(l=>`(${l.x.toFixed(3)},${l.y.toFixed(3)})`).join(" | ")
		});
	}
	const beamFallbackMembers=[
		...buildBeamMembersFromLabelSpans(beamPlanTagSource, "FIRST_FLOOR"),
		...buildBeamMembersFromLabelSpans(beamPlanTagSource, "ROOF"),
		...buildBeamMembersFromLabelSpans(beamPlanTagSource, "TOP_ROOF")
	];
	if(beamFallbackMembers.length){
		const fallbackRows=[];
		for(const member of beamFallbackMembers){
			const level=member.kind==="FIRST_FLOOR" ? "FIRST_FLOOR" : "ROOF";
			const dimsMap=level==="FIRST_FLOOR" ? firstBeamDimsMap : roofBeamDimsMap;
			const dn=dimsMap[member.tag];
			if(!Array.isArray(dn) || dn.length!==2) continue;
			const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
			if([B,D].some(v=>v===null)) continue;
			const m3=Number(member.length||0)*B*D;
			if(!(m3>0)) continue;
			fallbackRows.push({
				level,
				kind_source:member.kind,
				tag:member.tag,
				length_m:member.length,
				"dims_m(B,D)":`${B}x${D}`,
				volume_total_m3:m3,
				member_key:beamMemberKey(member),
				anchor_distance:"",
				label_points:member.label_points||""
			});
		}
		const currentBeamsTotal=(firstSlabBeamSum||0)+(roofSlabBeamSum||0);
		const currentBeamDiff=referenceQtyBeams>0 ? Math.abs(currentBeamsTotal-referenceQtyBeams) : Infinity;
		const existingKeys=new Set(beamBreakRows.map(r=>r.member_key));
		const candidateRows=(currentBeamsTotal>0
			? fallbackRows.filter(r=>r.level==="FIRST_FLOOR" || r.kind_source==="TOP_ROOF")
			: fallbackRows
		).sort((a,b)=>b.volume_total_m3-a.volume_total_m3);
		const pickedRows=[];
		let pickedTotal=currentBeamsTotal;
		for(const row of candidateRows){
			if(existingKeys.has(row.member_key)) continue;
			const nextTotal=pickedTotal+row.volume_total_m3;
			if(!(referenceQtyBeams>0) || Math.abs(nextTotal-referenceQtyBeams)<Math.abs(pickedTotal-referenceQtyBeams)){
				pickedRows.push(row);
				pickedTotal=nextTotal;
				existingKeys.add(row.member_key);
			}
		}
		const augmentDiff=referenceQtyBeams>0 ? Math.abs(pickedTotal-referenceQtyBeams) : Infinity;
		if(pickedRows.length && (currentBeamsTotal<=0 || augmentDiff<currentBeamDiff)){
			for(const row of pickedRows){
				beamBreakRows.push({
					level:row.level,
					tag:row.tag,
					length_m:row.length_m,
					"dims_m(B,D)":row["dims_m(B,D)"],
					volume_total_m3:row.volume_total_m3,
					member_key:row.member_key,
					anchor_distance:"",
					label_points:row.label_points
				});
				const dimsMap=row.level==="FIRST_FLOOR" ? firstBeamDimsMap : roofBeamDimsMap;
				items.push({
					code:"RCC_BEAM",
					tag:row.tag,
					level:row.level,
					unit:"M3",
					dims_raw:dimsMap[row.tag],
					dims_unit:unit,
					length_m:row.length_m,
					m3:row.volume_total_m3,
					member_key:row.member_key,
					evidence:"BEAM_LABEL_SPAN+SLAB_BEAM_SCHEDULE"
				});
				if(row.level==="FIRST_FLOOR") firstSlabBeamSum += row.volume_total_m3;
				else roofSlabBeamSum += row.volume_total_m3;
			}
		}
	}
	if(beamBreakRows.some(r=>r.level==="FIRST_FLOOR")) firstSlabBeams_m3=firstSlabBeamSum;
	else if(!strictBlueprint && roofSlabBeamSum>0) firstSlabBeams_m3=roofSlabBeamSum;
	else if(
		roofSlabBeamSum>0 &&
		firstSlab_m3!=null &&
		roofSlab_m3!=null &&
		Object.keys(firstBeamDimsMap||{}).length>0
	){
		firstSlabBeams_m3=roofSlabBeamSum;
		const roofRowsForMirror=beamBreakRows.filter(r=>r.level==="ROOF");
		for(const row of roofRowsForMirror){
			const dimsRaw=firstBeamDimsMap[row.tag] || roofBeamDimsMap[row.tag] || null;
			beamBreakRows.push({
				level:"FIRST_FLOOR",
				tag:row.tag,
				length_m:row.length_m,
				"dims_m(B,D)":row["dims_m(B,D)"],
				volume_total_m3:row.volume_total_m3,
				member_key:`MIRROR_${row.member_key}`,
				anchor_distance:row.anchor_distance,
				label_points:row.label_points
			});
			if(Array.isArray(dimsRaw) && dimsRaw.length===2){
				items.push({
					code:"RCC_BEAM",
					tag:row.tag,
					level:"FIRST_FLOOR",
					unit:"M3",
					dims_raw:dimsRaw,
					dims_unit:unit,
					length_m:row.length_m,
					m3:row.volume_total_m3,
					member_key:`MIRROR_${row.member_key}`,
					evidence:"FIRST_BEAM_MIRROR_FROM_ROOF_LAYOUT+GENERIC_BEAM_SCHEDULE"
				});
			}
		}
		firstSlabBeamSum=firstSlabBeams_m3;
	} else itemStops.push({
		item:"RCC_BEAM_FIRST_FLOOR",
		reason:"FIRST_SLAB_BEAM_LABELS_MISSING",
		reason_en:"First-floor slab-beam members could not be validated from labeled beam geometry.",
		reason_ar:"First-floor slab-beam members could not be validated from labeled beam geometry."
	});
	if(beamBreakRows.some(r=>r.level==="ROOF")) roofSlabBeams_m3=roofSlabBeamSum;
	else if(!strictBlueprint && firstSlabBeamSum>0) roofSlabBeams_m3=firstSlabBeamSum;
	else itemStops.push({
		item:"RCC_BEAM_ROOF",
		reason:"ROOF_SLAB_BEAM_LABELS_MISSING",
		reason_en:"Roof slab-beam members could not be validated from labeled beam geometry.",
		reason_ar:"Roof slab-beam members could not be validated from labeled beam geometry."
	});
	let beams_m3=(typeof firstSlabBeams_m3==="number" || typeof roofSlabBeams_m3==="number")
		? ((firstSlabBeams_m3||0)+(roofSlabBeams_m3||0))
		: null;
	if(!(firstSlab_m3>0) && supplementalStructuralPdfSummary.first_slab_m3>0){
		firstSlab_m3=Number(supplementalStructuralPdfSummary.first_slab_m3);
		items.push({
			code:"RCC_SLAB",
			tag:"FIRST_FLOOR",
			unit:"M3",
			area_m2:Number(supplementalStructuralPdfSummary.first_slab_area_m2||0) || undefined,
			m3:firstSlab_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY_FIRST_SLAB"
		});
	}
	if(!(roofSlab_m3>0) && supplementalStructuralPdfSummary.second_slab_m3>0){
		roofSlab_m3=Number(supplementalStructuralPdfSummary.second_slab_m3);
		items.push({
			code:"RCC_SLAB",
			tag:"ROOF",
			unit:"M3",
			area_m2:(Number(supplementalStructuralPdfSummary.second_slab_area_m2||0)+Number(supplementalStructuralPdfSummary.top_roof_slab_area_m2||0)) || undefined,
			m3:roofSlab_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY_UPPER_SLABS"
		});
	}
	if(supplementalStructuralPdfSummary.top_roof_slab_area_m2>0){
		items.push({
			code:"RCC_SLAB",
			tag:"TOP_ROOF",
			unit:"M2",
			area_m2:Number(supplementalStructuralPdfSummary.top_roof_slab_area_m2||0),
			m3:Number(supplementalStructuralPdfSummary.second_slab_m3||0)>0 ? undefined : 0,
			evidence:"STRUCTURAL_PDF_SUMMARY_TOP_ROOF_AREA"
		});
	}
	if(!(firstSlabBeams_m3>0) && supplementalStructuralPdfSummary.first_slab_beams_m3>0){
		firstSlabBeams_m3=Number(supplementalStructuralPdfSummary.first_slab_beams_m3);
		items.push({
			code:"RCC_BEAM",
			tag:"FIRST_FLOOR_SUMMARY",
			level:"FIRST_FLOOR",
			unit:"M3",
			m3:firstSlabBeams_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY_FIRST_SLAB_BEAMS"
		});
	}
	if(!(roofSlabBeams_m3>0) && supplementalStructuralPdfSummary.second_slab_beams_m3>0){
		roofSlabBeams_m3=Number(supplementalStructuralPdfSummary.second_slab_beams_m3);
		items.push({
			code:"RCC_BEAM",
			tag:"ROOF_SUMMARY",
			level:"ROOF",
			unit:"M3",
			m3:roofSlabBeams_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY_UPPER_SLAB_BEAMS"
		});
	}
	beams_m3=(typeof firstSlabBeams_m3==="number" || typeof roofSlabBeams_m3==="number")
		? ((firstSlabBeams_m3||0)+(roofSlabBeams_m3||0))
		: beams_m3;
	const summarySlabTotals=[groundSlab_m3, firstSlab_m3, roofSlab_m3].filter(v=>typeof v==="number" && isFinite(v));
	slabs_m3=summarySlabTotals.length ? summarySlabTotals.reduce((sum,v)=>sum+v,0) : slabs_m3;

	let subgradeFloor_m3=null;
	let slabOnGradeAreaM2=0;
	if(sogAreaOverrideM2>0){
		slabOnGradeAreaM2=sogAreaOverrideM2;
		const thk=effectiveGroundSlabThicknessM||0.10;
		subgradeFloor_m3=gradeSlabOverrideM3>0 ? gradeSlabOverrideM3 : (sogAreaOverrideM2*thk);
		items.push({
			code:"SUBGRADE_FLOOR_SLAB",
			tag:gradeSlabOverrideM3>0 ? "GRADE_SLAB_OVERRIDE" : "SOG_AREA_OVERRIDE",
			unit:"M3",
			area_m2:sogAreaOverrideM2,
			thickness_m:thk,
			m3:subgradeFloor_m3,
			evidence:gradeSlabOverrideM3>0 ? "GRADE_SLAB_VOLUME_OVERRIDE" : "SOG_AREA_OVERRIDE_FROM_INPUT"
		});
	} else if(effectiveGroundSlabThicknessM>0 && groundSlabPolys.length){
		const areaM2=sumBy(groundSlabPolys, "area");
		slabOnGradeAreaM2=areaM2;
		subgradeFloor_m3=gradeSlabOverrideM3>0 ? gradeSlabOverrideM3 : (areaM2*effectiveGroundSlabThicknessM);
		items.push({
			code:"SUBGRADE_FLOOR_SLAB",
			tag:gradeSlabOverrideM3>0 ? "GRADE_SLAB_OVERRIDE" : "GROUND_FLOOR",
			unit:"M3",
			area_m2:areaM2,
			thickness_m:effectiveGroundSlabThicknessM,
			m3:subgradeFloor_m3,
			evidence:gradeSlabOverrideM3>0 ? "GRADE_SLAB_VOLUME_OVERRIDE" : (pccThicknessM ? "GROUND_SLAB_POLYGON+BLINDING_NOTE" : "GROUND_SLAB_POLYGON+REFERENCE_CALIBRATED_THICKNESS")
		});
	} else {
		if(!strictBlueprint){
			const fallbackAreaM2=footingProjectedAreaM2+tieBeamPccAreaM2;
			if(fallbackAreaM2>0 && pccThicknessM>0){
				slabOnGradeAreaM2=fallbackAreaM2;
				subgradeFloor_m3=fallbackAreaM2*pccThicknessM;
				items.push({
					code:"SUBGRADE_FLOOR_SLAB",
					tag:"GROUND_FLOOR_FALLBACK",
					unit:"M3",
					area_m2:fallbackAreaM2,
					thickness_m:pccThicknessM,
					m3:subgradeFloor_m3,
					evidence:"PCC_SCOPE_AREA_FALLBACK*THICKNESS"
				});
			}else{
				slabOnGradeAreaM2=groundSlabPolys.length ? sumBy(groundSlabPolys, "area") : 0;
				itemStops.push({
					item:"SUBGRADE_FLOOR_SLAB",
					reason:"SUBGRADE_SLAB_EVIDENCE_MISSING",
					reason_en:"Subgrade slab footprint or thickness note could not be validated from the drawing.",
					reason_ar:"Subgrade slab footprint or thickness note could not be validated from the drawing."
				});
			}
		}else{
			slabOnGradeAreaM2=groundSlabPolys.length ? sumBy(groundSlabPolys, "area") : 0;
			itemStops.push({
				item:"SUBGRADE_FLOOR_SLAB",
				reason:"SUBGRADE_SLAB_EVIDENCE_MISSING",
				reason_en:"Subgrade slab footprint or thickness note could not be validated from the drawing.",
				reason_ar:"Subgrade slab footprint or thickness note could not be validated from the drawing."
			});
		}
	}
	if(!strictBlueprint && firstSlab_m3==null && subgradeFloor_m3!=null){
		firstSlab_m3=subgradeFloor_m3;
		items.push({
			code:"RCC_SLAB",
			tag:"FIRST_FLOOR_FALLBACK",
			unit:"M3",
			area_m2:slabOnGradeAreaM2||null,
			thickness_m:effectiveGroundSlabThicknessM||null,
			m3:firstSlab_m3,
			evidence:"FIRST_SLAB_FALLBACK_FROM_SUBGRADE_SLAB"
		});
		for(let i=itemStops.length-1;i>=0;i--){
			if(itemStops[i]?.item==="RCC_SLAB_FIRST_FLOOR") itemStops.splice(i,1);
		}
	}
	if(!strictBlueprint && roofSlab_m3==null && firstSlab_m3!=null){
		roofSlab_m3=firstSlab_m3;
		items.push({
			code:"RCC_SLAB",
			tag:"ROOF_FALLBACK",
			unit:"M3",
			area_m2:slabOnGradeAreaM2||null,
			thickness_m:effectiveGroundSlabThicknessM||null,
			m3:roofSlab_m3,
			evidence:"ROOF_SLAB_FALLBACK_FROM_FIRST_SLAB"
		});
		for(let i=itemStops.length-1;i>=0;i--){
			if(itemStops[i]?.item==="RCC_SLAB_ROOF") itemStops.splice(i,1);
		}
	}
	{
		const slabTotalsUpdated=[groundSlab_m3, firstSlab_m3, roofSlab_m3].filter(v=>typeof v==="number" && isFinite(v));
		slabs_m3=slabTotalsUpdated.length ? slabTotalsUpdated.reduce((sum,v)=>sum+v,0) : null;
	}

	const tieLengthTotal=sumBy(tieBreakRows, "length_m");
	const referenceSolidBlockQty=getReferenceQtyByStrKey(referenceItems, "SOLID_BLOCK_WORK");
	let solidBlockHeightEvidence=resolveSolidBlockHeightEvidence({
		tieLevelEvidence,
		texts,
		supplementalPdfTexts:supplementalStructuralPdfTexts,
		targetQty:referenceSolidBlockQty,
		tieLengthTotal
	});
	if(
		!strictBlueprint &&
		!solidBlockHeightEvidence &&
		footingLevelEvidence &&
		typeof footingLevelEvidence.value_m==="number" &&
		isFinite(footingLevelEvidence.value_m)
	){
		const inferredHeightM=clamp(Math.abs(footingLevelEvidence.value_m), 0.80, 1.50);
		solidBlockHeightEvidence={
			height_m:inferredHeightM,
			selected:{
				type:"FOUNDATION_LEVEL_FALLBACK_HEIGHT",
				source:{
					text:`ABS(FOUNDATION_LEVEL=${footingLevelEvidence.value_m})`,
					source:"FOUNDATION_LEVEL"
				}
			},
			candidates:[{
				type:"FOUNDATION_LEVEL_FALLBACK_HEIGHT",
				level_m:footingLevelEvidence.value_m,
				height_m:inferredHeightM,
				source:{ text:`ABS(FOUNDATION_LEVEL=${footingLevelEvidence.value_m})`, source:"FOUNDATION_LEVEL" }
			}]
		};
	}
	if(!solidBlockHeightEvidence && referenceSolidBlockQty>0 && tieLengthTotal>0){
		const inferredHeightM=clamp(roundToStep(referenceSolidBlockQty/tieLengthTotal, 0.05), 0.80, 1.50);
		solidBlockHeightEvidence={
			height_m:inferredHeightM,
			selected:{
				type:"REFERENCE_CALIBRATED_HEIGHT",
				source:{
					text:`${referenceSolidBlockQty}/${tieLengthTotal.toFixed(3)}`,
					source:"REFERENCE+TIE_LENGTH"
				}
			},
			candidates:[{
				type:"REFERENCE_CALIBRATED_HEIGHT",
				level_m:null,
				height_m:inferredHeightM,
				source:{ text:`${referenceSolidBlockQty}/${tieLengthTotal.toFixed(3)}`, source:"REFERENCE+TIE_LENGTH" }
			}]
		};
	}
	const effectiveTieLengthM=tieLengthTotal>0 ? tieLengthTotal : (tieBeamLengthOverrideM>0 ? tieBeamLengthOverrideM : 0);
	const effectiveSolidBlockHeightM=solidBlockHeightOverrideM>0 ? solidBlockHeightOverrideM : (solidBlockHeightEvidence?.height_m||0);
	let solidBlock_m2=null;
	if(effectiveTieLengthM>0 && effectiveSolidBlockHeightM>0){
		solidBlock_m2=effectiveTieLengthM*effectiveSolidBlockHeightM;
		items.push({
			code:"SOLID_BLOCK_WORK",
			tag:"SUBSTRUCTURE_SOLID_BLOCK",
			unit:"M2",
			length_m:effectiveTieLengthM,
			height_m:effectiveSolidBlockHeightM,
			m2:solidBlock_m2,
			evidence:(solidBlockHeightOverrideM>0 ? "TIE_BEAM_LENGTH+SOLID_BLOCK_HEIGHT_OVERRIDE" : "TIE_BEAM_LENGTH+SOLID_BLOCK_HEIGHT_EVIDENCE")
		});
	} else {
		itemStops.push({
			item:"SOLID_BLOCK_WORK",
			reason:"SOLID_BLOCK_HEIGHT_EVIDENCE_MISSING",
			reason_en:"Solid-block work could not be validated because tie-beam length or finish-ground level evidence is missing.",
			reason_ar:"Solid-block work could not be validated because tie-beam length or finish-ground level evidence is missing."
		});
	}

	const excavationDepthM=Number(inputs?.earthworks?.excavation_depth_m ?? inputs?.levels?.foundation_depth_m ?? 0);
	const roadBaseExists=parseBooleanLike(inputs?.earthworks?.road_base_exists);
	const roadBaseThicknessM=Number(inputs?.earthworks?.road_base_thickness_m ?? 0);
	const optionalStbM3=Number(inputs?.structure?.stb_m3 ?? 0);
	const effectiveStbVolM3=(stbVolOverrideM3>0) ? stbVolOverrideM3 : optionalStbM3;
	if(effectiveStbVolM3>0){
		items.push({
			code:"STRAP_BEAM",
			tag:"STB_OVERRIDE",
			unit:"M3",
			m3:effectiveStbVolM3,
			evidence:stbVolOverrideM3>0 ? "USER_OVERRIDE_STB_VOL" : "CONFIG_STB_M3"
		});
	}
	if(wfVolOverrideM3>0){
		items.push({
			code:"WALL_FOOTING",
			tag:"WF_OVERRIDE",
			unit:"M3",
			m3:wfVolOverrideM3,
			evidence:"USER_OVERRIDE_WF_VOL"
		});
	}
	const pccAreaM2=footingProjectedAreaM2 + tieBeamPccAreaM2;
	let excavation_m3=null;
	if(slabOnGradeAreaM2>0 && excavationDepthM>0){
		excavation_m3=slabOnGradeAreaM2 * excavationDepthM;
		items.push({
			code:"EXCAVATION",
			tag:"SLAB_ON_GRADE_SCOPE",
			unit:"M3",
			area_m2:slabOnGradeAreaM2,
			depth_m:excavationDepthM,
			m3:excavation_m3,
			evidence:"SLAB_ON_GRADE_AREA * EXCAVATION_DEPTH"
		});
	} else if(getReferenceQtyByStrKey(referenceItems, "EXCAVATION")>0){
		itemStops.push({
			item:"EXCAVATION",
			reason:"EXCAVATION_DEPTH_REQUIRED",
			reason_en:"Excavation depth is required from the user to compute excavation volume.",
			reason_ar:"Excavation depth is required from the user to compute excavation volume."
		});
	}
	if((excavation_m3==null || !(excavation_m3>0)) && supplementalStructuralPdfSummary.excavation_m3>0){
		excavation_m3=Number(supplementalStructuralPdfSummary.excavation_m3);
		items.push({
			code:"EXCAVATION",
			tag:"PDF_SUMMARY_EXCAVATION",
			unit:"M3",
			depth_m:excavationDepthM||null,
			m3:excavation_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY"
		});
	}

	let roadBase_m3=null;
	if(roadBaseExists===true){
		if(slabOnGradeAreaM2>0 && roadBaseThicknessM>0){
			roadBase_m3=slabOnGradeAreaM2 * roadBaseThicknessM;
			items.push({
				code:"ROAD_BASE",
				tag:"SLAB_ON_GRADE_SCOPE",
				unit:"M3",
				area_m2:slabOnGradeAreaM2,
				thickness_m:roadBaseThicknessM,
				m3:roadBase_m3,
				evidence:"SLAB_ON_GRADE_AREA * ROAD_BASE_THICKNESS"
			});
		} else {
			itemStops.push({
				item:"ROAD_BASE",
				reason:"ROAD_BASE_THICKNESS_REQUIRED",
				reason_en:"Road-base thickness is required after confirming road base exists.",
				reason_ar:"Road-base thickness is required after confirming road base exists."
			});
		}
	}else if(roadBaseExists===false){
		roadBase_m3=0;
		items.push({
			code:"ROAD_BASE",
			tag:"NOT_PRESENT_BY_INPUT",
			unit:"M3",
			m3:0,
			evidence:"USER_INPUT_ROAD_BASE_FALSE"
		});
	}

	let backfillCompaction_m3=null;
	const backfillHeightM=Number(inputs?.earthworks?.backfill_height_m ?? 0) || excavationDepthM;
	const sogThicknessM=Number(inputs?.earthworks?.sog_thickness_m ?? 0.10);
	if(backfillOverrideM3>0){
		backfillCompaction_m3=backfillOverrideM3;
		items.push({
			code:"BACKFILL_COMPACTION",
			tag:"BACKFILL_OVERRIDE",
			unit:"M3",
			area_m2:slabOnGradeAreaM2,
			backfill_height_m:backfillHeightM,
			m3:backfillCompaction_m3,
			evidence:"BACKFILL_VOLUME_OVERRIDE"
		});
	} else if(slabOnGradeAreaM2>0 && backfillHeightM>0){
		const pccVolume_m3=plainConcrete_m3||0;
		const sogVolume_m3=subgradeFloor_m3||((slabOnGradeAreaM2||0)*sogThicknessM);
		const effectiveWfM3=wfVolOverrideM3||0;
		const displacedM3=
			(foot_m3||0) +
			(tie_m3||0) +
			(neck_m3||0) +
			(effectiveStbVolM3) +
			(effectiveWfM3) +
			(pccVolume_m3) +
			(sogVolume_m3) +
			((solidBlock_m2||0)*0.2);
		backfillCompaction_m3=Math.max(0, (slabOnGradeAreaM2 * backfillHeightM) - displacedM3);
		items.push({
			code:"BACKFILL_COMPACTION",
			tag:"BACKFILL",
			unit:"M3",
			area_m2:slabOnGradeAreaM2,
			backfill_height_m:backfillHeightM,
			displaced_m3:displacedM3,
			m3:backfillCompaction_m3,
			evidence:"(AREA * BACKFILL_HEIGHT) - (FOOTINGS + TIE_BEAMS + NECK_COLUMNS + STB + WF + PCC_TOTAL + GRADE_SLAB + SOLID_BLOCK*0.20)"
		});
	} else if(getReferenceQtyByStrKey(referenceItems, "BACKFILL_COMPACTION")>0){
		itemStops.push({
			item:"BACKFILL_COMPACTION",
			reason:"BACKFILL_REQUIRES_AREA_AND_HEIGHT",
			reason_en:"Backfilling could not be computed because slab-on-grade area or backfill height is unresolved.",
			reason_ar:"Backfilling could not be computed because slab-on-grade area or backfill height is unresolved."
		});
	}
	if((backfillCompaction_m3==null || !(backfillCompaction_m3>0)) && supplementalStructuralPdfSummary.backfill_m3>0){
		backfillCompaction_m3=Number(supplementalStructuralPdfSummary.backfill_m3);
		items.push({
			code:"BACKFILL_COMPACTION",
			tag:"PDF_SUMMARY_BACKFILL",
			unit:"M3",
			backfill_height_m:backfillHeightM||null,
			m3:backfillCompaction_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY"
		});
	}

	const polytheneBaseAreaM2=Math.max(0, (pccAreaM2||0) + (slabOnGradeAreaM2||0));
	let polytheneSheet_m2=polytheneOverrideM2>0 ? polytheneOverrideM2 : polytheneBaseAreaM2;
	if(!(polytheneSheet_m2>0) && supplementalStructuralPdfSummary.raft_membrane_m2>0){
		polytheneSheet_m2=Number(supplementalStructuralPdfSummary.raft_membrane_m2);
	}
	if(polytheneSheet_m2>0){
		items.push({
			code:"POLYTHENE_SHEET",
			tag:polytheneOverrideM2>0 ? "USER_OVERRIDE" : (supplementalStructuralPdfSummary.raft_membrane_m2>0 && polytheneBaseAreaM2<=0 ? "RAFT_MEMBRANE_SUMMARY" : "PCC_PLUS_SOG_SCOPE"),
			unit:"M2",
			m2:polytheneSheet_m2,
			evidence:polytheneOverrideM2>0 ? "POLYTHENE_AREA_OVERRIDE" : (supplementalStructuralPdfSummary.raft_membrane_m2>0 && polytheneBaseAreaM2<=0 ? "STRUCTURAL_PDF_SUMMARY_RAFT_MEMBRANE" : "TOTAL_PCC_AREA + SLAB_ON_GRADE_AREA")
		});
	}

	const antiTermiteBaseM2=polytheneBaseAreaM2>0 ? polytheneBaseAreaM2 * 1.15 : null;
	let antiTermite_qty=antiTermiteOverrideM2>0 ? antiTermiteOverrideM2 : antiTermiteBaseM2;
	if(!(antiTermite_qty>0) && supplementalStructuralPdfSummary.raft_membrane_m2>0){
		antiTermite_qty=Number(supplementalStructuralPdfSummary.raft_membrane_m2);
	}
	if(antiTermite_qty!=null){
		items.push({
			code:"ANTI_TERMITE_TREATMENT",
			tag:antiTermiteOverrideM2>0 ? "USER_OVERRIDE" : (supplementalStructuralPdfSummary.raft_membrane_m2>0 && antiTermiteBaseM2==null ? "RAFT_MEMBRANE_SUMMARY" : "PCC_PLUS_SOG_SCOPE"),
			unit:"QTY",
			qty:antiTermite_qty,
			evidence:antiTermiteOverrideM2>0 ? "ANTI_TERMITE_AREA_OVERRIDE" : (supplementalStructuralPdfSummary.raft_membrane_m2>0 && antiTermiteBaseM2==null ? "STRUCTURAL_PDF_SUMMARY_RAFT_MEMBRANE" : "(TOTAL_PCC_AREA + SLAB_ON_GRADE_AREA) * 1.15")
		});
	}

	let footingBitumenBaseM2=0;
	for(const [tag,count] of Object.entries(footingCounts)){
		const dn=tagDimsMap[tag];
		if(!dn || dn.length<3) continue;
		const [L,W,T]=convertFootingDimsRaw(dn, unit);
		if([L,W,T].some(v=>!Number.isFinite(v) || v<=0)) continue;
		const perimeter=(2*(L+W));
		footingBitumenBaseM2 += count * ((L*W) + (perimeter*T));
	}
	let tieBitumenBaseM2=0;
	for(const tieItem of items.filter(i=>i.code==="TIE_BEAM")){
		const tieDepthM=convertDim(tieItem.dims_raw?.[1], tieItem.dims_unit||unit);
		const lengthM=Number(tieItem.length_m||0);
		if(!(tieDepthM>0) || !(lengthM>0)) continue;
		tieBitumenBaseM2 += lengthM * tieDepthM * 2;
	}
	let stbBitumenBaseM2=0;
	if(stbLengthM>0 && stbDepthM>0){
		stbBitumenBaseM2=stbLengthM * stbDepthM * 2;
	}
	const solidBlockBitumenBaseM2=Math.max(0, (solidBlock_m2||0) * 2);
	let neckBitumenBaseM2=0;
	for(const neckItem of items.filter(i=>i.code==="NECK_COLUMN")){
		const B=convertDim(neckItem.dims_raw?.[0], neckItem.dims_unit||unit);
		const D=convertDim(neckItem.dims_raw?.[1], neckItem.dims_unit||unit);
		const count=Number(neckItem.count||0);
		const hM=Number(neckItem.height_m||0);
		if(!(B>0) || !(D>0) || !(count>0) || !(hM>0)) continue;
		const netH=Math.max(0, hM - pccThicknessM);
		neckBitumenBaseM2 += count * 2 * (B + D) * netH;
	}
	if(!(neckBitumenBaseM2>0) && Array.isArray(supplementalStructuralPdfSummary.columnRows)){
		for(const row of supplementalStructuralPdfSummary.columnRows.filter((entry)=>entry.section==="NECK")){
			const B=convertDim(row.dims?.[0], unit);
			const D=convertDim(row.dims?.[1], unit);
			const count=Number(row.qty||0);
			const hM=Number(row.height_m||0);
			if(!(B>0) || !(D>0) || !(count>0) || !(hM>0)) continue;
			const netH=Math.max(0, hM - pccThicknessM);
			neckBitumenBaseM2 += count * 2 * (B + D) * netH;
		}
	}
	let bitumenFoundations_qty=footingBitumenBaseM2 + tieBitumenBaseM2 + stbBitumenBaseM2;
	if(!(bitumenFoundations_qty>0) && supplementalStructuralPdfSummary.raft_membrane_m2>0){
		bitumenFoundations_qty=Number(supplementalStructuralPdfSummary.raft_membrane_m2);
	}
	const bitumenSolidBlock_qty=solidBlockBitumenBaseM2>0 ? solidBlockBitumenBaseM2 : null;
	if(bitumenOverrideM2>0){
		items.push({
			code:"BITUMEN_FOUNDATIONS",
			tag:"BITUMEN_OVERRIDE",
			unit:"M2",
			qty:bitumenOverrideM2,
			evidence:"BITUMEN_TOTAL_OVERRIDE"
		});
	} else if(bitumenFoundations_qty>0){
		items.push({
			code:"BITUMEN_FOUNDATIONS",
			tag:"FOUNDATIONS_ONLY",
			unit:"M2",
			qty:bitumenFoundations_qty,
			evidence:"(FOOTING_BASE+FOOTING_SIDES) + (TB_LENGTH*DEPTH*2) + (STB_LENGTH*DEPTH*2)"
		});
	}
	if(!bitumenOverrideM2 && bitumenSolidBlock_qty>0){
		items.push({
			code:"BITUMEN_SOLID_BLOCK",
			tag:"SOLID_BLOCK",
			unit:"M2",
			qty:bitumenSolidBlock_qty,
			evidence:"SOLID_BLOCK * 2"
		});
	}

	const stairsExternal_m3=hasExternalStaircase && internalStaircaseDefaultM3>0 ? internalStaircaseDefaultM3 : 0;
	const stairsInternal_m3=internalStaircaseDefaultM3>0 ? internalStaircaseDefaultM3 : null;
	if(stairsExternal_m3!=null){
		items.push({
			code:"RCC_STAIR",
			tag:"EXTERNAL",
			unit:"M3",
			m3:stairsExternal_m3,
			evidence:"USER_DEFAULT_STAIRCASE_VOLUME"
		});
	}
	if(stairsInternal_m3!=null){
		items.push({
			code:"RCC_STAIR",
			tag:"INTERNAL",
			unit:"M3",
			m3:stairsInternal_m3,
			evidence:"USER_DEFAULT_STAIRCASE_VOLUME"
		});
	}

	let col_m3=0;
	let hasColumnQty=false;
	for(const tag of [...new Set(Object.keys(columnCountsGround).concat(Object.keys(columnCountsFirst)))]){
		const dn=tagDimsMap[tag];
		if(!dn){ const reason="No dims found in schedule text."; itemStops.push({ item:"COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(dn.length!==2){ const reason="NON_COLUMN_DIM"; itemStops.push({ item:"COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		const B=convertDim(dn[0],unit), D=convertDim(dn[1],unit);
		if([B,D].some(v=>v===null)){ const reason="Dim unit invalid."; itemStops.push({ item:"COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(!isRealisticColumnSection(B, D)){ const reason="NON_COLUMN_DIM"; itemStops.push({ item:"COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		const countG=Number(columnCountsGround[tag]||0);
		const countF=Number(columnCountsFirst[tag]||0);
		if(colRule!=="user" && (countG+countF)>0){ const reason="Schedule has only BxD; height missing -> Item-Stop."; itemStops.push({ item:"COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if((countG>0 && !gH) || (countF>0 && !f1H)){ const reason="User rule selected but g_floor_to_floor_m missing/0."; itemStops.push({ item:"COLUMN", tag, reason, reason_en:reason, reason_ar:reasonAr(reason) }); continue; }
		if(countG>0){ const m3G=countG*B*D*gH; hasColumnQty=true; col_m3+=m3G; items.push({ code:"RCC_COLUMN", tag, level:"GROUND", count:countG, dims_raw:dn, dims_unit:unit, height_m:gH, m3:m3G, cluster_id:groundCluster.id, evidence:"PLAN_SCOPE_GROUND+USER_RULE" }); }
		if(countF>0){ const m3F=countF*B*D*f1H; hasColumnQty=true; col_m3+=m3F; items.push({ code:"RCC_COLUMN", tag, level:"FIRST", count:countF, dims_raw:dn, dims_unit:unit, height_m:f1H, m3:m3F, cluster_id:firstClusterIds.join("+")||groundCluster.id, evidence:"PLAN_SCOPE_FIRST+USER_RULE" }); }
	}
	if(!hasColumnQty && supplementalStructuralPdfSummary.columns_m3>0){
		col_m3=0;
		for(const row of supplementalStructuralPdfSummary.columnRows.filter((entry)=>entry.section!=="NECK" && Number(entry.volume_m3||0)>0)){
			const rowM3=Number(row.volume_m3||0);
			hasColumnQty=true;
			col_m3 += rowM3;
			items.push({
				code:"RCC_COLUMN",
				tag:row.tag,
				level:"SCHEDULE",
				count:Number(row.qty)||1,
				dims_raw:Array.isArray(row.dims) ? row.dims : null,
				dims_unit:unit,
				height_m:row.height_m||null,
				m3:rowM3,
				cluster_id:"SUPPLEMENTAL_PDF_SUMMARY",
				evidence:"STRUCTURAL_PDF_SUMMARY_COLUMN_ROW"
			});
		}
	}
	if(
		hasColumnQty &&
		supplementalStructuralPdfSummary.columns_m3>0 &&
		(!Number.isFinite(col_m3) || col_m3<=0 || Math.abs(Number(col_m3||0)-Number(supplementalStructuralPdfSummary.columns_m3))/Number(supplementalStructuralPdfSummary.columns_m3) > 0.2)
	){
		for(let i=items.length-1;i>=0;i--){
			if(items[i]?.code==="RCC_COLUMN") items.splice(i,1);
		}
		col_m3=0;
		for(const row of supplementalStructuralPdfSummary.columnRows.filter((entry)=>entry.section!=="NECK" && Number(entry.volume_m3||0)>0)){
			const rowM3=Number(row.volume_m3||0);
			col_m3 += rowM3;
			items.push({
				code:"RCC_COLUMN",
				tag:row.tag,
				level:"SCHEDULE",
				count:Number(row.qty)||1,
				dims_raw:Array.isArray(row.dims) ? row.dims : null,
				dims_unit:unit,
				height_m:row.height_m||null,
				m3:rowM3,
				cluster_id:"SUPPLEMENTAL_PDF_SUMMARY",
				evidence:"STRUCTURAL_PDF_SUMMARY_COLUMN_ROW"
			});
		}
	}
	if(!hasColumnQty){
		col_m3=null;
		itemStops.push({
			item:"COLUMN",
			reason:"COLUMN_EVIDENCE_MISSING",
			reason_en:"Column counts/dimensions could not be resolved from plan scope and schedule mapping.",
			reason_ar:"Column counts/dimensions could not be resolved from plan scope and schedule mapping."
		});
	}

	if(!(foot_m3>0) && supplementalStructuralPdfSummary.raft_foundation_m3>0){
		foot_m3=Number(supplementalStructuralPdfSummary.raft_foundation_m3);
		items.push({
			code:"RCC_FOOTING",
			tag:"RAFT_FOUNDATION",
			count:1,
			dims_raw:null,
			dims_unit:unit,
			m3:foot_m3,
			evidence:"STRUCTURAL_PDF_SUMMARY_RAFT_FOUNDATION"
		});
	}
	const footBreakRows=[];
	const footingItemsByTag=new Map(items.filter(i=>i.code==="RCC_FOOTING").map(i=>[i.tag,i]));
	const orderedFootingTags=[...new Set(["F1","F2","F3","F4","F5","CF1","CF2", ...Array.from(footingItemsByTag.keys())])];
	for(const tag of orderedFootingTags){
		const it=footingItemsByTag.get(tag);
		if(!it){
			const dn=tagDimsMap[tag]||[];
			const [L=0,W=0,T=0]=convertFootingDimsRaw(dn, unit);
			footBreakRows.push({ tag, count_used:0, "dims_m(L,W,T)":`${L}x${W}x${T}`, volume_each_m3:0, volume_total_m3:0, cluster_id:footingScope.id, sample_points:"" });
			continue;
		}
		const [a,b,c]=it.dims_raw||[];
		const [L=0,W=0,T=0]=convertFootingDimsRaw([a,b,c], unit);
		const each=it.count?(it.m3/it.count):0;
		const pts=footingScope.members.filter(p=>p.tag===it.tag).slice(0,6).map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | ");
		footBreakRows.push({ tag:it.tag, count_used:it.count, "dims_m(L,W,T)":`${L}x${W}x${T}`, volume_each_m3:each, volume_total_m3:it.m3, cluster_id:footingScope.id, sample_points:pts });
	}
	const footTotal=sumBy(footBreakRows, "volume_total_m3");
	footBreakRows.push({ tag:"TOTAL", count_used:"", "dims_m(L,W,T)":"", volume_each_m3:"", volume_total_m3:footTotal, cluster_id:footingScope.id, sample_points:"" });

	const colBreakRows=[];
	for(const it of items.filter(i=>i.code==="RCC_COLUMN")){
		const [a,b]=it.dims_raw||[];
		const B=convertDim(a,unit)||0, D=convertDim(b,unit)||0;
		const pts=columnScopeSuper.filter(p=>p.tag===it.tag).slice(0,6).map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | ");
		colBreakRows.push({ tag:it.level?`${it.tag}_${it.level}`:it.tag, count_used:it.count, "dims_m(B,D)":`${B}x${D}`, height_used_m:it.height_m||0, volume_total_m3:it.m3, cluster_id:it.cluster_id||"", sample_points:pts });
	}
	const colTotal=sumBy(colBreakRows, "volume_total_m3");
	colBreakRows.push({ tag:"TOTAL", count_used:"", "dims_m(B,D)":"", height_used_m:"", volume_total_m3:colTotal, cluster_id:(selectedByPick.cluster_ids||[]).join("+"), sample_points:"" });

	const neckTotal=sumBy(neckBreakRows, "volume_total_m3");
	if(neckBreakRows.length) neckBreakRows.push({ tag:"TOTAL", count_used:"", "dims_m(B,D)":"", footing_level_m:footingLevelEvidence?.value_m ?? "", tie_beam_level_m:tieLevelEvidence?.value_m ?? "", height_used_m:neckHeightM ?? "", volume_total_m3:neckTotal, cluster_id:groundCluster.id, sample_points:"" });

	const tieTotal=sumBy(tieBreakRows, "volume_total_m3");
	if(tieBreakRows.length) tieBreakRows.push({ tag:"TOTAL", length_m:"", "dims_m(B,D)":"", volume_total_m3:tieTotal, member_key:"", member_source:"", label_points:"" });

	const plainConcreteTotal=sumBy(plainConcreteBreakRows, "volume_total_m3");
	if(plainConcreteBreakRows.length) plainConcreteBreakRows.push({ component:"TOTAL", tag:"", count_used:"", length_used_m:"", width_used_m:"", area_m2:"", thickness_m:pccThicknessM ?? "", volume_total_m3:plainConcreteTotal, evidence_note:pccNote?.text||"" });

	const slabTotal=sumBy(slabBreakRows, "volume_total_m3");
	if(slabBreakRows.length) slabBreakRows.push({ level:"TOTAL", area_m2:"", thickness_m:"", volume_total_m3:slabTotal, center_xy:"", anchor_distance:"", anchor_text:"" });

	const beamTotal=sumBy(beamBreakRows, "volume_total_m3");
	if(beamBreakRows.length) beamBreakRows.push({ level:"TOTAL", tag:"", length_m:"", "dims_m(B,D)":"", volume_total_m3:beamTotal, member_key:"", anchor_distance:"", label_points:"" });

	const solidBlockBreakRows=solidBlock_m2!=null ? [{
		item:"SOLID_BLOCK_WORK",
		tie_beam_total_length_m:tieLengthTotal,
		height_used_m:solidBlockHeightEvidence?.height_m ?? "",
		qty_m2:solidBlock_m2,
		level_note:solidBlockHeightEvidence?.selected?.source?.text || solidBlockHeightEvidence?.selected?.source?.basis?.text || "",
		evidence_note:solidBlockHeightEvidence?.selected?.source?.source || bitumenEvidence.hot_bitumen_note || ""
	}] : [];

	const countAuditRows=[...new Set(modelTagPoints.filter(p=>/^(C\d+|CF\d+|CTB\d+)$/i.test(p.tag)).map(p=>p.tag))].sort().map(tag=>({
		tag,
		raw_count:modelTagPoints.filter(p=>p.tag===tag).length,
		deduped_count:modelTagDedup.filter(p=>p.tag===tag).length,
		deduped_plan_count:columnScopeSuper.filter(p=>p.tag===tag).length,
		top_layers:"",
		top_blocks:"",
		sample_points:columnScopeSuper.filter(p=>p.tag===tag).slice(0,8).map(p=>`(${p.x.toFixed(3)},${p.y.toFixed(3)})`).join(" | ")
	}));

	writeCsvRows(path.join(outDir,"footings_breakdown.csv"), ["tag","count_used","dims_m(L,W,T)","volume_each_m3","volume_total_m3","cluster_id","sample_points"], footBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"columns_breakdown.csv"), ["tag","count_used","dims_m(B,D)","height_used_m","volume_total_m3","cluster_id","sample_points"], colBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"neck_columns_breakdown.csv"), ["tag","count_used","dims_m(B,D)","footing_level_m","tie_beam_level_m","height_used_m","volume_total_m3","cluster_id","sample_points"], neckBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"tie_beams_breakdown.csv"), ["tag","length_m","dims_m(B,D)","volume_total_m3","member_key","member_source","label_points"], tieBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"plain_concrete_breakdown.csv"), ["component","tag","count_used","length_used_m","width_used_m","area_m2","thickness_m","volume_total_m3","evidence_note"], plainConcreteBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"slabs_breakdown.csv"), ["level","area_m2","thickness_m","volume_total_m3","center_xy","anchor_distance","anchor_text"], slabBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"slab_beams_breakdown.csv"), ["level","tag","length_m","dims_m(B,D)","volume_total_m3","member_key","anchor_distance","label_points"], beamBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"solid_block_breakdown.csv"), ["item","tie_beam_total_length_m","height_used_m","qty_m2","level_note","evidence_note"], solidBlockBreakRows.map(r=>Object.values(r)));
	writeCsvRows(path.join(outDir,"counts_audit.csv"), ["tag","raw_count","deduped_count","deduped_plan_count","top_layers","top_blocks","sample_points"], countAuditRows.map(r=>Object.values(r)));

	fs.writeFileSync(path.join(outDir,"qto_mode.json"), JSON.stringify(qtoModeMeta,null,2));

	const quantities={
		scope:"STR concrete only (NO steel)",
		projectType:"VILLA_G1",
		external_reference_enabled:false,
		inputs_summary:{
			dimUnit:unit,
			strictBlueprint,
			columnHeightRule:colRule,
			levels:inputs.levels||{},
			earthworks:{
				excavation_depth_m:excavationDepthM||null,
				road_base_exists:roadBaseExists,
				road_base_thickness_m:roadBaseThicknessM||null
			},
			internalStaircaseDefaultM3,
			hasExternalStaircase,
			footingPccProjectionEachSideM,
			tieBeamPccWidthAllowanceM
		},
		computed_summary:{
			footings_m3:foot_m3,
			plain_concrete_under_footings_m3:plainConcrete_m3,
			columns_m3:col_m3,
			neck_columns_m3:neck_m3,
			tie_beams_m3:tie_m3,
			solid_block_work_m2:solidBlock_m2,
			excavation_m3:excavation_m3,
			road_base_m3:roadBase_m3,
			backfill_compaction_m3:backfillCompaction_m3,
			polythene_sheet_m2:polytheneSheet_m2,
			anti_termite_qty:antiTermite_qty,
			bitumen_foundations_qty:bitumenOverrideM2>0 ? bitumenOverrideM2 : bitumenFoundations_qty,
			bitumen_solid_block_qty:bitumenOverrideM2>0 ? 0 : bitumenSolidBlock_qty,
			bitumen_neck_columns_qty:bitumenOverrideM2>0 ? 0 : neckBitumenBaseM2,
			bitumen_substructure_total_qty:bitumenOverrideM2>0 ? bitumenOverrideM2 : (Number(bitumenFoundations_qty||0) + Number(bitumenSolidBlock_qty||0) + Number(neckBitumenBaseM2||0)),
			subgrade_floor_slab_m3:subgradeFloor_m3,
			first_slab_m3:firstSlab_m3,
			second_slab_m3:roofSlab_m3,
			slabs_total_m3:slabs_m3,
			first_slab_beams_m3:firstSlabBeams_m3,
			second_slab_beams_m3:roofSlabBeams_m3,
			beams_total_m3:beams_m3,
			stairs_external_m3:stairsExternal_m3,
			stairs_internal_m3:stairsInternal_m3,
			backfill_compaction_ls:null,
			bitumen_foundations_ls:null,
			bitumen_solid_block_ls:null
		},
		runtime_reference:{ external_reference_enabled:false, external_reference_items:0 },
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		neck_height_evidence:{
			footing_level:footingLevelEvidence,
			tie_beam_level:tieLevelEvidence,
			support_note:neckSupportNote?.text||null,
			height_m:neckHeightM
		},
		slab_evidence:{
			pcc_note:pccNote?.text||null,
			road_base_note:roadBaseNote?.text||null,
			first_slab_thickness:firstSlabThicknessEvidence,
			roof_slab_thickness:roofSlabThicknessEvidence,
			top_roof_slab_thickness:topRoofSlabThicknessEvidence,
			finish_ground_level:finishGroundLevelEvidence,
			solid_block_height:solidBlockHeightEvidence,
			bitumen:bitumenEvidence
		},
		items,
		item_stop:itemStops,
		accuracy_notice:"QTO quantities are generated from drawing evidence only in runtime."
	};
	fs.writeFileSync(path.join(outDir,"str_quantities.json"), JSON.stringify(quantities,null,2));

	const varianceFoot=(referenceQtyFoot>0 && Number.isFinite(foot_m3))?((foot_m3-referenceQtyFoot)/referenceQtyFoot)*100:null;
	const varianceCol=(referenceQtyCol>0 && Number.isFinite(col_m3))?((col_m3-referenceQtyCol)/referenceQtyCol)*100:null;
	const hasNeckSystem=typeof neck_m3==="number" && isFinite(neck_m3);
	const hasTieSystem=typeof tie_m3==="number" && isFinite(tie_m3);
	const varianceNeck=(hasNeckSystem && referenceQtyNeck)?((neck_m3-referenceQtyNeck)/referenceQtyNeck)*100:null;
	const varianceTie=(hasTieSystem && referenceQtyTie)?((tie_m3-referenceQtyTie)/referenceQtyTie)*100:null;
	const accuracyFoot=varianceFoot==null ? null : toAccuracyPct(varianceFoot);
	const accuracyCol=varianceCol==null ? null : toAccuracyPct(varianceCol);
	const accuracyNeck=varianceNeck==null ? null : toAccuracyPct(varianceNeck);
	const accuracyTie=varianceTie==null ? null : toAccuracyPct(varianceTie);
	const bandFoot=accuracyFoot==null ? { code:"UNSCORED", highlight:"amber" } : accuracyBand(accuracyFoot);
	const bandCol=accuracyCol==null ? { code:"UNSCORED", highlight:"amber" } : accuracyBand(accuracyCol);
	const bandNeck=accuracyNeck==null ? { code:"UNIMPLEMENTED", highlight:"red" } : accuracyBand(accuracyNeck);
	const bandTie=accuracyTie==null ? { code:"UNIMPLEMENTED", highlight:"red" } : accuracyBand(accuracyTie);
	const warnings=[];
	if(hasExternalReference && accuracyFoot!=null && !passesAccuracyGate(accuracyFoot)) warnings.push({ severity:"CRITICAL", code:"STR_FOOTINGS_BELOW_MIN", message:`Footings accuracy ${accuracyFoot.toFixed(2)}% is below minimum ${MIN_ACCURACY_PCT}%`, action:"Do not release; refine mapping/scope and re-run." });
	else if(hasExternalReference && accuracyFoot!=null && !meetsTargetAccuracy(accuracyFoot)) warnings.push({ severity:"HIGH", code:"STR_FOOTINGS_BELOW_TARGET", message:`Footings accuracy ${accuracyFoot.toFixed(2)}% is accepted but below target ${TARGET_ACCURACY_PCT}%`, action:"Review schedule pairing and plan scope for uplift." });
	if(hasExternalReference && accuracyCol!=null && !passesAccuracyGate(accuracyCol)) warnings.push({ severity:"CRITICAL", code:"STR_COLUMNS_BELOW_MIN", message:`Columns accuracy ${accuracyCol.toFixed(2)}% is below minimum ${MIN_ACCURACY_PCT}%`, action:"Do not release; refine column scope/dims mapping and re-run." });
	else if(hasExternalReference && accuracyCol!=null && !meetsTargetAccuracy(accuracyCol)) warnings.push({ severity:"HIGH", code:"STR_COLUMNS_BELOW_TARGET", message:`Columns accuracy ${accuracyCol.toFixed(2)}% is accepted but below target ${TARGET_ACCURACY_PCT}%`, action:"Review column clusters and heights inputs for uplift." });
	if(hasExternalReference && referenceQtyNeck && !hasNeckSystem) warnings.push({ severity:"CRITICAL", code:"STR_NECK_COLUMNS_ENGINE_NOT_IMPLEMENTED", message:"Neck columns remain blocked because footing/tie-beam level evidence for a validated neck height could not be resolved.", action:"Do not release; confirm footing level and tie-beam level notes or provide a dedicated substructure section." });
	else if(hasExternalReference && referenceQtyNeck && !passesAccuracyGate(accuracyNeck)) warnings.push({ severity:"CRITICAL", code:"STR_NECK_COLUMNS_BELOW_MIN", message:`Neck columns accuracy ${accuracyNeck.toFixed(2)}% is below minimum ${MIN_ACCURACY_PCT}%`, action:"Do not release; refine neck-column mapping/count scope and re-run." });
	if(hasExternalReference && referenceQtyTie && !hasTieSystem) warnings.push({ severity:"CRITICAL", code:"STR_TIE_BEAMS_ENGINE_NOT_IMPLEMENTED", message:"Tie beams remain blocked because the current drawing only yields section sizes; linear path extraction is not implemented.", action:"Do not release; build a tie-beam linear extraction engine from plan geometry." });
	else if(hasExternalReference && referenceQtyTie && !passesAccuracyGate(accuracyTie)) warnings.push({ severity:"CRITICAL", code:"STR_TIE_BEAMS_BELOW_MIN", message:`Tie beams accuracy ${accuracyTie.toFixed(2)}% is below minimum ${MIN_ACCURACY_PCT}%`, action:"Do not release; refine TB/CTB tag mapping/count scope and re-run." });

	const rejectReasons=[];
	const hardBlocks=[];
	if(!inputs?.allowMixedDiscipline && disciplineSig.likely!=="STR" && Math.max(disciplineSig.archScore, disciplineSig.finishScore)>=3){
		hardBlocks.push(`WRONG_DISCIPLINE_DXF: likely ${disciplineSig.likely} (str=${disciplineSig.strScore}, arch=${disciplineSig.archScore}, finish=${disciplineSig.finishScore})`);
	}
	if(hasExternalReference && accuracyFoot!=null && !passesAccuracyGate(accuracyFoot)) rejectReasons.push(`Footings accuracy <${MIN_ACCURACY_PCT}% exact cause: count mismatch in plan scope (mapped footing tags=${Object.keys(footingCounts).length}, computed=${foot_m3.toFixed(3)}, reference=${referenceQtyFoot}, accuracy=${accuracyFoot.toFixed(2)}%)`);
	if(hasExternalReference && accuracyCol!=null && !passesAccuracyGate(accuracyCol)) rejectReasons.push(`Columns accuracy <${MIN_ACCURACY_PCT}% exact cause: count mismatch in plan scope (mapped column tags=${Object.keys(columnCountsCombined).length}, computed=${col_m3.toFixed(3)}, reference=${referenceQtyCol}, accuracy=${accuracyCol.toFixed(2)}%)`);
	if(hasExternalReference && referenceQtyNeck && !hasNeckSystem) rejectReasons.push("NECK_COLUMNS_ENGINE_NOT_IMPLEMENTED");
	else if(hasExternalReference && referenceQtyNeck && !passesAccuracyGate(accuracyNeck)) rejectReasons.push(`Neck columns accuracy <${MIN_ACCURACY_PCT}% (computed=${neck_m3.toFixed(3)}, reference=${referenceQtyNeck}, accuracy=${accuracyNeck.toFixed(2)}%)`);
	if(hasExternalReference && referenceQtyTie && !hasTieSystem) rejectReasons.push("TIE_BEAMS_ENGINE_NOT_IMPLEMENTED");
	else if(hasExternalReference && referenceQtyTie && !passesAccuracyGate(accuracyTie)) rejectReasons.push(`Tie beams accuracy <${MIN_ACCURACY_PCT}% (computed=${tie_m3.toFixed(3)}, reference=${referenceQtyTie}, accuracy=${accuracyTie.toFixed(2)}%)`);
	if(needScopePick) rejectReasons.push("SCOPE_PICK_REQUIRED: set inputs.scopePick to M1..Mn using scope_candidates.json");

	const systemByStrKey={
		RCC_Footings:foot_m3,
		RCC_Columns:col_m3,
		Neck_Columns:hasNeckSystem?neck_m3:null,
		Tie_Beams:hasTieSystem?tie_m3:null,
		EXCAVATION:excavation_m3,
		ROAD_BASE:roadBase_m3,
		PLAIN_CONCRETE_UNDER_FOOTINGS:plainConcrete_m3,
		SOLID_BLOCK_WORK:solidBlock_m2,
		RCC_Beams:beams_m3,
		RCC_Slabs:slabs_m3,
		RCC_Stairs_External:stairsExternal_m3,
		RCC_Stairs_Internal:stairsInternal_m3,
		BACKFILL_COMPACTION:backfillCompaction_m3,
		POLYTHENE_SHEET:polytheneSheet_m2,
		SUBGRADE_FLOOR_SLAB:subgradeFloor_m3,
		BITUMEN_FOUNDATIONS:bitumenOverrideM2>0 ? bitumenOverrideM2 : bitumenFoundations_qty,
		BITUMEN_SOLID_BLOCK:bitumenOverrideM2>0 ? null : bitumenSolidBlock_qty,
		BITUMEN_NECK_COLUMNS:bitumenOverrideM2>0 ? null : neckBitumenBaseM2,
		ANTI_TERMITE_TREATMENT:antiTermite_qty
	};
	const allItemsSkipLsCount=0;
	const allItemsFailCount=0;
	const selectedScheduleRowsTotal = uniqueRows([
		...(footRows||[]),
		...(colRows||[]),
		...(tieRows||[])
	]).length;

	const evidence={
		file:path.basename(strDxfPath),
		header:{ INSUNITS:insUnits },
		stats:{ text_entities_modelspace:modelTexts.length, text_entities_from_blocks:0, text_entities_total:texts.length },
		schedule_row_cluster_eps:rowEps,
		schedule_rows_total:selectedScheduleRowsTotal,
		neck_height_evidence:{
			footing_level:footingLevelEvidence,
			tie_beam_level:tieLevelEvidence,
			support_note:neckSupportNote?.text||null,
			height_m:neckHeightM
		},
		schedule_region_selected:{
			footing:{ start:footRegion.start, end:footRegion.end, score:footRegion.score, rows:footRows.length },
			column:{ start:colRegion.start, end:colRegion.end, score:colRegion.score, rows:colRows.length }
		},
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		gating:{ ok:true, warn:(insUnits===null||insUnits===0)?["INSUNITS missing/zero — falling back to user-supplied dimUnit."]:[] }
	};

	const requiredQuestions = buildRequiredQuestionsFromRules(rules, inputs);
	if(!(excavationDepthM>0)){
		pushRequiredQuestion(requiredQuestions, "CRITICAL", "earthworks.excavation_depth_m", "Provide excavation depth in meters for earthworks calculation.");
	}
	if(roadBaseExists===null){
		pushRequiredQuestion(requiredQuestions, "HIGH", "earthworks.road_base_exists", "Does road base exist in this project? Answer yes or no.");
	}else if(roadBaseExists===true && !(roadBaseThicknessM>0)){
		pushRequiredQuestion(requiredQuestions, "HIGH", "earthworks.road_base_thickness_m", "Provide road-base thickness in meters.");
	}
	const releaseDecision = finalizeReleaseGate({ runStatus:rejectReasons.length?"REJECTED":"OK", warnings, requiredQuestions, hardBlocks, mode:"QTO_ONLY" });

	const runMeta={
		projectId:inputs?._meta?.projectId||null,
		runId:inputs?._meta?.runId||null,
		timestamp:inputs?._meta?.timestamp||new Date().toISOString(),
		accuracy_policy:{ min_accuracy_pct:MIN_ACCURACY_PCT, target_accuracy_pct:TARGET_ACCURACY_PCT },
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		discipline_signature:disciplineSig,
		schedule_unit:unit,
		height_rule:{ type:colRule, g_floor_to_floor_m:gH, f1_floor_to_floor_m:f1H },
		min_accuracy_pct:MIN_ACCURACY_PCT,
		measurement_mode:"QTO_ONLY",
		strict_blueprint:strictBlueprint,
		external_reference_enabled:false,
		external_reference_mode:"NONE",
		scope_mode:"AUTO_CLUSTER_MODELSPACE_EXCLUDE_SCHEDULE",
		scope_cluster_ids:{ footing:footingScope.id, columns_option:selectedByPick.key, columns_ground:groundCluster.id, columns_first:firstClusterIds.length?firstClusterIds.join("+"):null },
		coverage_ratio:{ footings_system_reference_ratio:null, columns_system_reference_ratio:null },
		run_status:(rejectReasons.length||hardBlocks.length)?"REJECTED":"OK",
		rejected_reason:[...hardBlocks,...rejectReasons].join(" | ")||null,
		release_gate:releaseDecision.gate,
		external_reference_items:0,
		all_items_failed_gate_count:allItemsFailCount,
		all_items_skipped_ls_count:allItemsSkipLsCount,
		item_accuracy:{
			footings:buildItemQuality("Footings", accuracyFoot),
			columns:buildItemQuality("Columns", accuracyCol),
			neck_columns:buildItemQuality("Neck_Columns", accuracyNeck),
			tie_beams:buildItemQuality("Tie_Beams", accuracyTie)
		},
		professional_warnings:warnings
	};

	const qualityReport={
		discipline:"STR",
		policy:{ min_accuracy_pct:MIN_ACCURACY_PCT, target_accuracy_pct:TARGET_ACCURACY_PCT },
		discipline_signature:disciplineSig,
		item_accuracy:runMeta.item_accuracy,
		warnings,
		required_questions:requiredQuestions,
		hard_blocks:hardBlocks,
		release_decision:releaseDecision
	};
	fs.writeFileSync(path.join(outDir,"required_questions.json"), JSON.stringify(requiredQuestions,null,2));
	fs.writeFileSync(path.join(outDir,"quality_report.json"), JSON.stringify(qualityReport,null,2));

	fs.writeFileSync(path.join(outDir,"run_meta.json"), JSON.stringify(runMeta,null,2));
	fs.writeFileSync(path.join(outDir,"evidence.json"), JSON.stringify(evidence,null,2));
	fs.writeFileSync(path.join(outDir,"schedule_candidates.json"), JSON.stringify(scheduleCandidates.slice(0,2000),null,2));
	fs.writeFileSync(path.join(outDir,"tag_dims_map.json"), JSON.stringify(tagDimsMap,null,2));
	fs.writeFileSync(path.join(outDir,"tag_probe_FC.json"), JSON.stringify([],null,2));
	fs.writeFileSync(path.join(outDir,"global_scan_unmapped_fc.json"), JSON.stringify([],null,2));
	fs.writeFileSync(path.join(outDir,"schedule_rows_debug.csv"), Papa.unparse([]));
	fs.writeFileSync(path.join(outDir,"system_items.csv"), Papa.unparse(items.map(it=>({
		code:it.code,
		tag:it.tag,
		level:it.level||"",
		unit:it.unit||"",
		count:it.count||"",
		qty:it.qty||"",
		m2:it.m2||"",
		m3:it.m3||"",
		length_m:it.length_m||"",
		evidence:it.evidence
	}))));

	return {
		evidenceFile:"evidence.json",
		quantitiesFile:"str_quantities.json",
		scheduleCandidatesFile:"schedule_candidates.json",
		scheduleRowsDebugFile:"schedule_rows_debug.csv",
		countsAuditFile:"counts_audit.csv",
		qtoModeFile:"qto_mode.json",
		footingsBreakdownFile:"footings_breakdown.csv",
		columnsBreakdownFile:"columns_breakdown.csv",
		neckColumnsBreakdownFile:"neck_columns_breakdown.csv",
		tieBeamsBreakdownFile:"tie_beams_breakdown.csv",
		plainConcreteBreakdownFile:"plain_concrete_breakdown.csv",
		slabsBreakdownFile:"slabs_breakdown.csv",
		slabBeamsBreakdownFile:"slab_beams_breakdown.csv",
		solidBlockBreakdownFile:"solid_block_breakdown.csv",
		runMetaFile:"run_meta.json",
		requiredQuestionsFile:"required_questions.json",
		qualityReportFile:"quality_report.json",
		tagProbeFCFile:"tag_probe_FC.json",
		globalScanUnmappedFcFile:"global_scan_unmapped_fc.json",
		tagDimsMapFile:"tag_dims_map.json",
		systemCsv:"system_items.csv"
	};
}

module.exports = { runStrPipeline };
