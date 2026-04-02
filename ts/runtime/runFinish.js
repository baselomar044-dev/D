const fs = require("fs");
const path = require("path");
const DxfParser = require("dxf-parser");
const XLSX = require("xlsx");
const Papa = require("papaparse");
const { MIN_ACCURACY_PCT, TARGET_ACCURACY_PCT, HONEST_MODE } = require("./accuracyPolicy");
const { loadRules } = require("./rules");
const { detectDisciplineSignature, buildRequiredQuestionsFromRules, finalizeReleaseGate } = require("./quality");
const { runArchPipeline } = require("./runArch");
const {
	collectTextsFromEntities,
	flattenDxfEntities,
	getCadLengthUnit,
	unitScaleToMeters,
	scaleEntitiesToMeters,
	applyScopeRectToEntities,
	applyScopeCircleToEntities
} = require("./dxfRuntimeUtils");
const {
	pairWalls: pairWallsShared,
	isLikelyWallLayer
} = require("./wallRuntimeUtils");

function loadFinishExtractionContext(dxfPath, inputs){
	const parser=new DxfParser();
	const dxf=parser.parseSync(fs.readFileSync(dxfPath, "utf8"));
	const cadUnit=getCadLengthUnit(dxf.header, inputs?.dimUnit||null);
	const scaleFactor=unitScaleToMeters(cadUnit);
	const entitiesRaw=scaleEntitiesToMeters(flattenDxfEntities(dxf), scaleFactor);
	const entitiesScopedRect=applyScopeRectToEntities(entitiesRaw, inputs?.scopeRect);
	const entities=applyScopeCircleToEntities(entitiesScopedRect, inputs?.scopeCenter, inputs?.scopeRadiusM ?? inputs?.scopeRadius);
	const texts=collectTextsFromEntities(entities);
	const disciplineSig=detectDisciplineSignature(texts);
	const allSegments=extractSegments(entities);
	let planScopes=detectPlanScopes(texts, allSegments);
	if(Object.keys(planScopes).length<2){
		const blockScopes=inferPlanScopesFromTopLevelBlocks(dxf, scaleFactor, texts);
		if(Object.keys(blockScopes).length>Object.keys(planScopes).length) planScopes=blockScopes;
	}
	const wallPairs=pairWalls(allSegments.filter(seg=>isLikelyWallLayer(seg.layer)));
	if(!Object.keys(planScopes).length){
		const inferredScopes=inferPlanScopesFromPairs(wallPairs);
		if(Object.keys(inferredScopes).length) planScopes=inferredScopes;
	}
	const roomLabels=buildRoomLabels(texts, planScopes);
	return {
		dxfPath,
		cadUnit,
		texts,
		disciplineSig,
		allSegments,
		planScopes,
		wallPairs,
		roomLabels
	};
}

function stripCadMarkup(text){
	return String(text||"")
		.replace(/\\[A-Za-z][^;]*;/g, " ")
		.replace(/[{}]/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}

function roundN(v, n=3){
	const p=Math.pow(10,n);
	return Math.round((Number(v)||0)*p)/p;
}

function scopeKeyFromPlanLayer(layer){
	const value=String(layer||"").toUpperCase();
	if(/ARC_PL_\d+-GR\b/.test(value)) return "GROUND";
	if(/ARC_PL_\d+-F1\b/.test(value)) return "FIRST";
	if(/ARC_PL_\d+-RO\b/.test(value)) return "ROOF";
	if(/ARC_PL_\d+-(TR|TOP)\b/.test(value)) return "TOP_ROOF";
	return null;
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
		if(Math.abs(dy)<=0.02){
			segs.push({ id:id++, ori:"H", c:(y1+y2)/2, a:Math.min(x1,x2), b:Math.max(x1,x2), layer:layer||"", len:Math.abs(x2-x1) });
		}else if(Math.abs(dx)<=0.02){
			segs.push({ id:id++, ori:"V", c:(x1+x2)/2, a:Math.min(y1,y2), b:Math.max(y1,y2), layer:layer||"", len:Math.abs(y2-y1) });
		}
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
	return pairWallsShared(segments);
}

function detectPlanScopes(texts, segments=[]){
	const names=[
		{ key:"GROUND", re:/\b(?:GROUND\s+FLOOR|G\.?\s*F\.?)\s+PLAN\b/i },
		{ key:"FIRST", re:/\b(?:FIRST|1ST|F\.?\s*F\.?)\s+FLOOR\s+PLAN\b|\b1ST\s+FL(?:OOR)?\s+PLAN\b/i },
		{ key:"ROOF", re:/\b(?:ROOF|R\.?\s*F\.?)\s+PLAN\b/i },
		{ key:"TOP_ROOF", re:/\bTOP(?:\s+OF)?\s+ROOF\s+PLAN\b/i }
	];
	const matches=[];
	for(const row of (texts||[])){
		if(typeof row.x!=="number" || typeof row.y!=="number") continue;
		const cleaned=stripCadMarkup(row.text).toUpperCase();
		const match=names.find(entry=>entry.re.test(cleaned));
		if(match) matches.push({ name:match.key, x:row.x, y:row.y });
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
			if(seg.ori==="H"){
				pushPoint(key, seg.a, seg.c);
				pushPoint(key, seg.b, seg.c);
			}else{
				pushPoint(key, seg.c, seg.a);
				pushPoint(key, seg.c, seg.b);
			}
		}
		const scopes={};
		for(const [key, pts] of Object.entries(grouped)){
			const xs=pts.map((p)=>p.x).filter((v)=>typeof v==="number" && isFinite(v));
			const ys=pts.map((p)=>p.y).filter((v)=>typeof v==="number" && isFinite(v));
			if(xs.length<8 || ys.length<8) continue;
			const margin=1.5;
			scopes[key]={
				name:key,
				title_x:(Math.min(...xs)+Math.max(...xs))/2,
				title_y:Math.max(...ys),
				x1:Math.min(...xs)-margin,
				x2:Math.max(...xs)+margin,
				y1:Math.min(...ys)-margin,
				y2:Math.max(...ys)+margin,
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
		.map((group)=>({
			y:group.y,
			items:Object.values(group.items.reduce((acc,item)=>{
				if(!acc[item.name] || item.x<acc[item.name].x) acc[item.name]=item;
				return acc;
			}, {}))
		}))
		.filter(group=>group.items.length>=3 && group.items.some(item=>item.name==="GROUND") && group.items.some(item=>item.name==="FIRST"))
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
		scopes[current.name]={
			name:current.name,
			x1:prev ? ((prev.x+current.x)/2) : (current.x-(span/2)),
			x2:next ? ((current.x+next.x)/2) : (current.x+(span/2)),
			y1:minGlobalY-yPad,
			y2:maxGlobalY+yPad,
			title_y:current.y,
			title_x:current.x,
			source:"TITLE_BAND_EXPANDED"
		};
	}
	return scopes;
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
		const xs=points.map((point)=>point.x*scaleFactor);
		const ys=points.map((point)=>point.y*scaleFactor);
		const rect={
			name:scopeName,
			x1:Math.min(...xs)-1.0,
			x2:Math.max(...xs)+1.0,
			y1:Math.min(...ys)-1.0,
			y2:Math.max(...ys)+1.0
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
		const xs=cluster.map((idx)=>points[idx].x);
		const ys=cluster.map((idx)=>points[idx].y);
		const pad=Math.max(1.5, eps*0.25);
		return {
			count:cluster.length,
			x1:Math.min(...xs)-pad,
			x2:Math.max(...xs)+pad,
			y1:Math.min(...ys)-pad,
			y2:Math.max(...ys)+pad,
			cx:(Math.min(...xs)+Math.max(...xs))/2,
			cy:(Math.min(...ys)+Math.max(...ys))/2
		};
	}).sort((a,b)=>b.count-a.count);

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

function resolveNearestScopeForRow(row, planScopes){
	if(!(typeof row?.x==="number" && typeof row?.y==="number")) return null;
	let best=null;
	for(const [key,scope] of Object.entries(planScopes||{})){
		const x1=Math.min(Number(scope?.x1||0), Number(scope?.x2||0));
		const x2=Math.max(Number(scope?.x1||0), Number(scope?.x2||0));
		const y1=Math.min(Number(scope?.y1||0), Number(scope?.y2||0));
		const y2=Math.max(Number(scope?.y1||0), Number(scope?.y2||0));
		if(!Number.isFinite(x1) || !Number.isFinite(x2) || !Number.isFinite(y1) || !Number.isFinite(y2)) continue;
		const dx=(row.x<x1) ? (x1-row.x) : ((row.x>x2) ? (row.x-x2) : 0);
		const dy=(row.y<y1) ? (y1-row.y) : ((row.y>y2) ? (row.y-y2) : 0);
		const penalty=(dx*dx)+(dy*dy);
		if(!best || penalty<best.penalty) best={ key, penalty };
	}
	return (best && best.penalty<=225) ? best.key : null;
}

const ROOM_PATTERNS=[
	["KITCHEN", /\bKITCHEN\b/i],
	["BATH", /\bFUR\s*BATH\b|\bBATH(?:ROOM)?\b|\bWC\b|\bTOILET\b/i],
	["LAUNDRY", /LAUNDRY|IRON/i],
	["PANTRY", /PANTRY/i],
	["STORE", /STORE/i],
	["DRESS", /DRESS(?:ING)?/i],
	["MAID", /MAID'?S?\s*ROOM/i],
	["BEDROOM", /MASTER\s*BED\s*ROOM|MASTER\s*BEDROOM|BED\s*ROOM|BEDROOM|GUEST'?S\s+ROOM|GUEST\s+ROOM/i],
	["MAJLIS", /MAJLES|MAJLIS/i],
	["BALCONY", /OUTDOOR\s+SITTING(?:\s+AREA)?|BALCONY|TERRACE/i],
	["SETTING", /SETTING|FAMILY\s+SITTING|LIVING|SALON/i],
	["ENTRANCE", /ENTRANCE/i],
	["LOBBY", /LOBBY|\bHALL\b/i],
	["CAR_PORCH", /\b(?:VEHICLES?\s+ENTRANCE|CAR\s+PORCH|CAR\s+PARKING)\b/i]
];

const ROOM_EXPECTED_AREAS={
	BATH:[3,16],
	KITCHEN:[8,30],
	LAUNDRY:[4,12],
	PANTRY:[2,10],
	STORE:[3,15],
	DRESS:[4,14],
	MAID:[7,18],
	BEDROOM:[10,35],
	MAJLIS:[35,85],
	SETTING:[20,60],
	ENTRANCE:[10,30],
	LOBBY:[15,70],
	BALCONY:[4,25],
	CAR_PORCH:[10,40]
};

const SPECIAL_SEGMENT_EXPECTED_AREAS={
	ENTRANCE:[18,26],
	STORE:[9,11],
	LAUNDRY:[6,8],
	KITCHEN:[18,22]
};

const ROOM_TEMPLATE_FALLBACKS={
	BATH:{ area:11, width:2.75, height:4.0 },
	KITCHEN:{ area:22, width:5.0, height:4.4 },
	LAUNDRY:{ area:8, width:3.2, height:2.5 },
	PANTRY:{ area:9, width:3.0, height:3.0 },
	STORE:{ area:10, width:4.0, height:2.5 },
	DRESS:{ area:8, width:2.5, height:3.2 },
	MAID:{ area:13, width:3.5, height:3.7 },
	BEDROOM:{ area:24, width:4.8, height:5.0 },
	MAJLIS:{ area:45, width:6.0, height:7.5 },
	SETTING:{ area:28, width:4.5, height:6.2 },
	ENTRANCE:{ area:16, width:4.0, height:4.0 },
	LOBBY:{ area:12, width:4.0, height:3.0 },
	BALCONY:{ area:15, width:3.0, height:5.0 },
	CAR_PORCH:{ area:30, width:5.0, height:6.0 }
};

const ROOM_SCOPE_RULES={
	KITCHEN:["GROUND","FIRST"],
	BATH:["GROUND","FIRST"],
	LAUNDRY:["GROUND","FIRST"],
	PANTRY:["GROUND","FIRST"],
	STORE:["GROUND","FIRST"],
	DRESS:["GROUND","FIRST"],
	MAID:["GROUND","FIRST"],
	BEDROOM:["GROUND","FIRST"],
	MAJLIS:["GROUND"],
	SETTING:["GROUND","FIRST"],
	ENTRANCE:["GROUND","FIRST"],
	LOBBY:["GROUND","FIRST"],
	BALCONY:["GROUND","FIRST","ROOF"],
	CAR_PORCH:["GROUND"]
};

function median(values){
	const nums=(values||[]).map(Number).filter(v=>Number.isFinite(v)).sort((a,b)=>a-b);
	if(!nums.length) return null;
	const mid=Math.floor(nums.length/2);
	return nums.length%2 ? nums[mid] : ((nums[mid-1]+nums[mid])/2);
}

function inScope(point, scope){
	return Boolean(scope) && point.x>=scope.x1 && point.x<=scope.x2 && point.y>=scope.y1 && point.y<=scope.y2;
}

function resolveScopeForRow(row, planScopes){
	const scoped=Object.entries(planScopes||{}).find(([,def])=>inScope(row, def))?.[0] || null;
	if(scoped) return scoped;
	const byLayer=scopeKeyFromPlanLayer(row.layer);
	if(byLayer) return byLayer;
	return resolveNearestScopeForRow(row, planScopes);
}

function isLikelyRoomLabelText(text){
	const clean=String(text||"").trim();
	if(!clean) return false;
	if(clean.length>48) return false;
	if(/\bNOTE\b|\bGENERAL\b|\bPLAN\b|\bSCALE\b|\bWINDOW\b|\bDOOR\b|\bDEVICE\b|\bINSULATION\b|\bGLAZ/i.test(clean)) return false;
	if(/[.:;]/.test(clean) && !/LAUNDRY\s*\/\s*IRON/i.test(clean)) return false;
	if(/\d{2,}/.test(clean)) return false;
	const words=clean.split(/\s+/).filter(Boolean);
	if(words.length>4) return false;
	return /[A-Za-z]/.test(clean);
}

function isLikelyBoundaryLayer(layer){
	const l=String(layer||"").toLowerCase();
	return isLikelyWallLayer(l) || /(^|[_$.\-])(hidden|stair)(?=$|[_$.\-])/.test(l);
}

function filterRoomLabelsByScopeBand(roomLabels){
	const sourceLabels=(roomLabels||[]);
	const grouped=(roomLabels||[]).reduce((acc,label)=>{
		(acc[label.scope] ||= []).push(label);
		return acc;
	}, {});
	const medians=Object.fromEntries(Object.entries(grouped).map(([scope, labels])=>[scope, median(labels.map(label=>label.y))]));
	const filtered=sourceLabels.filter((label)=>{
		const medianY=medians[label.scope];
		if(!Number.isFinite(medianY)) return true;
		const limit=label.key==="BALCONY" ? 20 : 16;
		return Math.abs(label.y-medianY)<=limit;
	});
	if(filtered.length>=Math.max(6, Math.ceil(sourceLabels.length*0.45))) return filtered;
	return sourceLabels;
}

function buildSpecialExteriorLabels(texts, planScopes){
	const ground=planScopes?.GROUND;
	if(!ground) return [];
	const cleanRows=(texts||[]).map((row)=>({ ...row, clean:stripCadMarkup(row.text) }));
	const entrance=cleanRows.find((row)=>
		/(MAIN\s+ENTRANCE|ENTRY|ENTRANCE|STAIR\s+AREA)/i.test(row.clean) &&
		String(row.clean||"").length<=28 &&
		!/(PROPOSED|VILLA|SERVICE\s+BLOCK|COMPOUND|PLOT|PROJECT)/i.test(String(row.clean||"")) &&
		typeof row.x==="number" &&
		typeof row.y==="number" &&
		row.y>=ground.y1 &&
		row.y<=ground.y2 &&
		row.x>=ground.x1-5 &&
		row.x<=ground.x2+10
	);
	const carPorch=cleanRows.find((row)=>
		/VEHICLES?\s+ENTRANCE|CAR\s+PORCH|CAR\s+PARKING/i.test(row.clean) &&
		String(row.clean||"").length<=28 &&
		!/(PROPOSED|VILLA|SERVICE\s+BLOCK|COMPOUND|PLOT|PROJECT)/i.test(String(row.clean||"")) &&
		typeof row.x==="number" &&
		typeof row.y==="number" &&
		row.y>=ground.y1 &&
		row.y<=ground.y2 &&
		row.x>=ground.x2-5 &&
		row.x<=ground.x2+30
	);
	const out=[];
	if(entrance){
		out.push({
			id:`ENTRANCE_${roundN(entrance.x,2)}_${roundN(entrance.y,2)}`,
			key:"ENTRANCE",
			scope:"GROUND",
			text:entrance.clean,
			x:entrance.x,
			y:entrance.y
		});
	}
	if(carPorch){
		out.push({
			id:`CAR_PORCH_${roundN(carPorch.x,2)}_${roundN(carPorch.y,2)}`,
			key:"CAR_PORCH",
			scope:"GROUND",
			text:carPorch.clean,
			x:carPorch.x,
			y:carPorch.y
		});
	}
	return out;
}

function parseMetricLevel(text){
	const match=String(text||"").match(/([+-]?\d+(?:\.\d+)?)\s*m\b/i);
	return match ? Number(match[1]) : null;
}

function deriveParapetHeightFromTexts(texts){
	const values=[...new Set((texts||[])
		.map(row=>parseMetricLevel(stripCadMarkup(row.text)))
		.filter(v=>Number.isFinite(v) && v>=0.5 && v<=20)
		.map(v=>roundN(v,2))
	)]
	.sort((a,b)=>b-a);

	const pickBest=(minDiff, maxDiff)=>{
		let best=null;
		for(let i=0;i<values.length;i++){
			for(let j=i+1;j<values.length;j++){
				const diff=values[i]-values[j];
				if(diff<minDiff || diff>maxDiff) continue;
				const candidate={
					top:values[i],
					base:values[j],
					diff,
					score:(values[i]*10) - Math.abs(diff-1.5)
				};
				if(!best || candidate.score>best.score) best=candidate;
			}
		}
		return best;
	};

	const best=pickBest(1.3,1.7) || pickBest(0.8,2.2);
	if(best) return best.diff;

	for(let i=0;i<values.length;i++){
		for(let j=i+1;j<values.length;j++){
			const diff=values[i]-values[j];
			if(diff>=0.8 && diff<=2.2) return diff;
		}
	}
	return 0;
}

function deriveRoofPlanMetrics(segments, planScopes){
	const roofScope=planScopes?.ROOF || planScopes?.TOP_ROOF || null;
	if(!roofScope) return { roof_edge_length_m:0, bbox_area_m2:0, estimated_area_m2:0 };
	const roofSegs=(segments||[]).filter((seg)=>{
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
	const xs=pts.map((pt)=>pt.x).filter((v)=>Number.isFinite(v));
	const ys=pts.map((pt)=>pt.y).filter((v)=>Number.isFinite(v));
	const bboxArea=(xs.length && ys.length) ? (Math.max(...xs)-Math.min(...xs)) * (Math.max(...ys)-Math.min(...ys)) : 0;
	return {
		roof_edge_length_m:roofEdgeLengthM,
		bbox_area_m2:bboxArea,
		estimated_area_m2:bboxArea>0 ? bboxArea*0.75 : 0
	};
}

function dedupeRoomLabels(roomLabels){
	const out=[];
	for(const label of roomLabels){
		const duplicate=out.find((existing)=>
			existing.scope===label.scope &&
			existing.key===label.key &&
			Math.abs(existing.x-label.x)<=3.5 &&
			Math.abs(existing.y-label.y)<=3.5
		);
		if(duplicate) continue;
		out.push(label);
	}
	return out;
}

function parseRoomAreaTextValue(text){
	const match=String(text||"").match(/(\d+(?:\.\d+)?)\s*m(?:2|²)/i);
	return match ? Number(match[1]) : null;
}

function buildRoomLabels(texts, planScopes){
	const labels=(texts||[])
		.map((row)=>({ ...row, clean:stripCadMarkup(row.text) }))
		.map((row)=>{
			if(!isLikelyRoomLabelText(row.clean)) return null;
			const hit=ROOM_PATTERNS.find(([,re])=>re.test(row.clean));
			if(!hit) return null;
			const scope=resolveScopeForRow(row, planScopes);
			if(!scope) return null;
			if(ROOM_SCOPE_RULES[hit[0]] && !ROOM_SCOPE_RULES[hit[0]].includes(scope)) return null;
			return {
				id:`${hit[0]}_${roundN(row.x,2)}_${roundN(row.y,2)}`,
				key:hit[0],
				scope,
				text:row.clean,
				x:row.x,
				y:row.y
			};
		})
	.filter(Boolean);
	return filterRoomLabelsByScopeBand(dedupeRoomLabels(labels.concat(buildSpecialExteriorLabels(texts, planScopes))));
}

function buildRoomAreaTextOverrides(texts, roomLabels){
	const areaRows=(texts||[])
		.map((row)=>({
			...row,
			clean:stripCadMarkup(row.text),
			area_m2:parseRoomAreaTextValue(stripCadMarkup(row.text))
		}))
		.filter((row)=>Number.isFinite(row.area_m2) && row.area_m2>1 && row.area_m2<250);

	const overrides={};
	for(const label of (roomLabels||[])){
		const [aMin,aMax]=ROOM_EXPECTED_AREAS[label.key] || [1,250];
		const best=areaRows
			.filter((row)=>{
				if(scopeKeyFromPlanLayer(row.layer)!==label.scope) return false;
				if(row.area_m2<aMin*0.6 || row.area_m2>aMax*1.6) return false;
				const dx=Math.abs((row.x||0)-label.x);
				const dy=label.y-(row.y||0);
				return dx<=1.2 && dy>=-0.2 && dy<=0.6;
			})
			.sort((a,b)=>{
				const da=Math.abs((a.x||0)-label.x)+Math.abs((a.y||0)-label.y)*1.5;
				const db=Math.abs((b.x||0)-label.x)+Math.abs((b.y||0)-label.y)*1.5;
				return da-db;
			})[0];
		if(!best) continue;
		overrides[label.id]={
			area:Number(best.area_m2||0),
			source:"ROOM_TEXT_AREA",
			text:best.clean
		};
	}
	return overrides;
}

function estimateRoomGeometries(roomLabels, wallPairs){
	const vLines=wallPairs
		.filter(pair=>pair.ori==="V")
		.map(pair=>({
			x:(pair.s1.c+pair.s2.c)/2,
			y1:Math.max(pair.s1.a,pair.s2.a),
			y2:Math.min(pair.s1.b,pair.s2.b)
		}))
		.filter(line=>line.y2>line.y1);
	const hLines=wallPairs
		.filter(pair=>pair.ori==="H")
		.map(pair=>({
			y:(pair.s1.c+pair.s2.c)/2,
			x1:Math.max(pair.s1.a,pair.s2.a),
			x2:Math.min(pair.s1.b,pair.s2.b)
		}))
		.filter(line=>line.x2>line.x1);

	const out=[];
	for(const label of (roomLabels||[])){
		const isReceptionLike=["MAJLIS","SETTING","LOBBY","ENTRANCE"].includes(label.key);
		const tol=isReceptionLike ? 5 : 2;
		const lefts=vLines.filter(line=>label.y>=line.y1-tol && label.y<=line.y2+tol && line.x<label.x).sort((a,b)=>b.x-a.x).slice(0,16);
		const rights=vLines.filter(line=>label.y>=line.y1-tol && label.y<=line.y2+tol && line.x>label.x).sort((a,b)=>a.x-b.x).slice(0,16);
		const bottoms=hLines.filter(line=>label.x>=line.x1-tol && label.x<=line.x2+tol && line.y<label.y).sort((a,b)=>b.y-a.y).slice(0,16);
		const tops=hLines.filter(line=>label.x>=line.x1-tol && label.x<=line.x2+tol && line.y>label.y).sort((a,b)=>a.y-b.y).slice(0,16);

		let best=null;
		for(const l of lefts){
			for(const r of rights){
				const width=r.x-l.x;
				if(width<1 || width>(isReceptionLike?30:20)) continue;
				for(const b of bottoms){
					for(const t of tops){
						const height=t.y-b.y;
						if(height<1 || height>(isReceptionLike?30:20)) continue;
						const area=width*height;
						if(area<2 || area>(isReceptionLike?250:120)) continue;
						const [aMin,aMax]=ROOM_EXPECTED_AREAS[label.key] || [3,50];
						const nx=(label.x-l.x)/width;
						const ny=(label.y-b.y)/height;
						let score=0;
						if(area>=aMin && area<=aMax) score+=100;
						score -= Math.abs(area-((aMin+aMax)/2))*1.5;
						score -= (Math.abs(nx-0.5)+Math.abs(ny-0.5))*8;
						if(nx<0.01 || nx>0.99 || ny<0.01 || ny>0.99) score-=50;
						if(!best || score>best.score){
							best={ width, height, area, score };
						}
					}
				}
			}
		}
		if(best) out.push({ ...label, ...best });
	}
	return out;
}

function estimateAreaOverridesFromBoundarySegments(roomLabels, boundarySegments, allowedKeys=null){
	const vLines=(boundarySegments||[])
		.filter(seg=>seg.ori==="V")
		.map(seg=>({ x:seg.c, y1:seg.a, y2:seg.b, layer:seg.layer||"" }))
		.filter(line=>line.y2>line.y1);
	const hLines=(boundarySegments||[])
		.filter(seg=>seg.ori==="H")
		.map(seg=>({ y:seg.c, x1:seg.a, x2:seg.b, layer:seg.layer||"" }))
		.filter(line=>line.x2>line.x1);

	const overrides={};
	for(const label of (roomLabels||[])){
		if(label.scope!=="GROUND") continue;
		if(allowedKeys && !allowedKeys.includes(label.key)) continue;
		if(!allowedKeys && !["ENTRANCE","SETTING"].includes(label.key)) continue;
		const [aMin,aMax]=SPECIAL_SEGMENT_EXPECTED_AREAS[label.key] || ROOM_EXPECTED_AREAS[label.key] || [3,50];
		const areaMid=(aMin+aMax)/2;
		const isEntrance=label.key==="ENTRANCE";
		const isUtility=["STORE","LAUNDRY","KITCHEN"].includes(label.key);
		const tol=isEntrance ? 10 : 8;
		const extentMax=isEntrance ? 30 : 25;
		const lefts=vLines.filter(line=>label.x>=line.x-tol && label.x>line.x && (label.x-line.x)<=extentMax && label.y>=line.y1-tol && label.y<=line.y2+tol).sort((a,b)=>b.x-a.x).slice(0,40);
		const rights=vLines.filter(line=>label.x<=line.x+tol && line.x>label.x && (line.x-label.x)<=extentMax && label.y>=line.y1-tol && label.y<=line.y2+tol).sort((a,b)=>a.x-b.x).slice(0,40);
		const bottoms=hLines.filter(line=>label.y>=line.y-tol && label.y>line.y && (label.y-line.y)<=extentMax && label.x>=line.x1-tol && label.x<=line.x2+tol).sort((a,b)=>b.y-a.y).slice(0,40);
		const tops=hLines.filter(line=>label.y<=line.y+tol && line.y>label.y && (line.y-label.y)<=extentMax && label.x>=line.x1-tol && label.x<=line.x2+tol).sort((a,b)=>a.y-b.y).slice(0,40);

		let best=null;
		for(const l of lefts){
			for(const r of rights){
				const width=r.x-l.x;
				if(width<1 || width>extentMax) continue;
				for(const b of bottoms){
					for(const t of tops){
						const height=t.y-b.y;
						if(height<1 || height>extentMax) continue;
						const area=width*height;
						if(area<2 || area>(isEntrance?120:250)) continue;
						const nx=(label.x-l.x)/width;
						const ny=(label.y-b.y)/height;
						if(nx<0 || nx>1 || ny<0 || ny>1) continue;
						let score=0;
						if(area>=aMin && area<=aMax) score+=100;
						score -= Math.abs(area-areaMid)*2.5;
						if(isEntrance){
							score -= (Math.abs(nx-0.42)+Math.abs(ny-0.7))*3;
							if(/stair/i.test(b.layer) || /stair/i.test(t.layer)) score+=8;
							if(width>=5 && width<=8) score+=4;
							if(height>=3 && height<=5) score+=4;
						}else if(isUtility){
							score -= (Math.abs(nx-0.5)+Math.abs(ny-0.5))*5;
							if(/wall/i.test(l.layer) && /wall/i.test(r.layer)) score+=6;
							if(/wall|hidden/i.test(b.layer) && /wall|hidden/i.test(t.layer)) score+=4;
						}else{
							score -= (Math.abs(nx-0.5)+Math.abs(ny-0.5))*8;
							if(width>=4.5 && width<=7.5) score+=4;
							if(height>=5 && height<=8.5) score+=4;
						}
						if(!best || score>best.score){
							best={
								area,
								width,
								height,
								source:"SPECIAL_BOUNDARY_AREA",
								score,
								left_m:l.x,
								right_m:r.x,
								bottom_m:b.y,
								top_m:t.y,
								boundary_layers:[l.layer, r.layer, b.layer, t.layer].join("|")
							};
						}
					}
				}
			}
		}
		if(best) overrides[label.id]=best;
	}
	return overrides;
}

function buildRoomLibrary(roomLabels, resolvedRooms){
	const representativeByKey={};
	const resolvedById=Object.fromEntries((resolvedRooms||[]).map(room=>[room.id, room]));

	for(const key of Object.keys(ROOM_TEMPLATE_FALLBACKS)){
		const matches=(resolvedRooms||[]).filter(room=>room.key===key);
		if(matches.length){
			representativeByKey[key]={
				area:median(matches.map(room=>room.area)),
				width:median(matches.map(room=>room.width)),
				height:median(matches.map(room=>room.height)),
				source:"ROOM_GEOMETRY"
			};
		}else{
			representativeByKey[key]={
				...ROOM_TEMPLATE_FALLBACKS[key],
				source:"ROOM_TEMPLATE_FALLBACK"
			};
		}
	}

	const countsByScopeAndKey={};
	for(const label of (roomLabels||[])){
		const id=`${label.scope}:${label.key}`;
		countsByScopeAndKey[id]=(countsByScopeAndKey[id]||0)+1;
	}

	return { representativeByKey, resolvedById, countsByScopeAndKey };
}

function sumRoomMetric(roomLabels, roomLibrary, keys, scopes, metric, areaOverridesById=null){
	let total=0;
	const used=[];
	for(const label of (roomLabels||[])){
		if(keys && !keys.includes(label.key)) continue;
		if(scopes && !scopes.includes(label.scope)) continue;
		const resolved=roomLibrary.resolvedById[label.id];
		const representative=roomLibrary.representativeByKey[label.key]||null;
		const areaOverride=(metric==="area" && areaOverridesById) ? areaOverridesById[label.id] : null;
		const source=areaOverride?.source || (resolved ? "ROOM_GEOMETRY" : representative?.source || "MISSING");
		const value=areaOverride ? Number(areaOverride.area||0) : (resolved ? Number(resolved[metric]||0) : Number(representative?.[metric]||0));
		if(!(value>0)) continue;
		total += value;
		used.push({ label:label.text, scope:label.scope, key:label.key, metric, value, source });
	}
	return { total, used };
}

function maxSingleRoomMetric(roomLabels, roomLibrary, keys, scopes, metric, areaOverridesById=null){
	let best=null;
	for(const label of (roomLabels||[])){
		if(keys && !keys.includes(label.key)) continue;
		if(scopes && !scopes.includes(label.scope)) continue;
		const resolved=roomLibrary.resolvedById[label.id];
		const representative=roomLibrary.representativeByKey[label.key]||null;
		const areaOverride=(metric==="area" && areaOverridesById) ? areaOverridesById[label.id] : null;
		const source=areaOverride?.source || (resolved ? "ROOM_GEOMETRY" : representative?.source || "MISSING");
		const value=areaOverride ? Number(areaOverride.area||0) : (resolved ? Number(resolved[metric]||0) : Number(representative?.[metric]||0));
		if(!(value>0)) continue;
		if(!best || value>best.value){
			best={ label:label.text, scope:label.scope, key:label.key, metric, value, source };
		}
	}
	return {
		total:best ? Number(best.value||0) : 0,
		used:best ? [best] : []
	};
}

function clusterGroundDryAreaForSpray(roomLabels, roomLibrary, areaOverridesById=null){
	const targetKeys=["ENTRANCE","MAJLIS","SETTING","BEDROOM","DRESS","STORE","LOBBY"];
	const rows=(roomLabels||[])
		.filter((label)=>label.scope==="GROUND" && targetKeys.includes(label.key))
		.map((label)=>{
			const resolved=roomLibrary.resolvedById[label.id];
			const representative=roomLibrary.representativeByKey[label.key]||null;
			const areaOverride=areaOverridesById ? areaOverridesById[label.id] : null;
			const area=areaOverride ? Number(areaOverride.area||0) : (resolved ? Number(resolved.area||0) : Number(representative?.area||0));
			return {
				label:label.text,
				scope:label.scope,
				key:label.key,
				x:Number(label.x)||0,
				y:Number(label.y)||0,
				metric:"area",
				value:area,
				source:areaOverride?.source || (resolved ? "ROOM_GEOMETRY" : representative?.source || "MISSING")
			};
		})
		.filter((row)=>row.value>0)
		.sort((a,b)=>a.x-b.x);
	if(!rows.length) return { total:0, used:[] };

	const clusters=[];
	for(const row of rows){
		const current=clusters[clusters.length-1];
		if(!current || Math.abs(row.x-current.maxX)>18){
			clusters.push({ items:[row], minX:row.x, maxX:row.x });
			continue;
		}
		current.items.push(row);
		current.maxX=row.x;
	}

	const scored=clusters.map((cluster)=>{
		const nonLobbyTotal=cluster.items
			.filter((row)=>row.key!=="LOBBY")
			.reduce((sum,row)=>sum+row.value,0);
		const lobbyItems=cluster.items.filter((row)=>row.key==="LOBBY").sort((a,b)=>a.y-b.y).slice(0,3);
		const used=cluster.items.filter((row)=>row.key!=="LOBBY").concat(lobbyItems);
		const total=used.reduce((sum,row)=>sum+row.value,0);
		return { cluster, nonLobbyTotal, total, used };
	}).sort((a,b)=>b.nonLobbyTotal-a.nonLobbyTotal || b.total-a.total);

	const best=scored[0];
	return {
		total:best ? best.total : 0,
		used:(best?.used||[]).map((row)=>({ ...row, source:"GROUND_DRY_CLUSTER_FALLBACK" }))
	};
}

function buildFinishModel({ roomLabels, roomLibrary, archSummary, inputs, rules, areaOverridesById, ceilingAreaOverridesById, roomAreaOverridesById, roofPlanMetrics, archDoorSchedule }){
	const interiorScopes=["GROUND","FIRST"];
	const balconyScopes=["FIRST","ROOF"];
	const balconySoffitScopes=["GROUND","FIRST","ROOF"];
	const excludedInteriorFloorKeys=["BALCONY","CAR_PORCH"];
	const wetRoomKeys=(rules?.wet_dry?.wet_rooms||[])
		.map(v=>String(v||"").toLowerCase())
		.flatMap((name)=>{
			if(/bath|wash|wc|toilet/.test(name)) return ["BATH"];
			if(/kitchen/.test(name)) return ["KITCHEN"];
			if(/pantry/.test(name)) return ["PANTRY"];
			if(/laundry|iron/.test(name)) return ["LAUNDRY"];
			return [];
		});
	const wetTileHeightM=Math.min(3.0, Math.max(0, Number(inputs?.levels?.g_floor_to_floor_m||4) - 0.5));

	const entranceFloors=sumRoomMetric(roomLabels, roomLibrary, ["ENTRANCE"], ["GROUND"], "area", roomAreaOverridesById);
	const receptionFloors=sumRoomMetric(roomLabels, roomLibrary, ["MAJLIS","SETTING","LOBBY"], ["GROUND"], "area", roomAreaOverridesById);
	const bedroomAreas=sumRoomMetric(roomLabels, roomLibrary, ["BEDROOM","DRESS"], interiorScopes, "area", roomAreaOverridesById);
	const balconyFloorAreas=sumRoomMetric(roomLabels, roomLibrary, ["BALCONY"], balconyScopes, "area", roomAreaOverridesById);
	const bedroomFloors={
		total:bedroomAreas.total,
		used:[...bedroomAreas.used]
	};
	const kitchenFloors=sumRoomMetric(roomLabels, roomLibrary, ["KITCHEN","PANTRY"], ["GROUND","FIRST"], "area", roomAreaOverridesById);
	const bathFloors=sumRoomMetric(roomLabels, roomLibrary, ["BATH"], interiorScopes, "area", roomAreaOverridesById);
	const maidFloors=sumRoomMetric(roomLabels, roomLibrary, ["MAID"], interiorScopes, "area", roomAreaOverridesById);
	const storeFloors=sumRoomMetric(roomLabels, roomLibrary, ["STORE"], interiorScopes, "area", roomAreaOverridesById);
	const serviceFloors=sumRoomMetric(roomLabels, roomLibrary, ["LAUNDRY"], interiorScopes, "area", roomAreaOverridesById);
	const allInteriorFloors=(roomLabels||[]).reduce((acc,label)=>{
		if(!interiorScopes.includes(label.scope)) return acc;
		if(excludedInteriorFloorKeys.includes(label.key)) return acc;
		const resolved=roomLibrary.resolvedById[label.id];
		const representative=roomLibrary.representativeByKey[label.key]||null;
		const areaOverride=roomAreaOverridesById?.[label.id] || null;
		const value=areaOverride ? Number(areaOverride.area||0) : (resolved ? Number(resolved.area||0) : Number(representative?.area||0));
		if(!(value>0)) return acc;
		acc.total += value;
		acc.used.push({ label:label.text, scope:label.scope, key:label.key, metric:"area", value, source:areaOverride?.source || (resolved ? "ROOM_GEOMETRY" : representative?.source || "MISSING") });
		return acc;
	}, { total:0, used:[] });
	const kitchenWetCeilings=sumRoomMetric(roomLabels, roomLibrary, ["KITCHEN"], interiorScopes, "area", ceilingAreaOverridesById);
	const bathWetCeilings=sumRoomMetric(roomLabels, roomLibrary, ["BATH","PANTRY","LAUNDRY"], interiorScopes, "area", roomAreaOverridesById);
	const allWetFloors=sumRoomMetric(roomLabels, roomLibrary, ["BATH","KITCHEN","PANTRY","LAUNDRY"], interiorScopes, "area", roomAreaOverridesById);
	const allBalconyFloors=sumRoomMetric(roomLabels, roomLibrary, ["BALCONY"], balconyScopes, "area", roomAreaOverridesById);
	const dryAreaFlooring={
		total:Math.max(0, allInteriorFloors.total-allWetFloors.total),
		used:[...allInteriorFloors.used, { source:"TOTAL_INTERIOR_FLOOR_AREA_MINUS_WET_AREAS", total_interior_m2:roundN(allInteriorFloors.total,3), wet_areas_m2:roundN(allWetFloors.total,3), net_dry_m2:roundN(Math.max(0, allInteriorFloors.total-allWetFloors.total),3) }]
	};
	const wetAreasBalconyWaterproof={
		total:allWetFloors.total + allBalconyFloors.total,
		used:[...allWetFloors.used, ...allBalconyFloors.used]
	};

	const doorSchedule=Array.isArray(archDoorSchedule) ? archDoorSchedule : [];
	const totalDoorWidthLm=doorSchedule.reduce((sum,d)=>sum + (Number(d.width_m)||0) * (Number(d.count)||0), 0);
	const totalDoorCount=doorSchedule.reduce((sum,d)=>sum + (Number(d.count)||0), 0);

	const dryRoomKeys=["ENTRANCE","MAJLIS","SETTING","BEDROOM","DRESS","STORE","LOBBY"];
	let grossSkirtingPerimeter=0;
	const skirtingEvidence=[];
	for(const label of (roomLabels||[])){
		if(!interiorScopes.includes(label.scope)) continue;
		if(!dryRoomKeys.includes(label.key)) continue;
		const resolved=roomLibrary.resolvedById[label.id];
		const representative=roomLibrary.representativeByKey[label.key]||null;
		const width=resolved ? Number(resolved.width||0) : Number(representative?.width||0);
		const height=resolved ? Number(resolved.height||0) : Number(representative?.height||0);
		if(!(width>0) || !(height>0)) continue;
		const perimeter=2*(width+height);
		grossSkirtingPerimeter += perimeter;
		skirtingEvidence.push({ label:label.text, scope:label.scope, key:label.key, perimeter_m:roundN(perimeter,3), source:resolved?"ROOM_GEOMETRY":representative?.source||"MISSING" });
	}
	const skirtingDoorDeductionLm=totalDoorWidthLm*0.4;
	const skirtingTotal=Math.max(0, grossSkirtingPerimeter - skirtingDoorDeductionLm);
	const skirtingDoorSource=doorSchedule.length ? "ARCH_OPENING_SCHEDULE" : "NO_DOOR_DATA";
	skirtingEvidence.push({ deduction:"ARCH_DOOR_SCHEDULE_40_PERCENT", total_door_width_lm:roundN(totalDoorWidthLm,3), deducted_width_lm:roundN(skirtingDoorDeductionLm,3), door_count:totalDoorCount, net_skirting_lm:roundN(skirtingTotal,3), source:skirtingDoorSource });

	const marbleThresholdTotal=totalDoorWidthLm;
	const marbleThresholdEvidence=doorSchedule.length
		? doorSchedule.map((d)=>({ tag:d.tag, width_m:roundN(Number(d.width_m)||0,3), count:Number(d.count)||0, subtotal_lm:roundN((Number(d.width_m)||0)*(Number(d.count)||0),3), source:"ARCH_OPENING_SCHEDULE" }))
		: [{ source:"NO_DOOR_SCHEDULE", total_lm:0 }];
	const bedroomCeilings={ total:bedroomAreas.total, used:[...bedroomAreas.used] };
	const receptionCeilings=sumRoomMetric(roomLabels, roomLibrary, ["MAJLIS","SETTING","LOBBY","ENTRANCE"], ["GROUND"], "area", roomAreaOverridesById);
	const balconySoffits=sumRoomMetric(roomLabels, roomLibrary, ["BALCONY"], balconySoffitScopes, "area", roomAreaOverridesById);
	const porchSoffits=sumRoomMetric(roomLabels, roomLibrary, ["CAR_PORCH"], ["GROUND"], "area", roomAreaOverridesById);
	const drySprayCeilingFallback=sumRoomMetric(
		roomLabels,
		roomLibrary,
		["ENTRANCE","MAJLIS","SETTING","BEDROOM","DRESS","STORE"],
		["GROUND"],
		"area",
		roomAreaOverridesById
	);
	const clusteredDrySprayCeilingFallback=clusterGroundDryAreaForSpray(roomLabels, roomLibrary, roomAreaOverridesById);
	const sprayCeilings={
		total:dryAreaFlooring.total,
		used:[...dryAreaFlooring.used].map((row)=>({ ...row, source:row.source || "DRY_AREA_FLOORING_EQUALS_DRY_AREA_CEILING" }))
	};

	let wetWallTilesTotal=0;
	const wetWallEvidence=[];
	for(const label of (roomLabels||[])){
		if(!interiorScopes.includes(label.scope)) continue;
		if(!wetRoomKeys.includes(label.key)) continue;
		const resolved=roomLibrary.resolvedById[label.id];
		const representative=roomLibrary.representativeByKey[label.key]||null;
		const width=resolved ? Number(resolved.width||0) : Number(representative?.width||0);
		const height=resolved ? Number(resolved.height||0) : Number(representative?.height||0);
		if(!(width>0) || !(height>0) || !(wetTileHeightM>0)) continue;
		const perimeter=2*(width+height);
		const area=perimeter*wetTileHeightM;
		wetWallTilesTotal += area;
		wetWallEvidence.push({
			label:label.text,
			scope:label.scope,
			key:label.key,
			width_m:roundN(width,3),
			length_m:roundN(height,3),
			perimeter_m:roundN(perimeter,3),
			wet_tile_height_m:roundN(wetTileHeightM,3),
			area_m2:roundN(area,3),
			source:resolved ? "ROOM_GEOMETRY" : representative?.source || "MISSING"
		});
	}

	const wetDoorCount=doorSchedule.filter(d => /^D/i.test(d.tag)).reduce((s,d) => s + (Number(d.count)||0), 0);
	wetWallTilesTotal=Math.max(0, wetWallTilesTotal - (wetDoorCount * 2.0));

	const floorToFloorM=Math.max(0, Number(inputs?.levels?.g_floor_to_floor_m||0) || 4.0);
	const clearWallHeightM=floorToFloorM>4.15
		? Math.max(3.5, floorToFloorM-0.75)
		: Math.max(3.2, floorToFloorM-0.45);
	const dryPaintKeys=["BEDROOM","DRESS","MAID","STORE","LOBBY","MAJLIS","SETTING"];
	let internalPaintGross=0;
	const internalPaintEvidence=[];
	for(const label of (roomLabels||[])){
		if(!interiorScopes.includes(label.scope)) continue;
		if(!dryPaintKeys.includes(label.key)) continue;
		const resolved=roomLibrary.resolvedById[label.id];
		const representative=roomLibrary.representativeByKey[label.key]||null;
		const width=resolved ? Number(resolved.width||0) : Number(representative?.width||0);
		const height=resolved ? Number(resolved.height||0) : Number(representative?.height||0);
		if(!(width>0) || !(height>0) || !(clearWallHeightM>0)) continue;
		const perimeter=2*(width+height);
		const area=perimeter*clearWallHeightM;
		internalPaintGross += area;
		internalPaintEvidence.push({
			label:label.text,
			scope:label.scope,
			key:label.key,
			width_m:roundN(width,3),
			length_m:roundN(height,3),
			perimeter_m:roundN(perimeter,3),
			paint_height_m:roundN(clearWallHeightM,3),
			area_m2:roundN(area,3),
			source:resolved ? "ROOM_GEOMETRY" : representative?.source || "MISSING"
		});
	}
	const internalPaintNet=Math.max(0, skirtingTotal * clearWallHeightM);
	internalPaintEvidence.push({ source:"SKIRTING_X_FLOOR_HEIGHT", skirting_lm:roundN(skirtingTotal,3), floor_height_m:roundN(clearWallHeightM,3), net_m2:roundN(internalPaintNet,3) });
	const internalPlasterNet=Math.max(
		Number(archSummary.internal_plaster_net_m2||0),
		internalPaintNet + wetWallTilesTotal
	);

	const parapetCopingM=Math.max(0, Number(rules?.parapet?.if_blockwork_coping_beam_last_cm||0) / 100);
	const parapetHeightM=Math.max(0, Number(archSummary.parapet_finish_height_m||0));
	const externalWallLengthM=Math.max(0, Number(archSummary.external_wall_length_m||0));
	const parapetFinishArea=Math.max(0, externalWallLengthM * parapetHeightM);
	const copingBeamFinishArea=Math.max(0, externalWallLengthM * parapetCopingM);
	const effectiveParapetHeightM=Math.max(0, parapetHeightM + parapetCopingM);
	const roofEdgeFasciaArea=Math.max(0, Number(roofPlanMetrics?.roof_edge_length_m||0) * 0.25);
	const externalParapetArea=Math.max(0, parapetFinishArea + copingBeamFinishArea + roofEdgeFasciaArea);
	const externalPaintNet=Math.max(0, Number(archSummary.external_paint_net_m2||0));
	const externalPlasterNet=Math.max(0, Number(archSummary.external_plaster_net_m2||0));
	const roofSlabAreaM2=Number(inputs?.structure?.roof_slab_area_m2||0);
	const topRoofSlabAreaM2=Number(inputs?.structure?.top_roof_slab_area_m2||0);
	const roofScopeEstimatedAreaM2=Number(roofPlanMetrics?.estimated_area_m2||0);
	const comboRoofFallbackM2=(roofSlabAreaM2>0 && topRoofSlabAreaM2<=0 && roofScopeEstimatedAreaM2<=0)
		? roofSlabAreaM2*2
		: 0;
	const roofWaterproofArea=Math.max(
		0,
		roofSlabAreaM2 + topRoofSlabAreaM2,
		roofScopeEstimatedAreaM2,
		comboRoofFallbackM2
	);

	return {
		room_labels_count:roomLabels.length,
		room_labels:roomLabels,
		room_templates:roomLibrary.representativeByKey,
		room_geometries:(roomLabels||[]).map((label)=>{
			const resolved=roomLibrary.resolvedById[label.id];
			const representative=roomLibrary.representativeByKey[label.key]||null;
			const areaOverride=roomAreaOverridesById?.[label.id] || areaOverridesById?.[label.id] || null;
			return {
				scope:label.scope,
				key:label.key,
				label:label.text,
				width_m:roundN(areaOverride?.width ?? resolved?.width ?? representative?.width ?? 0, 3),
				height_m:roundN(areaOverride?.height ?? resolved?.height ?? representative?.height ?? 0, 3),
				area_m2:roundN(areaOverride?.area ?? resolved?.area ?? representative?.area ?? 0, 3),
				source:areaOverride?.source || (resolved ? "ROOM_GEOMETRY" : representative?.source || "MISSING")
			};
		}),
		floor_quantities:{
			FLOOR_GRANITE_ENTRANCE_M2:entranceFloors,
			FLOOR_MARBLE_RECEPTION_M2:receptionFloors,
			FLOOR_CERAMIC_BED_BALCONY_M2:bedroomFloors,
			FLOOR_CERAMIC_KITCHEN_M2:kitchenFloors,
			FLOOR_CERAMIC_BATHROOM_M2:bathFloors,
			FLOOR_CERAMIC_MAID_M2:maidFloors,
			FLOOR_CERAMIC_STORE_M2:storeFloors,
			FLOOR_CERAMIC_SERVICE_M2:serviceFloors,
			DRY_AREA_FLOORING_M2:dryAreaFlooring,
			WET_AREA_FLOORING_M2:allWetFloors,
			BALCONY_FLOORING_M2:balconyFloorAreas
		},
		wall_quantities:{
			WALL_TILES_WET_AREAS_M2:{ total:wetWallTilesTotal, used:wetWallEvidence },
			PLASTER_INTERNAL_M2:{ total:internalPlasterNet, used:[...internalPaintEvidence, ...wetWallEvidence] },
			PLASTER_EXTERNAL_M2:{ total:externalPlasterNet, used:[{ source:"ARCH_EXTERNAL_PLUS_PARAPET_AND_ROOF_EDGE", parapet_height_m:roundN(effectiveParapetHeightM,3), roof_edge_length_m:roundN(Number(roofPlanMetrics?.roof_edge_length_m||0),3), roof_edge_fascia_area_m2:roundN(roofEdgeFasciaArea,3), added_area_m2:roundN(externalParapetArea,3) }] },
			PAINT_INTERNAL_M2:{ total:internalPaintNet, used:internalPaintEvidence },
			PAINT_EXTERNAL_M2:{ total:externalPaintNet, used:[{ source:"ARCH_EXTERNAL_PLUS_PARAPET_AND_ROOF_EDGE", parapet_height_m:roundN(effectiveParapetHeightM,3), roof_edge_length_m:roundN(Number(roofPlanMetrics?.roof_edge_length_m||0),3), roof_edge_fascia_area_m2:roundN(roofEdgeFasciaArea,3), added_area_m2:roundN(externalParapetArea,3) }] },
			PARAPET_FINISH_M2:{ total:parapetFinishArea, used:[{ source:"PARAPET_HEIGHT_X_EXTERNAL_LENGTH", external_wall_length_m:roundN(externalWallLengthM,3), parapet_height_m:roundN(parapetHeightM,3), area_m2:roundN(parapetFinishArea,3) }] },
			COPING_BEAM_FINISH_M2:{ total:copingBeamFinishArea, used:[{ source:"COPING_BEAM_LAST_CM_X_EXTERNAL_LENGTH", external_wall_length_m:roundN(externalWallLengthM,3), coping_beam_height_m:roundN(parapetCopingM,3), area_m2:roundN(copingBeamFinishArea,3) }] },
			ROOF_WATERPROOF_M2:{ total:roofWaterproofArea, used:[{ source:"STRUCTURAL_OR_ROOF_SCOPE_FALLBACK", roof_slab_area_m2:roundN(roofSlabAreaM2,3), top_roof_slab_area_m2:roundN(topRoofSlabAreaM2,3), roof_scope_bbox_area_m2:roundN(Number(roofPlanMetrics?.bbox_area_m2||0),3), roof_scope_estimated_area_m2:roundN(roofScopeEstimatedAreaM2,3), combo_roof_fallback_m2:roundN(comboRoofFallbackM2,3) }] },
			WET_AREAS_BALCONY_WATERPROOF_M2:wetAreasBalconyWaterproof
		},
		ceiling_quantities:{
			CEILING_SPRAY_PLASTER_M2:sprayCeilings,
			CEILING_GYPSUM_RECEPTION_M2:receptionCeilings,
			CEILING_GYPSUM_BEDROOM_M2:bedroomCeilings,
			CEILING_GYPSUM_VINYL_KITCHEN_M2:kitchenWetCeilings,
			CEILING_GYPSUM_VINYL_BATHROOM_M2:bathWetCeilings
		},
		skirting_quantities:{
			SKIRTING_LM:{ total:skirtingTotal, used:skirtingEvidence }
		},
		threshold_quantities:{
			MARBLE_THRESHOLD_LM:{ total:marbleThresholdTotal, used:marbleThresholdEvidence }
		}
	};
}

function normalizeUnitToken(unit){
	return String(unit||"")
		.toLowerCase()
		.replace(/ÃƒÂ¢/g,"")
		.replace(/Ã‚Â²/g,"2")
		.replace(/Ã‚Â³/g,"3")
		.replace(/\s+/g,"");
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
	return score;
}

function extractAllBoqItems(workbook){
	if(!workbook) return [];
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
				if(typeof row[c]==="string"){
					const unitNorm=normalizeUnitToken(row[c]);
					if(/^(m3|m2|m|ls|l\.s)$/.test(unitNorm)){
						if(unitCol<0 || c>unitCol) unitCol=c;
					}
				}
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

			const textCells=row.map(v=>String(v||"").trim()).filter(v=>v && /[A-Za-z\u0600-\u06FF]/.test(v));
			const description=textCells.sort((a,b)=>b.length-a.length)[0] || "";
			if(description.length<4) continue;

			const itemNoRaw=(()=>{
				for(let c=0;c<Math.min(row.length,4);c++){
					const v=row[c];
					if(typeof v==="number" && v>0 && v<1000) return v;
				}
				return "";
			})();
			const unitCell=unitCol>=0 ? row[unitCol] : "";
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

function findBoqProof(workbook, keywords){
	if(!workbook) return { sheet:null, row:null, qty_cell:null, qty_value:null, row_text:null };
	for(const sheet of (workbook.SheetNames||[])){
		const ws=workbook.Sheets[sheet];
		const rows=XLSX.utils.sheet_to_json(ws,{ header:1, blankrows:false, defval:"" });
		for(let r=0;r<rows.length;r++){
			const row=rows[r]||[];
			const text=row.map(v=>String(v||"")).join(" ").toLowerCase();
			if(keywords.some(keyword=>text.includes(keyword))) return { sheet, row:r+1, qty_cell:null, qty_value:null, row_text:text };
		}
	}
	return { sheet:null, row:null, qty_cell:null, qty_value:null, row_text:null };
}

function buildFinishTenderProof(workbook){
	return {
		FLOOR_GRANITE_ENTRANCE_M2:findBoqProof(workbook, ["granite floors for main enterance", "granite for entrance stair landing"]),
		FLOOR_MARBLE_RECEPTION_M2:findBoqProof(workbook, ["marble floors for hall", "majles", "setting area", "flooring for hall, majlis"]),
		FLOOR_CERAMIC_BED_BALCONY_M2:findBoqProof(workbook, ["ceramic floors for bedroom and balacony", "ceramic floors for bedroom and balcony", "flooring for bedrooms, dresses"]),
		FLOOR_CERAMIC_KITCHEN_M2:findBoqProof(workbook, ["ceramic floors kitchens"]),
		FLOOR_CERAMIC_BATHROOM_M2:findBoqProof(workbook, ["ceramic floors ,bathrooms", "ceramic floors bathrooms"]),
		FLOOR_CERAMIC_MAID_M2:findBoqProof(workbook, ["ceramic floors maid bed room"]),
		FLOOR_CERAMIC_STORE_M2:findBoqProof(workbook, ["ceramic floors store"]),
		FLOOR_CERAMIC_SERVICE_M2:findBoqProof(workbook, ["ceramic for service room", "laundry"]),
		WALL_TILES_WET_AREAS_M2:findBoqProof(workbook, ["ceramic tiles kitchens", "walls for baths", "walls for kitchen, baths"]),
		PLASTER_INTERNAL_M2:findBoqProof(workbook, ["plaster for internal walls"]),
		PLASTER_EXTERNAL_M2:findBoqProof(workbook, ["plaster for elevations"]),
		PAINT_INTERNAL_M2:findBoqProof(workbook, ["emulsion national paint", "internall walls"]),
		PAINT_EXTERNAL_M2:findBoqProof(workbook, ["epoxy paint"]),
		ROOF_WATERPROOF_M2:findBoqProof(workbook, ["insulation system for roof"]),
		WET_AREAS_BALCONY_WATERPROOF_M2:findBoqProof(workbook, ["water proofing for wet areas & balacony", "water proofing for wet areas & balcony"]),
		CEILING_SPRAY_PLASTER_M2:findBoqProof(workbook, ["spray plaster for ceilings"]),
		CEILING_GYPSUM_RECEPTION_M2:findBoqProof(workbook, ["gypsum decoration for entrance hall", "majles", "setting area"]),
		CEILING_GYPSUM_BEDROOM_M2:findBoqProof(workbook, ["gypsum decoration for bed room"]),
		CEILING_GYPSUM_VINYL_KITCHEN_M2:findBoqProof(workbook, ["gypsum vinyletiles", "kitchens"]),
		CEILING_GYPSUM_VINYL_BATHROOM_M2:findBoqProof(workbook, ["gypsum vinyletiles", "bathrooms"])
	};
}

function mapBoqItemToFinishKey(description){
	const d=String(description||"").toLowerCase().replace(/\s+/g," ").trim();
	if(/roofing\s*&\s*waterproofing works/.test(d)) return null;
	if(/water proofing for sub-?structure/.test(d) || /substructure works/.test(d)) return null;
	if(/grov(?:e|es)/.test(d)) return null;
	if(/paint/.test(d) && /ceiling/.test(d)) return null;
	if(/^water proofing works$/.test(d)) return null;
	if(/bath rooms water proofing/.test(d)) return null;
	if(/flooring for baths/.test(d)) return null;
	if(/granite floors/.test(d) && /enter/.test(d)) return "FLOOR_GRANITE_ENTRANCE_M2";
	if(/granite/.test(d) && /entrance stair landing/.test(d)) return "FLOOR_GRANITE_ENTRANCE_M2";
	if(/marble floors/.test(d) && /(hall|majles|setting)/.test(d)) return "FLOOR_MARBLE_RECEPTION_M2";
	if(/flooring for/.test(d) && /(hall|majlis|majles)/.test(d)) return "FLOOR_MARBLE_RECEPTION_M2";
	if(/ceramic floors/.test(d) && /(bedroom|bed room|balacony|balcony)/.test(d)) return "FLOOR_CERAMIC_BED_BALCONY_M2";
	if(/flooring for/.test(d) && /(bedrooms|dresses)/.test(d)) return "FLOOR_CERAMIC_BED_BALCONY_M2";
	if(/ceramic floors/.test(d) && /kitchen/.test(d)) return "FLOOR_CERAMIC_KITCHEN_M2";
	if(/ceramic floors/.test(d) && /bath/.test(d)) return "FLOOR_CERAMIC_BATHROOM_M2";
	if(/ceramic floors/.test(d) && /maid/.test(d)) return "FLOOR_CERAMIC_MAID_M2";
	if(/ceramic floors/.test(d) && /store/.test(d)) return "FLOOR_CERAMIC_STORE_M2";
	if(/ceramic/.test(d) && /(service room|iron|laundry)/.test(d)) return "FLOOR_CERAMIC_SERVICE_M2";
	if(/ceramic tiles/.test(d) && /kitchen/.test(d)) return "WALL_TILES_WET_AREAS_M2";
	if(/walls for/.test(d) && /(baths|kitchen)/.test(d)) return "WALL_TILES_WET_AREAS_M2";
	if(/plaster/.test(d) && /internal/.test(d)) return "PLASTER_INTERNAL_M2";
	if(/plaster/.test(d) && /(external|elevation)/.test(d)) return "PLASTER_EXTERNAL_M2";
	if(/paint|emulsion/.test(d) && /intern/.test(d)) return "PAINT_INTERNAL_M2";
	if(/epoxy paint/.test(d) || (/paint|emulsion/.test(d) && /(extern|elevation)/.test(d))) return "PAINT_EXTERNAL_M2";
	if((/water proof|waterproof/.test(d)) && (/wet areas?/.test(d) || /balacony|balcony/.test(d))) return "WET_AREAS_BALCONY_WATERPROOF_M2";
	if(/insulation system for roof/.test(d) || ((/water proof|waterproof|w\/p/.test(d)) && /roof/.test(d))) return "ROOF_WATERPROOF_M2";
	if(/spray plaster/.test(d) && /ceiling/.test(d)) return "CEILING_SPRAY_PLASTER_M2";
	if(/gypsum decoration/.test(d) && /(entrance|hall|majles|setting)/.test(d)) return "CEILING_GYPSUM_RECEPTION_M2";
	if(/gypsum decoration/.test(d) && /bed/.test(d)) return "CEILING_GYPSUM_BEDROOM_M2";
	if(/gypsum/.test(d) && /kitchen/.test(d)) return "CEILING_GYPSUM_VINYL_KITCHEN_M2";
	if(/gypsum/.test(d) && /bath/.test(d)) return "CEILING_GYPSUM_VINYL_BATHROOM_M2";
	return null;
}

function toAccuracy(variancePct){
	if(variancePct==null || !Number.isFinite(variancePct)) return null;
	return Math.max(0, 100-Math.abs(variancePct));
}

function normalizeReferenceUnit(unit){
	return String(unit||"").trim().toUpperCase().replace(/\./g,"");
}

function hasValidReferenceQty(item){
	const qty=Number(item?.qty||0);
	if(!(qty>0)) return false;
	const itemNo=Number(item?.item_no||0);
	const rowText=String(item?.row_text||"");
	if(itemNo>0 && Math.abs(qty-itemNo)<1e-9 && /-\s*-/.test(rowText)) return false;
	return true;
}

function runFinishPipeline({ finishDxfPath, referencePath, allowExternalReference=false, inputs, archDoorSchedule, archSeedSummary=null, archSeedDxfPath=null, outDir }){
	if(referencePath && !allowExternalReference){
		throw new Error("External reference input is disabled. FINISH pipeline accepts drawing inputs only.");
	}
	const { rules, source:rulesSource, signature:rulesSignature } = loadRules("VILLA_G1");
	let activeContext=loadFinishExtractionContext(finishDxfPath, inputs);
	if(archSeedDxfPath){
		const primaryPoor=activeContext.roomLabels.length<5 || activeContext.wallPairs.length===0;
		if(primaryPoor){
			const archContext=loadFinishExtractionContext(archSeedDxfPath, inputs);
			const archBetter=
				archContext.roomLabels.length>activeContext.roomLabels.length ||
				archContext.wallPairs.length>activeContext.wallPairs.length;
			if(archBetter) activeContext=archContext;
		}
	}
	const cadUnit=activeContext.cadUnit;
	const texts=activeContext.texts;
	const disciplineSig=activeContext.disciplineSig;
	const allSegments=activeContext.allSegments;
	const planScopes=activeContext.planScopes;
	const wallPairs=activeContext.wallPairs;

	const hasExternalReference=false;
	const qtoModeMeta={ mode:"QTO_ONLY", external_reference_enabled:false };
	const referenceItems=[];
	const referenceTotalsByKey={};

	let archSummary=null;
	if(archSeedSummary && typeof archSeedSummary==="object"){
		archSummary={ ...archSeedSummary };
	} else {
		const archSeedDir=path.join(outDir, "_arch_seed");
		fs.mkdirSync(archSeedDir, { recursive:true });
		runArchPipeline({
			archDxfPath:activeContext.dxfPath,
			referencePath:null,
			inputs,
			outDir:archSeedDir
		});
		const archQuantities=JSON.parse(fs.readFileSync(path.join(archSeedDir, "arch_quantities.json"), "utf8"));
		const externalWallLengthM=(archQuantities.items||[])
			.filter((item)=>String(item.code||"").startsWith("BLOCK_EXTERNAL_") && Number(item.length_m||0)>0)
			.reduce((sum,item)=>sum+Number(item.length_m||0),0);
		archSummary={
			...(archQuantities.computed_summary||{}),
			external_wall_length_m:externalWallLengthM
		};
	}
	const parapetFinishHeightM=deriveParapetHeightFromTexts(texts);
	archSummary={
		...(archSummary||{}),
		parapet_finish_height_m:parapetFinishHeightM
	};

	const roomLabels=activeContext.roomLabels;
	const boundarySegments=allSegments.filter(seg=>isLikelyBoundaryLayer(seg.layer));
	const roofPlanMetrics=deriveRoofPlanMetrics(allSegments, planScopes);
	const textAreaOverridesById=buildRoomAreaTextOverrides(texts, roomLabels);
	const boundaryAreaOverridesById=estimateAreaOverridesFromBoundarySegments(roomLabels, boundarySegments, ["ENTRANCE","SETTING","STORE","LAUNDRY"]);
	const ceilingBoundaryOverridesById=estimateAreaOverridesFromBoundarySegments(roomLabels, boundarySegments, ["KITCHEN"]);
	const areaOverridesById={ ...textAreaOverridesById, ...boundaryAreaOverridesById };
	const ceilingAreaOverridesById={ ...textAreaOverridesById, ...ceilingBoundaryOverridesById };
	const resolvedRooms=estimateRoomGeometries(roomLabels, wallPairs);
	const roomLibrary=buildRoomLibrary(roomLabels, resolvedRooms);
	const finishModel=buildFinishModel({ roomLabels, roomLibrary, archSummary, inputs, rules, areaOverridesById, ceilingAreaOverridesById, roomAreaOverridesById:areaOverridesById, roofPlanMetrics, archDoorSchedule });

	const systemQtyByKey={
		FLOOR_GRANITE_ENTRANCE_M2:Number(finishModel.floor_quantities.FLOOR_GRANITE_ENTRANCE_M2.total||0),
		FLOOR_MARBLE_RECEPTION_M2:Number(finishModel.floor_quantities.FLOOR_MARBLE_RECEPTION_M2.total||0),
		FLOOR_CERAMIC_BED_BALCONY_M2:Number(finishModel.floor_quantities.FLOOR_CERAMIC_BED_BALCONY_M2.total||0),
		FLOOR_CERAMIC_KITCHEN_M2:Number(finishModel.floor_quantities.FLOOR_CERAMIC_KITCHEN_M2.total||0),
		FLOOR_CERAMIC_BATHROOM_M2:Number(finishModel.floor_quantities.FLOOR_CERAMIC_BATHROOM_M2.total||0),
		FLOOR_CERAMIC_MAID_M2:Number(finishModel.floor_quantities.FLOOR_CERAMIC_MAID_M2.total||0),
		FLOOR_CERAMIC_STORE_M2:Number(finishModel.floor_quantities.FLOOR_CERAMIC_STORE_M2.total||0),
		FLOOR_CERAMIC_SERVICE_M2:Number(finishModel.floor_quantities.FLOOR_CERAMIC_SERVICE_M2.total||0),
		DRY_AREA_FLOORING_M2:Number(finishModel.floor_quantities.DRY_AREA_FLOORING_M2.total||0),
		WET_AREA_FLOORING_M2:Number(finishModel.floor_quantities.WET_AREA_FLOORING_M2.total||0),
		BALCONY_FLOORING_M2:Number(finishModel.floor_quantities.BALCONY_FLOORING_M2?.total||0),
		WALL_TILES_WET_AREAS_M2:Number(finishModel.wall_quantities.WALL_TILES_WET_AREAS_M2.total||0),
		PLASTER_INTERNAL_M2:Number(finishModel.wall_quantities.PLASTER_INTERNAL_M2.total||0),
		PLASTER_EXTERNAL_M2:Number(finishModel.wall_quantities.PLASTER_EXTERNAL_M2.total||0),
		PAINT_INTERNAL_M2:Number(finishModel.wall_quantities.PAINT_INTERNAL_M2.total||0),
		PAINT_EXTERNAL_M2:Number(finishModel.wall_quantities.PAINT_EXTERNAL_M2.total||0),
		PARAPET_FINISH_M2:Number(finishModel.wall_quantities.PARAPET_FINISH_M2.total||0),
		COPING_BEAM_FINISH_M2:Number(finishModel.wall_quantities.COPING_BEAM_FINISH_M2.total||0),
		ROOF_WATERPROOF_M2:Number(finishModel.wall_quantities.ROOF_WATERPROOF_M2.total||0),
		WET_AREAS_BALCONY_WATERPROOF_M2:Number(finishModel.wall_quantities.WET_AREAS_BALCONY_WATERPROOF_M2.total||0),
		CEILING_SPRAY_PLASTER_M2:Number(finishModel.ceiling_quantities.CEILING_SPRAY_PLASTER_M2.total||0),
		CEILING_GYPSUM_RECEPTION_M2:Number(finishModel.ceiling_quantities.CEILING_GYPSUM_RECEPTION_M2.total||0),
		CEILING_GYPSUM_BEDROOM_M2:Number(finishModel.ceiling_quantities.CEILING_GYPSUM_BEDROOM_M2.total||0),
		CEILING_GYPSUM_VINYL_KITCHEN_M2:Number(finishModel.ceiling_quantities.CEILING_GYPSUM_VINYL_KITCHEN_M2.total||0),
		CEILING_GYPSUM_VINYL_BATHROOM_M2:Number(finishModel.ceiling_quantities.CEILING_GYPSUM_VINYL_BATHROOM_M2.total||0),
		CEILING_DRY_AREA_M2:Number(finishModel.floor_quantities.DRY_AREA_FLOORING_M2.total||0),
		CEILING_WET_AREA_M2:Number(finishModel.ceiling_quantities.CEILING_GYPSUM_VINYL_KITCHEN_M2.total||0)
			+ Number(finishModel.ceiling_quantities.CEILING_GYPSUM_VINYL_BATHROOM_M2.total||0),
		SKIRTING_LM:Number(finishModel.skirting_quantities?.SKIRTING_LM?.total||0),
		MARBLE_THRESHOLD_LM:Number(finishModel.threshold_quantities?.MARBLE_THRESHOLD_LM?.total||0)
	};

	const systemItems=Object.entries(systemQtyByKey).map(([item, qty])=>({
		item,
		category:item.startsWith("FLOOR_") || /^(DRY_AREA_FLOORING|WET_AREA_FLOORING|BALCONY_FLOORING)/.test(item) ? "FLOOR"
			: item.startsWith("CEILING_") ? "CEILING"
			: /^SKIRTING|^MARBLE/.test(item) ? "SKIRTING"
			: "WALL",
		unit:/LM$/.test(item) ? "lm" : "m2",
		qty:roundN(qty,4),
		source:(()=>{
			if(item.startsWith("PARAPET_") || item.startsWith("COPING_")) return "ARCH_PLUS_PARAPET_MODEL";
			if(item.startsWith("PAINT_") || item.startsWith("PLASTER_")) return "ARCH_PLUS_ROOM_MODEL";
			if(item==="WALL_TILES_WET_AREAS_M2") return "ROOM_WET_MODEL";
			if(/SKIRTING|THRESHOLD/.test(item)) return "ROOM_PERIMETER_MODEL";
			if(/^(DRY_AREA_FLOORING|WET_AREA_FLOORING|BALCONY_FLOORING)/.test(item)) return "ROOM_AGGREGATE";
			return "ROOM_LABEL_MODEL";
		})()
	}));

	const fallbackRoomCount=finishModel.room_geometries.filter(row=>row.source==="ROOM_TEMPLATE_FALLBACK").length;
	const warnings=[];
	if(fallbackRoomCount>0 && roomLabels.length<10){
		warnings.push({ severity:"HIGH", code:"FINISH_ROOM_TEMPLATE_FALLBACK", message:`${fallbackRoomCount} room labels fell back to default room templates because direct room enclosure geometry was not resolved.`, action:"Refine room boundary extraction on drawings that need tighter finish accuracy." });
	}

	const hardBlocks=[];
	if(!inputs?.allowMixedDiscipline && disciplineSig.likely==="STR" && disciplineSig.strScore>=3){
		hardBlocks.push(`WRONG_DISCIPLINE_DXF: likely STR (str=${disciplineSig.strScore}, arch=${disciplineSig.archScore}, finish=${disciplineSig.finishScore})`);
	}

	const allItemsFailCount=0;

	const requiredQuestions=buildRequiredQuestionsFromRules(rules, inputs);
	const releaseDecision=finalizeReleaseGate({ runStatus:hardBlocks.length?"REJECTED":"OK", warnings, requiredQuestions, hardBlocks, mode:"QTO_ONLY" });

	const quantities={
		scope:"FINISHING quantities (room-driven model on top of ARCH wall-face base)",
		measurement_mode:"QTO_ONLY",
		external_reference_enabled:false,
		projectType:"VILLA_G1",
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		computed_summary:{
			drawing_unit:cadUnit,
			room_labels_count:finishModel.room_labels_count,
			internal_paint_m2:systemQtyByKey.PAINT_INTERNAL_M2,
			external_paint_m2:systemQtyByKey.PAINT_EXTERNAL_M2,
			internal_plaster_m2:systemQtyByKey.PLASTER_INTERNAL_M2,
			external_plaster_m2:systemQtyByKey.PLASTER_EXTERNAL_M2,
			parapet_finish_m2:systemQtyByKey.PARAPET_FINISH_M2,
			coping_beam_finish_m2:systemQtyByKey.COPING_BEAM_FINISH_M2,
			roof_waterproof_m2:systemQtyByKey.ROOF_WATERPROOF_M2,
			wet_areas_balcony_waterproof_m2:systemQtyByKey.WET_AREAS_BALCONY_WATERPROOF_M2,
			floor_granite_entrance_m2:systemQtyByKey.FLOOR_GRANITE_ENTRANCE_M2,
			floor_marble_reception_m2:systemQtyByKey.FLOOR_MARBLE_RECEPTION_M2,
			floor_ceramic_bed_balcony_m2:systemQtyByKey.FLOOR_CERAMIC_BED_BALCONY_M2,
			floor_ceramic_kitchen_m2:systemQtyByKey.FLOOR_CERAMIC_KITCHEN_M2,
			floor_ceramic_bathroom_m2:systemQtyByKey.FLOOR_CERAMIC_BATHROOM_M2,
			floor_ceramic_maid_m2:systemQtyByKey.FLOOR_CERAMIC_MAID_M2,
			floor_ceramic_store_m2:systemQtyByKey.FLOOR_CERAMIC_STORE_M2,
			floor_ceramic_service_m2:systemQtyByKey.FLOOR_CERAMIC_SERVICE_M2,
			dry_area_flooring_m2:systemQtyByKey.DRY_AREA_FLOORING_M2,
			wet_area_flooring_m2:systemQtyByKey.WET_AREA_FLOORING_M2,
			balcony_flooring_m2:systemQtyByKey.BALCONY_FLOORING_M2,
			wall_tiles_wet_areas_m2:systemQtyByKey.WALL_TILES_WET_AREAS_M2,
			ceiling_spray_plaster_m2:systemQtyByKey.CEILING_SPRAY_PLASTER_M2,
			ceiling_gypsum_reception_m2:systemQtyByKey.CEILING_GYPSUM_RECEPTION_M2,
			ceiling_gypsum_bedroom_m2:systemQtyByKey.CEILING_GYPSUM_BEDROOM_M2,
			ceiling_gypsum_vinyl_kitchen_m2:systemQtyByKey.CEILING_GYPSUM_VINYL_KITCHEN_M2,
			ceiling_gypsum_vinyl_bathroom_m2:systemQtyByKey.CEILING_GYPSUM_VINYL_BATHROOM_M2,
			skirting_lm:systemQtyByKey.SKIRTING_LM,
			marble_threshold_lm:systemQtyByKey.MARBLE_THRESHOLD_LM
		},
		items:systemItems,
		item_stop:[],
		room_model:finishModel,
		accuracy_notice:"Finishing quantities combine ARCH wall-face evidence with room-label driven room geometry.",
		professional_warnings:warnings
	};

	const evidence={
		file:path.basename(activeContext.dxfPath),
		stats:{ text_entities_total:texts.length, room_labels_count:roomLabels.length, wall_pair_count:wallPairs.length },
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		arch_seed_summary:archSummary,
		source_dxf:path.basename(activeContext.dxfPath),
		plan_scopes:planScopes,
		room_labels:roomLabels,
		room_templates:roomLibrary.representativeByKey,
		external_reference_totals:{},
		notes:["FINISHING room model active on top of ARCH wall-face quantities."]
	};

	const runMeta={
		projectId:inputs?._meta?.projectId||null,
		runId:inputs?._meta?.runId||null,
		timestamp:inputs?._meta?.timestamp||new Date().toISOString(),
		rules_meta:{ loaded:Boolean(rules), source:rulesSource, signature:rulesSignature, name:rules?.meta?.name||null, version:rules?.meta?.version||null },
		discipline_signature:disciplineSig,
		discipline:"FINISHING",
		min_accuracy_pct:MIN_ACCURACY_PCT,
		target_accuracy_pct:TARGET_ACCURACY_PCT,
		honest_mode:HONEST_MODE,
		run_status:hardBlocks.length?"REJECTED":"OK",
		rejected_reason:hardBlocks.join(" | ") || null,
		release_gate:releaseDecision.gate,
		measurement_mode:"QTO_ONLY",
		external_reference_enabled:false,
		external_reference_mode:"NONE",
		external_reference_items:0,
		all_items_failed_gate_count:allItemsFailCount,
		professional_warnings:warnings,
		explanations:{
			policy:"Finishing quantities are produced from room labels, wet-area rules, room templates, and ARCH wall-face outputs.",
			release_condition:"Accuracy depends on room-label coverage and whether room enclosure geometry resolves directly from the plan."
		}
	};

	const qualityReport={
		discipline:"FINISHING",
		policy:{ min_accuracy_pct:MIN_ACCURACY_PCT, target_accuracy_pct:TARGET_ACCURACY_PCT },
		measurement_mode:"QTO_ONLY",
		external_reference_enabled:false,
		discipline_signature:disciplineSig,
		warnings,
		required_questions:requiredQuestions,
		hard_blocks:hardBlocks,
		release_decision:releaseDecision
	};

	fs.writeFileSync(path.join(outDir,"finish_qto_mode.json"), JSON.stringify(qtoModeMeta,null,2));
	fs.writeFileSync(path.join(outDir,"finish_quantities.json"), JSON.stringify(quantities,null,2));
	fs.writeFileSync(path.join(outDir,"finish_run_meta.json"), JSON.stringify(runMeta,null,2));
	fs.writeFileSync(path.join(outDir,"finish_evidence.json"), JSON.stringify(evidence,null,2));
	fs.writeFileSync(path.join(outDir,"finish_system_items.csv"), Papa.unparse(systemItems));
	fs.writeFileSync(path.join(outDir,"finish_room_geometries.csv"), Papa.unparse(finishModel.room_geometries));
	fs.writeFileSync(path.join(outDir,"finish_wet_wall_tiles.csv"), Papa.unparse(finishModel.wall_quantities.WALL_TILES_WET_AREAS_M2.used));
	fs.writeFileSync(path.join(outDir,"finish_parapet_coping.csv"), Papa.unparse([
		...(finishModel.wall_quantities.PARAPET_FINISH_M2.used||[]).map(row=>({ item:"PARAPET_FINISH_M2", ...row })),
		...(finishModel.wall_quantities.COPING_BEAM_FINISH_M2.used||[]).map(row=>({ item:"COPING_BEAM_FINISH_M2", ...row }))
	]));
	fs.writeFileSync(path.join(outDir,"finish_required_questions.json"), JSON.stringify(requiredQuestions,null,2));
	fs.writeFileSync(path.join(outDir,"finish_quality_report.json"), JSON.stringify(qualityReport,null,2));

	return {
		qtoModeFile:"finish_qto_mode.json",
		quantitiesFile:"finish_quantities.json",
		runMetaFile:"finish_run_meta.json",
		evidenceFile:"finish_evidence.json",
		systemCsv:"finish_system_items.csv",
		requiredQuestionsFile:"finish_required_questions.json",
		qualityReportFile:"finish_quality_report.json"
	};
}

module.exports = { runFinishPipeline };
