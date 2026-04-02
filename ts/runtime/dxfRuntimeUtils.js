function normText(s){
	if(typeof s!=="string") return "";
	return s.replace(/\u0000/g, "").replace(/\\P/g, " ").replace(/\s+/g, " ").trim();
}

function getXY(e){
	const p=e.startPoint || e.position || e.insert || null;
	if(!p || typeof p.x!=="number" || typeof p.y!=="number") return { x:null, y:null };
	return { x:p.x, y:p.y };
}

function collectTextsFromEntities(entities){
	const out=[];
	for(const e of (entities||[])){
		const { x, y }=getXY(e);
		if(e.type==="TEXT" && typeof e.text==="string") out.push({ text:normText(e.text), x, y, layer:e.layer||"" });
		else if(e.type==="MTEXT" && typeof e.text==="string") out.push({ text:normText(e.text), x, y, layer:e.layer||"" });
		else if(e.type==="ATTRIB" && typeof e.text==="string") out.push({ text:normText(e.text), x, y, layer:e.layer||"" });
	}
	return out;
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

function collectInsertReferences(doc){
	const refs=[];
	const topLevel=Array.isArray(doc?.entities) ? doc.entities : [];
	const visit=(insert, depth=0, parentPath="")=>{
		if(!insert || insert.type!=="INSERT" || depth>6) return;
		const name=String(insert.name||"");
		const position=insert.position || null;
		const pathToken=parentPath ? `${parentPath}>${name}` : name;
		refs.push({
			name,
			layer:String(insert.layer||""),
			x:typeof position?.x==="number" ? position.x : null,
			y:typeof position?.y==="number" ? position.y : null,
			depth,
			path:pathToken
		});
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
	return refs;
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

function getCadLengthUnit(header, fallbackUnit="m"){
	const userUnit=normalizeLengthUnitToken(fallbackUnit);
	const ins=Number(header?.$INSUNITS);
	if(ins===4) return "mm";
	if(ins===5) return "cm";
	if(ins===6) return "m";
	if(ins===1){
		if(userUnit) return userUnit;
		return "in";
	}
	if(ins===2){
		if(userUnit) return userUnit;
		return "ft";
	}
	if(userUnit) return userUnit;
	const measurement=Number(header?.$MEASUREMENT);
	if(measurement===1) return "mm";
	return "m";
}

function unitScaleToMeters(unit){
	if(unit==="mm") return 0.001;
	if(unit==="cm") return 0.01;
	if(unit==="m") return 1;
	if(unit==="in") return 0.0254;
	if(unit==="ft") return 0.3048;
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

function scaleInsertRefsToMeters(insertRefs, factor){
	if(!(factor>0) || factor===1) return insertRefs||[];
	return (insertRefs||[]).map((row)=>({
		...row,
		x:typeof row?.x==="number" ? row.x*factor : row?.x,
		y:typeof row?.y==="number" ? row.y*factor : row?.y
	}));
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

function computeBoundsFromPoints(points){
	let minX=Infinity;
	let maxX=-Infinity;
	let minY=Infinity;
	let maxY=-Infinity;
	let found=false;
	for(const point of (points||[])){
		const x=Number(point?.x);
		const y=Number(point?.y);
		if(!Number.isFinite(x) || !Number.isFinite(y)) continue;
		if(x<minX) minX=x;
		if(x>maxX) maxX=x;
		if(y<minY) minY=y;
		if(y>maxY) maxY=y;
		found=true;
	}
	if(!found) return null;
	return { x1:minX, x2:maxX, y1:minY, y2:maxY };
}

function entityIntersectsScopeRect(entity, scopeRect){
	const points=collectEntityPoints(entity);
	if(!points.length) return true;
	const bounds=computeBoundsFromPoints(points);
	if(!bounds) return true;
	return !(bounds.x2<scopeRect.x1 || bounds.x1>scopeRect.x2 || bounds.y2<scopeRect.y1 || bounds.y1>scopeRect.y2);
}

function applyScopeRectToEntities(entities, scopeRect){
	const normalized=normalizeScopeRect(scopeRect);
	if(!normalized) return entities;
	return (entities||[]).filter((entity)=>entityIntersectsScopeRect(entity, normalized));
}

function applyScopeRectToInsertRefs(insertRefs, scopeRect){
	const normalized=normalizeScopeRect(scopeRect);
	if(!normalized) return insertRefs||[];
	return (insertRefs||[]).filter((row)=>{
		if(row?.x==null || row?.y==null) return true;
		const x=Number(row.x);
		const y=Number(row.y);
		if(!Number.isFinite(x) || !Number.isFinite(y)) return true;
		return x>=normalized.x1 && x<=normalized.x2 && y>=normalized.y1 && y<=normalized.y2;
	});
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

function applyScopeCircleToInsertRefs(insertRefs, scopeCenter, scopeRadius){
	const normalized=normalizeScopeCircle(scopeCenter, scopeRadius);
	if(!normalized) return insertRefs||[];
	const r2=normalized.r*normalized.r;
	return (insertRefs||[]).filter((row)=>{
		if(row?.x==null || row?.y==null) return true;
		const x=Number(row.x);
		const y=Number(row.y);
		if(!Number.isFinite(x) || !Number.isFinite(y)) return true;
		const dx=x-normalized.x;
		const dy=y-normalized.y;
		return (dx*dx + dy*dy)<=r2;
	});
}

module.exports={
	normText,
	getXY,
	collectTextsFromEntities,
	cloneEntity,
	transformPoint,
	expandInsertEntities,
	flattenDxfEntities,
	collectInsertReferences,
	normalizeLengthUnitToken,
	getCadLengthUnit,
	unitScaleToMeters,
	scalePointToMeters,
	scaleEntitiesToMeters,
	scaleInsertRefsToMeters,
	normalizeScopeRect,
	collectEntityPoints,
	computeBoundsFromPoints,
	entityIntersectsScopeRect,
	applyScopeRectToEntities,
	applyScopeRectToInsertRefs,
	normalizeScopeCircle,
	entityIntersectsScopeCircle,
	applyScopeCircleToEntities,
	applyScopeCircleToInsertRefs
};