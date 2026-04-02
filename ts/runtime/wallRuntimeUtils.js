function overlapLen(a1,a2,b1,b2){
	const lo=Math.max(a1,b1);
	const hi=Math.min(a2,b2);
	return Math.max(0, hi-lo);
}

function quantizeThickness(t){
	if(Math.abs(t-0.143)<=0.02) return 0.15;
	const options=[0.10,0.15,0.20,0.25];
	let best=options[0];
	let bestD=Math.abs(t-options[0]);
	for(const option of options){
		const delta=Math.abs(t-option);
		if(delta<bestD){
			best=option;
			bestD=delta;
		}
	}
	return best;
}

function buildWallPairs(segments, opts={}){
	const minDist=Number.isFinite(opts.minDist) ? opts.minDist : 0.08;
	const maxDist=Number.isFinite(opts.maxDist) ? opts.maxDist : 0.30;
	const minOverlap=Number.isFinite(opts.minOverlap) ? opts.minOverlap : 1.2;
	const minOverlapRatio=Number.isFinite(opts.minOverlapRatio) ? opts.minOverlapRatio : 0.7;
	const byOri={ H:(segments||[]).filter((segment)=>segment.ori==="H"), V:(segments||[]).filter((segment)=>segment.ori==="V") };
	const out=[];
	for(const ori of ["H","V"]){
		const arr=byOri[ori];
		for(let i=0;i<arr.length;i++){
			for(let j=i+1;j<arr.length;j++){
				const s1=arr[i];
				const s2=arr[j];
				const dist=Math.abs(s1.c-s2.c);
				if(dist<minDist || dist>maxDist) continue;
				const ov=overlapLen(s1.a,s1.b,s2.a,s2.b);
				if(ov<minOverlap) continue;
				if(ov < minOverlapRatio*Math.min(s1.len,s2.len)) continue;
				out.push({ s1, s2, thickness:quantizeThickness(dist), overlap:ov, ori });
			}
		}
	}
	out.sort((a,b)=>b.overlap-a.overlap);
	const used=new Set();
	const keep=[];
	for(const pair of out){
		if(used.has(pair.s1.id) || used.has(pair.s2.id)) continue;
		used.add(pair.s1.id);
		used.add(pair.s2.id);
		keep.push(pair);
	}
	return keep;
}

function estimateDominantFoldAngleDeg(segments){
	const bins=new Map();
	for(const seg of (segments||[])){
		const dx=(Number(seg.x2)||0)-(Number(seg.x1)||0);
		const dy=(Number(seg.y2)||0)-(Number(seg.y1)||0);
		const len=Math.hypot(dx,dy);
		if(!(len>=1.0)) continue;
		let ang=Math.atan2(dy,dx)*180/Math.PI;
		ang=((ang%180)+180)%180;
		if(ang>90) ang=180-ang;
		const key=Math.round(ang/2)*2;
		bins.set(key,(bins.get(key)||0)+len);
	}
	let best=0;
	let bestScore=-1;
	for(const [key,value] of bins.entries()){
		if(value>bestScore){
			bestScore=value;
			best=Number(key)||0;
		}
	}
	return best;
}

function toAxisAlignedSegments(segments){
	const thetaDeg=estimateDominantFoldAngleDeg(segments);
	const thetaRad=thetaDeg*Math.PI/180;
	const c=Math.cos(thetaRad);
	const s=Math.sin(thetaRad);
	return (segments||[]).map((seg)=>{
		const x1r=(seg.x1*c)+(seg.y1*s);
		const y1r=(-(seg.x1*s))+(seg.y1*c);
		const x2r=(seg.x2*c)+(seg.y2*s);
		const y2r=(-(seg.x2*s))+(seg.y2*c);
		const dx=x2r-x1r;
		const dy=y2r-y1r;
		const len=Math.hypot(dx,dy);
		if(!(len>0.2)) return null;
		const tol=Math.max(0.02, len*0.05);
		if(Math.abs(dy)<=tol){
			return {
				...seg,
				ori:"H",
				c:(y1r+y2r)/2,
				a:Math.min(x1r,x2r),
				b:Math.max(x1r,x2r),
				len
			};
		}
		if(Math.abs(dx)<=tol){
			return {
				...seg,
				ori:"V",
				c:(x1r+x2r)/2,
				a:Math.min(y1r,y2r),
				b:Math.max(y1r,y2r),
				len
			};
		}
		return null;
	}).filter(Boolean);
}

function pairWalls(segments, opts={}){
	const useAxisAlignment=opts.useAxisAlignment===true;
	const smallSetThreshold=Number.isFinite(opts.smallSetThreshold) ? opts.smallSetThreshold : 200;
	const strictOptions=opts.strictOptions || {};
	const fallbackOptions=opts.fallbackOptions || {
		minDist:0.03,
		maxDist:0.60,
		minOverlap:0.5,
		minOverlapRatio:0.35
	};
	let workSegs=segments||[];
	if(useAxisAlignment){
		const axisReady=(segments||[]).filter((segment)=>segment.ori==="H"||segment.ori==="V");
		workSegs=axisReady.length>=40 ? axisReady : toAxisAlignedSegments(segments);
	}
	const strictPairs=buildWallPairs(workSegs, strictOptions);
	if(strictPairs.length>0 || workSegs.length<smallSetThreshold) return strictPairs;
	return buildWallPairs(workSegs, fallbackOptions);
}

function isLikelyWallLayer(layer){
	const value=String(layer||"").toLowerCase();
	if(!value) return false;
	if(/dim|text|hatch|axis|grid|center|door|window|furn|elev|sect|note|anno|title|tbl|table|rev|level|slope|steel|rebar|glaz|glass|curtain|mullion|railing|stair|elect|power|light|hvac|ac|plumb|drain|water|gas|hidden|spot|symbol/.test(value)) return false;
	return /(^|[$_.-])(wall|walls)(?=$|[$_.-])|block|masonry|brick|mabani|jidar/.test(value);
}

module.exports={
	overlapLen,
	quantizeThickness,
	buildWallPairs,
	pairWalls,
	isLikelyWallLayer,
	estimateDominantFoldAngleDeg,
	toAxisAlignedSegments
};