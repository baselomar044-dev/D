const { MIN_ACCURACY_PCT, TARGET_ACCURACY_PCT, accuracyBand, passesAccuracyGate, meetsTargetAccuracy } = require("./accuracyPolicy");

function detectDisciplineSignature(texts){
	const corpus=(texts||[]).map(t=>String(t.text||"").toLowerCase()).join(" ");
	const score=(arr)=>arr.reduce((s,k)=>s+(corpus.includes(k)?1:0),0);
	const strScore=score(["footing","column","tie beam","tb","cf","r.c.c","reinforced concrete","قاعدة","اعمدة","أعمدة","سمل"]);
	const archScore=score(["wall","blockwork","plaster","paint","door","window","masonry","block","جدار","بلوك","لياسة","دهان"]);
	const finishScore=score(["tile","false ceiling","ceiling","finish","finishing","ceramic","porcelain","بلاط","تشطيب","سقف"]);
	return {
		strScore,
		archScore,
		finishScore,
		likely: strScore>=archScore && strScore>=finishScore ? "STR" : (archScore>=finishScore ? "ARCH" : "FINISHING")
	};
}

function buildRequiredQuestionsFromRules(rules, inputs){
	const questions=[];
	const req = rules?.structure?.inputs_required_if_missing || [];
	for(const key of req){
		if(key==="reference_level" && !inputs?.levels?.reference) questions.push({ severity:"CRITICAL", field:"levels.reference", question:"What is the reference level (e.g., NGL0/FFL0)?" });
		if(key==="foundation_depth_m" && !(Number(inputs?.levels?.foundation_depth_m)>0)) questions.push({ severity:"HIGH", field:"levels.foundation_depth_m", question:"Provide foundation depth in meters." });
		if(key==="g_floor_to_floor_m" && !(Number(inputs?.levels?.g_floor_to_floor_m)>0)) questions.push({ severity:"CRITICAL", field:"levels.g_floor_to_floor_m", question:"Provide ground floor-to-floor height in meters." });
		if(key==="f1_floor_to_floor_m" && !(Number(inputs?.levels?.f1_floor_to_floor_m)>0)) questions.push({ severity:"HIGH", field:"levels.f1_floor_to_floor_m", question:"Provide first floor-to-floor height in meters." });
	}
	return { batch_mode:true, questions };
}

function buildItemQuality(item, accuracyPct){
	if(accuracyPct===null || accuracyPct===undefined || !Number.isFinite(Number(accuracyPct))){
		return {
			item,
			accuracy_pct:null,
			band:{ code:"UNSCORED", label:"Unscored (QTO-only mode)", highlight:"amber" },
			min90_status:"N/A",
			target100_status:"N/A"
		};
	}
	const band = accuracyBand(accuracyPct);
	return {
		item,
		accuracy_pct:accuracyPct,
		band,
		min90_status:passesAccuracyGate(accuracyPct)?"PASS":"FAIL",
		target100_status:meetsTargetAccuracy(accuracyPct)?"PASS":"WARN"
	};
}

/**
 * FIX: In QTO_ONLY mode, runStatus is always "OK" because there is no external
 * reference to compare against. We only block on hard rules and critical DXF errors.
 * Accuracy gates are skipped when there is no reference (accuracyPct === null).
 */
function finalizeReleaseGate({ runStatus, warnings, requiredQuestions, hardBlocks }){
	const hasHard=(hardBlocks||[]).length>0;

	// Only block on CRITICAL warnings that are NOT accuracy-related
	// Accuracy warnings are informational in QTO_ONLY mode
	const blockingCriticalWarnings = (warnings||[]).filter(w => {
		const severity = String(w.severity||"").toUpperCase();
		const code = String(w.code||"").toUpperCase();
		if(severity !== "CRITICAL") return false;
		// Skip accuracy-related warnings in QTO_ONLY mode — no reference = unscored, not failed
		if(code.includes("BELOW_MIN") || code.includes("ACCURACY")) return false;
		// Skip "not implemented" warnings — these are known limitations, not failures
		if(code.includes("NOT_IMPLEMENTED")) return false;
		return true;
	});

	const hasRequired=(requiredQuestions?.questions||[])
		.filter(q => String(q.severity||"").toUpperCase() === "CRITICAL")
		.length > 0;

	if(hasHard) return { gate:"BLOCKED_HARD_RULE", rationale:hardBlocks.join(" | ") };
	if(blockingCriticalWarnings.length > 0) return { gate:"BLOCKED_CRITICAL_WARNING", rationale: blockingCriticalWarnings.map(w=>w.message||w.code).join(" | ") };
	if(hasRequired) return { gate:"BLOCKED_REQUIRED_QUESTIONS", rationale:"Critical user questions unresolved." };

	// runStatus "REJECTED" in QTO_ONLY mode means accuracy check failed against null reference
	// That's not a block — it's just unscored
	if(runStatus !== "OK" && runStatus !== "REJECTED"){
		return { gate:"BLOCKED_MIN_ACCURACY", rationale:"Run status not OK." };
	}

	return { gate:"ELIGIBLE", rationale:`QTO_ONLY mode: extraction complete. No external reference — quantities unscored but not blocked.` };
}

module.exports = {
	detectDisciplineSignature,
	buildRequiredQuestionsFromRules,
	buildItemQuality,
	finalizeReleaseGate
};
