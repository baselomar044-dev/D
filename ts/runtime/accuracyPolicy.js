const MIN_ACCURACY_PCT = 90.0;
const TARGET_ACCURACY_PCT = 100.0;
const HONEST_MODE = true;

function toAccuracyPct(variancePct){
	return Math.max(0, 100 - Math.abs(Number(variancePct) || 0));
}

/**
 * FIX: In QTO_ONLY mode (no external reference), accuracyPct is null.
 * null means "unscored" — not a failure. Only fail if we have a score AND it's below threshold.
 */
function passesAccuracyGate(accuracyPct){
	if(accuracyPct === null || accuracyPct === undefined || !Number.isFinite(Number(accuracyPct))){
		return true; // No reference = unscored = not blocked
	}
	return Number(accuracyPct) >= MIN_ACCURACY_PCT;
}

function meetsTargetAccuracy(accuracyPct){
	if(accuracyPct === null || accuracyPct === undefined || !Number.isFinite(Number(accuracyPct))){
		return false; // Unscored = not at target, but not a failure
	}
	return Number(accuracyPct) >= TARGET_ACCURACY_PCT;
}

function accuracyBand(accuracyPct){
	if(accuracyPct === null || accuracyPct === undefined || !Number.isFinite(Number(accuracyPct))){
		return { code:"UNSCORED", label:"Unscored (QTO-only mode)", highlight:"amber" };
	}
	const a = Number(accuracyPct);
	if(a >= TARGET_ACCURACY_PCT) return { code:"EXCELLENT_100", label:"Perfect (100%)", highlight:"green" };
	if(a >= MIN_ACCURACY_PCT) return { code:"ACCEPTED_90_99", label:"Accepted (90-99.99%)", highlight:"amber" };
	return { code:"BELOW_MIN_90", label:"Below Minimum (<90%)", highlight:"red" };
}

module.exports = {
	MIN_ACCURACY_PCT,
	TARGET_ACCURACY_PCT,
	HONEST_MODE,
	toAccuracyPct,
	passesAccuracyGate,
	meetsTargetAccuracy,
	accuracyBand
};
