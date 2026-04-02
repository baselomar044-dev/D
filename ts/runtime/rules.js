const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

function stableStringify(obj){
	if(obj===null || typeof obj!=="object") return JSON.stringify(obj);
	if(Array.isArray(obj)) return `[${obj.map(stableStringify).join(",")}]`;
	const keys=Object.keys(obj).sort();
	return `{${keys.map(k=>`${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
}

function rulesSignature(rules){
	const s=stableStringify(rules||{});
	return crypto.createHash("sha256").update(s).digest("hex");
}

function loadRules(projectType){
	if(String(projectType||"").toUpperCase()!=="VILLA_G1") return { rules:null, source:null, signature:null };
	const searchPaths = [
		path.join(__dirname, "..", "..", "config", "VILLA_G1_QTO_RULES_MASTER.json"),
		path.join(__dirname, "..", "config", "VILLA_G1_QTO_RULES_MASTER.json"),
		process.env.QTO_RULES_PATH || "",
	].filter(Boolean);
	for(const p of searchPaths){
		if(fs.existsSync(p)){
			const rules = JSON.parse(fs.readFileSync(p, "utf8"));
			return { rules, source:p, signature:rulesSignature(rules) };
		}
	}
	return { rules:null, source:null, signature:null };
}

module.exports = { loadRules, rulesSignature };
