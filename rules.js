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
	const localPath = path.join(__dirname, "..", "config", "VILLA_G1_QTO_RULES_MASTER.json");
	if(fs.existsSync(localPath)){
		const rules = JSON.parse(fs.readFileSync(localPath, "utf8"));
		return { rules, source:localPath, signature:rulesSignature(rules) };
	}
	const fallbackPath = "C:/Users/basel/Downloads/VILLA_G1_QTO_RULES_MASTER.json";
	if(fs.existsSync(fallbackPath)){
		const rules = JSON.parse(fs.readFileSync(fallbackPath, "utf8"));
		return { rules, source:fallbackPath, signature:rulesSignature(rules) };
	}
	return { rules:null, source:null, signature:null };
}

module.exports = { loadRules, rulesSignature };
