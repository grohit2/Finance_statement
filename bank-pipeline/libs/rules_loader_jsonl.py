from __future__ import annotations
import json, re
from pathlib import Path
from typing import Any, Dict, List

REQUIRED_KEYS = {"id", "priority", "logic", "actions"}

def _validate_rule(rule: Dict[str, Any]) -> None:
    missing = REQUIRED_KEYS - set(rule.keys())
    if missing:
        raise ValueError(f"Rule {rule.get('id')} missing keys: {sorted(missing)}")
    if str(rule.get("logic", "")).lower() not in ("any", "all"):
        raise ValueError(f"Rule {rule.get('id')} has invalid logic: {rule.get('logic')}")
    # Light regex validation
    for p in rule.get("match", []) or []:
        if str(p.get("op","")) == "regex":
            try:
                re.compile(str(p.get("value","")))
            except re.error as e:
                raise ValueError(f"Rule {rule.get('id')} invalid regex: {e}")

def load_rules_from_ndjson(dir_path: str) -> Dict[str, Any]:
    base = Path(dir_path)
    if not base.exists():
        raise FileNotFoundError(f"Rules directory not found: {dir_path}")
    rules_by_id: Dict[str, Dict[str, Any]] = {}
    for fp in sorted(base.glob("*.ndjson")):
        with fp.open("r", encoding="utf-8") as f:
            for ln, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON in {fp}:{ln}: {e}")
                _validate_rule(obj)
                obj["logic"] = str(obj["logic"]).lower()
                rules_by_id[obj["id"]] = obj
    # Sort by (priority, id) asc
    rules: List[Dict[str, Any]] = sorted(
        rules_by_id.values(), key=lambda r: (int(r.get("priority", 1000)), str(r.get("id", "zzz")))
    )
    return {"rules": rules}

