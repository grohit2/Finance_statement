from __future__ import annotations
import json, re
from pathlib import Path
from typing import Any, Dict, List

def load_rules_dir(dir_path: str) -> Dict[str, Any]:
    base = Path(dir_path)
    if not base.exists():
        raise FileNotFoundError(f"Rules directory not found: {dir_path}")
    rules: List[Dict[str, Any]] = []
    for fp in sorted(base.glob("*.ndjson")):
        with fp.open("r", encoding="utf-8") as f:
            for ln, line in enumerate(f, 1):
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError as e:
                    raise ValueError(f"{fp}:{ln} invalid JSON: {e}")
                obj.setdefault("id", f"{fp.stem}:{ln}")
                obj.setdefault("column", "name")
                obj.setdefault("op", "regex")
                if obj["op"].lower() != "regex":
                    raise ValueError(f"{fp}:{ln} only 'regex' op supported")
                try:
                    re.compile(obj["value"])
                except re.error as e:
                    raise ValueError(f"{fp}:{ln} invalid regex: {e}")
                rules.append(obj)
    return {"rules": rules, "source": dir_path}
