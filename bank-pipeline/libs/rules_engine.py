from typing import Dict, Any, List
import yaml
from pyspark.sql import DataFrame, functions as F


def load_rules(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _normalize_case(col, case_insensitive: bool):
    c = F.col(col)
    return F.lower(c) if case_insensitive else c


def _coerce_numeric(col):
    return F.col(col).cast("double")


def _compile_predicate(p: Dict[str, Any]) -> F.Column:
    col = p["column"]
    op = p["op"].lower()
    ci = bool(p.get("case_insensitive", False))
    if op == "equals":
        v = p["value"]
        left = _normalize_case(col, ci)
        litv = v.lower() if ci and isinstance(v, str) else v
        return left.eqNullSafe(F.lit(litv))
    if op == "contains":
        v = str(p["value"])
        left = F.lower(F.col(col)) if ci else F.col(col)
        return (F.instr(left.cast("string"), v.lower() if ci else v) > F.lit(0))
    if op == "regex":
        pat = str(p["value"])
        return F.col(col).cast("string").rlike(pat)
    if op == "startswith":
        v = str(p["value"])
        left = F.lower(F.col(col)) if ci else F.col(col)
        return left.cast("string").startswith(v.lower() if ci else v)
    if op == "endswith":
        v = str(p["value"])
        left = F.lower(F.col(col)) if ci else F.col(col)
        return left.cast("string").endswith(v.lower() if ci else v)
    if op == "in":
        vals = p["values"]
        vals = [v.lower() for v in vals] if ci else vals
        left = F.lower(F.col(col)) if ci else F.col(col)
        return left.isin(vals)
    if op in ("gt", "gte", "lt", "lte"):
        v = float(p["value"])
        left = _coerce_numeric(col)
        if op == "gt":
            return left > F.lit(v)
        if op == "gte":
            return left >= F.lit(v)
        if op == "lt":
            return left < F.lit(v)
        if op == "lte":
            return left <= F.lit(v)
    if op == "between":
        a, b = p["values"]
        left = _coerce_numeric(col)
        return (left >= F.lit(float(a))) & (left <= F.lit(float(b)))
    raise ValueError(f"Unsupported op: {op}")


def _compile_rule_expr(rule: Dict[str, Any]) -> F.Column:
    preds = rule.get("match", [])
    logic = (rule.get("logic") or "all").lower()
    if not preds:
        return F.lit(True)
    exprs = [_compile_predicate(p) for p in preds]
    if logic == "any":
        cond = exprs[0]
        for e in exprs[1:]:
            cond = cond | e
        return cond
    cond = exprs[0]
    for e in exprs[1:]:
        cond = cond & e
    return cond


def _ensure_columns(df: DataFrame, names: List[str]) -> DataFrame:
    cur = df
    for n in names:
        if n not in cur.columns:
            cur = cur.withColumn(n, F.lit(None))
    return cur


def apply_rules(df: DataFrame, rules_spec: Dict[str, Any], current_profile: str = None) -> DataFrame:
    """
    Applies rules in ascending priority. Later rules can overwrite earlier ones.
    If a rule has stop_after_match=true, rows matched by it won't be considered by later rules.
    """
    rules = rules_spec.get("rules", [])
    rules = sorted(rules, key=lambda r: (int(r.get("priority", 1000)), str(r.get("id", "zzz"))))
    # Make sure targets exist
    targets = set()
    for r in rules:
        for a in r.get("actions", []):
            t = a.get("set", {}).get("target")
            if t:
                targets.add(t)
    cur = _ensure_columns(df, list(targets))
    # Applied mask (for stop_after_match)
    applied_col = "__rules_applied_mask"
    cur = cur.withColumn(applied_col, F.lit(False))

    for r in rules:
        profiles = r.get("profiles")
        if profiles and current_profile and current_profile not in profiles:
            continue
        base_cond = _compile_rule_expr(r)
        # effective condition (respect stop_after_match)
        eff = F.when(F.col(applied_col), F.lit(False)).otherwise(base_cond)
        actions = r.get("actions", [])
        for act in actions:
            s = act.get("set", {})
            if not s:
                continue
            target = s["target"]
            value = s["value"]
            litval = F.lit(value)
            if isinstance(value, bool):
                litval = F.lit(bool(value))
            cur = cur.withColumn(target, F.when(eff, litval).otherwise(F.col(target)))
        if r.get("stop_after_match", False):
            cur = cur.withColumn(
                applied_col, F.when(eff, F.lit(True)).otherwise(F.col(applied_col))
            )

    if applied_col in cur.columns:
        cur = cur.drop(applied_col)
    return cur
