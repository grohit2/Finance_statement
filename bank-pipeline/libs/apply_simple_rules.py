from typing import Dict, Any, List
from pyspark.sql import DataFrame, functions as F


def apply_rules(df: DataFrame, rules_spec: Dict[str, Any]) -> DataFrame:
    rules: List[Dict[str, Any]] = rules_spec.get("rules", [])
    cur = df
    for col, typ in [("category", "string"), ("subcategory", "string"), ("applied_rules", "array<string>"), ("operation", "string")]:
        if col not in cur.columns:
            cur = cur.withColumn(col, F.lit(None).cast(typ))

    for idx, r in enumerate(rules):
        rid = r.get("id") or f"rule_{idx}"
        col = r.get("column") or "name"
        pat = r["value"]
        cat = r.get("category")
        sub = r.get("subcategory")

        cond = F.col(col).cast("string").rlike(pat)

        # Track applied rule ids (unique)
        cur = cur.withColumn(
            "applied_rules",
            F.when(
                cond,
                F.when(F.col("applied_rules").isNull(), F.array(F.lit(rid)))
                .otherwise(F.array_union(F.col("applied_rules"), F.array(F.lit(rid))))
            ).otherwise(F.col("applied_rules"))
        )

        # Stable flag before mutating category, so subcategory/operation use the same first-set decision
        cur = cur.withColumn("__first_set__", (cond & F.col("category").isNull()).cast("boolean"))

        if cat is not None:
            cur = cur.withColumn("category", F.when(F.col("__first_set__"), F.lit(cat)).otherwise(F.col("category")))
        if sub is not None:
            cur = cur.withColumn("subcategory", F.when(F.col("__first_set__"), F.lit(sub)).otherwise(F.col("subcategory")))

        cur = cur.withColumn(
            "operation",
            F.when(
                F.col("__first_set__") & F.col("operation").isNull(),
                F.lit(f"set_category:{rid}:{cat}{('/'+sub) if sub else ''}")
            ).otherwise(F.col("operation"))
        ).drop("__first_set__")

        # Backfill missing subcategory/operation if category already set previously
        if sub is not None and cat is not None:
            backfill_cond = cond & F.col("subcategory").isNull() & (F.col("category").isNull() | (F.col("category") == F.lit(cat)))
            cur = cur.withColumn("subcategory", F.when(backfill_cond, F.lit(sub)).otherwise(F.col("subcategory")))
            cur = cur.withColumn(
                "operation",
                F.when(F.col("operation").isNull() & backfill_cond, F.lit(f"set_category:{rid}:{cat}{('/'+sub) if sub else ''}")).otherwise(F.col("operation"))
            )

    return cur
