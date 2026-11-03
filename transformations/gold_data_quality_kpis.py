from pyspark import pipelines as dp
from pyspark.sql.functions import *

# exact same predicates as the gate, so the KPIs can never drift from it
from dq_rules import RULES, WARN_RULES

# Long-format monitoring table. Every row is (kpi_type, dataset, kpi_name,
# evaluated_rows, flagged_rows, ratio) where ratio is uniformly the "bad"
# share of rows (reject share, warn-flag share, or missing-value share —
# for column_completion the completion rate is 1 - ratio).
@dp.table(name="gold_data_quality_kpis")
def gold_data_quality_kpis ():
    valid = spark.read.table("flights_valid")
    quarantine = spark.read.table("flights_quarantine")

    # ---- reject ratio per hard gate rule ---------------------------------
    # denominator: every row the gate evaluated (valid + quarantined)
    totals = (
        valid.agg(count(lit(1)).alias("valid_rows"))
        .crossJoin(quarantine.agg(count(lit(1)).alias("rejected_rows")))
        .select((col("valid_rows") + col("rejected_rows")).alias("evaluated_rows"))
    )
    # a quarantined row can fail several rules, so split the comma-joined
    # rule_name; rules with no rejects appear thanks to the RULES seed
    rules_df = spark.createDataFrame([(name,) for name, _ in RULES], "kpi_name STRING")
    per_rule = (
        quarantine.select(explode(split(col("rule_name"), ", ")).alias("kpi_name"))
        .groupBy("kpi_name")
        .agg(count(lit(1)).alias("flagged_rows"))
    )
    reject_kpis = (
        rules_df.join(per_rule, "kpi_name", "left")
        .crossJoin(totals)
        .select(
            lit("reject_rule").alias("kpi_type"),
            lit("flights_gate").alias("dataset"),
            "kpi_name", "evaluated_rows",
            coalesce(col("flagged_rows"), lit(0)).cast("long").alias("flagged_rows"),
            (coalesce(col("flagged_rows"), lit(0)) / col("evaluated_rows")).alias("ratio"),
        )
    )

    # ---- warn-flag ratios (dq_*) ------------------------------------------
    # WARN_RULES predicates are detection-style (true when the row looks
    # bad); the COALESCE mirrors how the expectations treat NULL predicates
    # (NULL evaluation counts as a pass, not a violation)
    fv = spark.read.table("flights_valid")
    warn_counts = fv.select(
        count(lit(1)).alias("evaluated_rows"),
        *[
            sum(when(expr(f"coalesce(({predicate}), false)"), lit(1)).otherwise(lit(0))).alias(name)
            for name, predicate in WARN_RULES
        ],
    )
    warn_stack = ", ".join(f"'{name}', {name}" for name, _ in WARN_RULES)
    warn_kpis = (
        warn_counts.select(
            "evaluated_rows",
            expr(f"stack({len(WARN_RULES)}, {warn_stack})").alias("kpi_name", "flagged_rows"),
        )
        .select(
            lit("dq_flag").alias("kpi_type"),
            lit("flights_valid").alias("dataset"),
            "kpi_name", "evaluated_rows", "flagged_rows",
            (col("flagged_rows") / col("evaluated_rows")).alias("ratio"),
        )
    )

    # ---- the two enrichment flags defined on the silver tables ----------
    # mirrors dq_unmatched_operator on silver_callsign_parsed
    csp = spark.read.table("silver_callsign_parsed")
    op_flag = csp.select(
        count(lit(1)).alias("evaluated_rows"),
        sum(
            when(col("operator_icao").isNotNull() & col("operator_name").isNull(), lit(1)).otherwise(lit(0))
        ).alias("flagged_rows"),
    )
    op_kpis = op_flag.select(
        lit("dq_flag").alias("kpi_type"),
        lit("silver_callsign_parsed").alias("dataset"),
        lit("dq_unmatched_operator").alias("kpi_name"),
        "evaluated_rows", "flagged_rows",
        (col("flagged_rows") / col("evaluated_rows")).alias("ratio"),
    )

    # mirrors dq_unmatched_squawk on silver_state_vector
    ssv = spark.read.table("silver_state_vector")
    sq_flag = ssv.select(
        count(lit(1)).alias("evaluated_rows"),
        sum(
            when(col("squawk").isNotNull() & col("squawk_meaning").isNull(), lit(1)).otherwise(lit(0))
        ).alias("flagged_rows"),
    )
    sq_kpis = sq_flag.select(
        lit("dq_flag").alias("kpi_type"),
        lit("silver_state_vector").alias("dataset"),
        lit("dq_unmatched_squawk").alias("kpi_name"),
        "evaluated_rows", "flagged_rows",
        (col("flagged_rows") / col("evaluated_rows")).alias("ratio"),
    )

    # ---- completion per column of the silver fact table ------------------
    # flagged_rows counts the rows where the column is missing, so the
    # completion rate of a column is 1 - ratio
    cols = [f.name for f in ssv.schema.fields]
    missing = ssv.select(
        count(lit(1)).alias("evaluated_rows"),
        *[(count(lit(1)) - count(col(n))).alias(n) for n in cols],
    )
    col_stack = ", ".join(f"'{n}', {n}" for n in cols)
    completion_kpis = (
        missing.select(
            "evaluated_rows",
            expr(f"stack({len(cols)}, {col_stack})").alias("kpi_name", "flagged_rows"),
        )
        .select(
            lit("column_completion").alias("kpi_type"),
            lit("silver_state_vector").alias("dataset"),
            "kpi_name", "evaluated_rows", "flagged_rows",
            (col("flagged_rows") / col("evaluated_rows")).alias("ratio"),
        )
    )

    return (
        reject_kpis
        .unionByName(warn_kpis)
        .unionByName(op_kpis)
        .unionByName(sq_kpis)
        .unionByName(completion_kpis)
    )
