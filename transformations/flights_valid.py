from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Hard/soft rule definitions live in dq_rules.py so the monitoring KPIs in
# gold_data_quality_kpis.py can reuse the exact same predicates.
from dq_rules import RULES, VALID_ROW, WARN_RULES

def _failed_rules():
    """Column holding the name of every rule a row did not pass.
    A predicate evaluating to NULL (e.g. RLIKE on a NULL field) counts
    as a failure, matching how filter() treats NULL in flights_valid."""
    return F.array_compact(
        F.array(*[
            F.when(F.expr(f"NOT COALESCE(({predicate}), false)"), F.lit(name))
            for name, predicate in RULES
        ])
    )

# WARN_RULES are detection predicates (true when the row looks bad).
# An expectation constraint must assert the opposite (true when the row
# is fine), hence the inversion below. COALESCE makes a NULL evaluation
# (e.g. NULL altitude in the predicate) pass instead of counting as a
# violation.
@dp.table
@dp.expect_all({
    name: f"NOT COALESCE(({predicate}), false)"
    for name, predicate in WARN_RULES
})
def flights_valid ():
    return spark.read.table("ingest_flights").filter(VALID_ROW)

@dp.table
def flights_quarantine ():
    df = spark.read.table("ingest_flights").withColumn("_failed", _failed_rules())
    return (
        df.filter(F.size(F.col("_failed")) > 0)
          .withColumn("rule_name", F.array_join(F.col("_failed"), ", "))
          .withColumn("rejected_at", F.current_timestamp())
          .drop("_failed")
    )
