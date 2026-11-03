from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Callsign classification. Order matters: the airline pattern is checked
# first, then country registration prefixes, everything else is unknown.
CALLSIGN_TYPE = """
    CASE
        WHEN callsign RLIKE '^[A-Z]{3}[0-9][0-9A-Z]{0,3}$' THEN 'airline'
        WHEN callsign RLIKE '^N[0-9][0-9A-Z]{0,4}$' THEN 'us_registration'
        WHEN callsign RLIKE '^C-?[FGI][A-Z0-9]{3,4}$' THEN 'ca_registration'
        WHEN callsign RLIKE '^G-[A-Z]{4}$' THEN 'gb_registration'
        WHEN callsign RLIKE '^D-[A-Z0-9]{4}$' THEN 'de_registration'
        WHEN callsign RLIKE '^F-[GH][A-Z0-9]{3}$' THEN 'fr_registration'
        WHEN callsign RLIKE '^VH-[A-Z]{3,4}$' THEN 'au_registration'
        WHEN callsign RLIKE '^PH-[A-Z]{3,4}$' THEN 'nl_registration'
        WHEN callsign RLIKE '^EI-[A-Z]{3,4}$' THEN 'ie_registration'
        WHEN callsign RLIKE '^EC-[A-Z]{3,4}$' THEN 'es_registration'
        WHEN callsign RLIKE '^I-[A-Z]{3,4}$' THEN 'it_registration'
        WHEN callsign RLIKE '^HB-[A-Z]{3,4}$' THEN 'ch_registration'
        WHEN callsign RLIKE '^OE-[A-Z]{3,4}$' THEN 'at_registration'
        WHEN callsign RLIKE '^P[PT]-[A-Z]{3,4}$' THEN 'br_registration'
        WHEN callsign RLIKE '^JA[0-9][0-9A-Z]{3,4}$' THEN 'jp_registration'
        WHEN callsign RLIKE '^TI-?[A-Z0-9]{3,4}$' THEN 'cr_registration'
        WHEN callsign RLIKE '^YV[0-9]{2,4}[A-Z]?$' THEN 've_registration'
        ELSE 'unknown'
    END
"""

@dp.table(
    schema="""
        callsign STRING PRIMARY KEY,
        callsign_type STRING,
        operator_icao STRING,
        flight_number STRING,
        operator_name STRING,
        is_cargo BOOLEAN,
        FOREIGN KEY (operator_icao) REFERENCES ref_operator (operator_icao)
    """
)
# warn-level flag: airline callsigns whose operator is not in ref_operator
# (assert-good form: no operator extracted, or the operator was resolved)
@dp.expect("dq_unmatched_operator", "operator_icao IS NULL OR operator_name IS NOT NULL")
def silver_callsign_parsed () :
    df = spark.read.table("flights_valid")
    # normalize like the other silver tables: trim + uppercase, and treat
    # empty strings and the literal 'null' as missing
    cleaned = when(trim(col("callsign")).isin("", "null"), lit(None)).otherwise(upper(trim(col("callsign"))))
    base = (
        df.select(cleaned.alias("callsign"))
          .filter(col("callsign").isNotNull())
          .distinct()
          .withColumn("callsign_type", expr(CALLSIGN_TYPE))
          # operator and flight number only make sense for airline callsigns
          .withColumn("operator_icao", when(col("callsign_type") == "airline", expr("substring(callsign, 1, 3)")))
          .withColumn("flight_number", when(col("callsign_type") == "airline", expr("substring(callsign, 4)")))
    )
    # enrich with the operator referential; unmatched operators keep their
    # callsign row with NULL name/is_cargo and are flagged by the expectation
    ref = spark.read.table("ref_operator").select(
        col("operator_icao").alias("ref_icao"),
        col("name").alias("operator_name"),
        col("is_cargo"),
    )
    return base.join(ref, base["operator_icao"] == col("ref_icao"), "left").drop("ref_icao")