from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    schema="""
        icao24 STRING PRIMARY KEY,
        origin_country STRING,
        first_seen TIMESTAMP,
        last_seen TIMESTAMP,
        last_known_callsign STRING
    """
)
@dp.expect_or_fail("valid_seen_window", "first_seen <= last_seen")
def silver_dim_aircraft ():
    df = spark.read.table("flights_valid")
    return df.groupBy("icao24").agg(
        max("origin_country").alias("origin_country"),
        min("time_ingest").alias("first_seen"),
        max("time_ingest").alias("last_seen"),
        # last usable callsign (trimmed; '' and 'null' treated as missing),
        # taken from the newest row — written as SQL because NULLIF/MAX_BY
        # are SQL functions, not reliable Python ones here
        expr("max_by(nullif(nullif(trim(callsign), ''), 'null'), time_ingest)").alias("last_known_callsign"),
    )
    