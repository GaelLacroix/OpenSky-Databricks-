from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

@dp.table
# warn-level flag: rows carrying a squawk that is not in ref_squawk
@dp.expect("dq_unmatched_squawk", "squawk IS NULL OR squawk_meaning IS NOT NULL")
def silver_state_vector ():
    df = spark.read.table("flights_valid")
    w = Window.partitionBy("icao24","time_position").orderBy(col("time_ingest").desc())
    df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    df = df.drop("sensors", "category")
    df = df.select(
        [
            when(col(f.name) == "null", lit(None)).otherwise(col(f.name)).alias(f.name)
            if f.dataType.typeName() == "string"
            else col(f.name)
            for f in df.schema.fields
        ]
    )
    df = df.withColumn("geo_altitude_ft", col("geo_altitude") / 0.3048 )
    df = df.withColumn("baro_altitude_ft", col("baro_altitude") / 0.3048 )
    df = df.withColumn("flight_level", round(col("baro_altitude") / 0.3048 / 100))
    df = df.withColumn("velocity_kt", col("velocity") * 3600 / 1852 )
    df = df.withColumn("velocity_kmh", col("velocity") * 3.6 )
    df = df.withColumn("vertical_rate_fpm", col("vertical_rate")* 60 / 0.3048)
    df = df.withColumn("position_age_s", unix_timestamp(col("last_contact")) - unix_timestamp(col("time_position")))
    df = df.withColumn("callsign",
        when(
            trim(upper(col("callsign"))).isin(
                "", "00000000"
            ),
            lit(None)
        ).otherwise(trim(upper(col("callsign"))))
    )
    df = df.withColumn("time_position", col("time_position").cast("timestamp"))
    df = df.withColumn("last_contact", col("last_contact").cast("timestamp"))
    # attach the squawk referential; unmatched codes keep their row with a
    # NULL meaning and are flagged by the dq_unmatched_squawk expectation
    ref = spark.read.table("ref_squawk").select(
        col("squawk_code").alias("ref_code"),
        col("meaning").alias("squawk_meaning"),
    )
    return df.join(ref, df["squawk"] == col("ref_code"), "left").drop("ref_code")
