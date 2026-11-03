from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

@dp.table(name="gold_traffic_by_operator")
def gold_traffic_by_operator ():
    ssv = spark.read.table("silver_state_vector").select(
        "callsign", "icao24", "on_ground", "velocity_kt", "geo_altitude"
    )
    csp = spark.read.table("silver_callsign_parsed").select(
        "callsign", "operator_icao", "operator_name", "is_cargo"
    )
    # only callsigns resolved to a seeded operator contribute
    df = ssv.join(csp, "callsign").filter(col("operator_name").isNotNull())
    result = df.groupBy("operator_icao", "operator_name", "is_cargo").agg(
        expr("count(distinct icao24)").alias("planes_total"),
        expr("count(distinct case when on_ground then icao24 end)").alias("planes_on_ground"),
        expr("count(distinct case when not coalesce(on_ground, false) then icao24 end)").alias("planes_in_air"),
        # speed and altitude from airborne positions only (ground would drag
        # the averages down); NULL positions are ignored by avg/median
        expr("avg(case when not coalesce(on_ground, false) then velocity_kt end)").alias("avg_speed_kt"),
        expr("median(case when not coalesce(on_ground, false) then geo_altitude end)").alias("median_altitude_m"),
    )
    # cargo/passenger ratio is a global metric (each operator is entirely
    # one or the other), so it is computed once and repeated on every row
    w = Window.partitionBy()
    cargo_planes = sum(when(col("is_cargo"), col("planes_total")).otherwise(lit(0))).over(w)
    passenger_planes = sum(when(~col("is_cargo"), col("planes_total")).otherwise(lit(0))).over(w)
    return result.withColumn("cargo_passenger_ratio", cargo_planes / passenger_planes)
