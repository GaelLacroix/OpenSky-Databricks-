from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

# Flight phase rules, evaluated in order (first match wins):
#   ground:             on_ground
#   cruise:             altitude > 7000 m and |vertical_rate| < 1 m/s
#   approach_departure: altitude < 3000 m and within 30 km of a seeded
#                       airport (terminal phase, any vertical rate)
#   climb / descent:    |vertical_rate| >= 1 m/s (the 3000-7000 m band and
#                       high-altitude climbs/descents)
#   unknown:            level flight that is not cruise (e.g. holding),
#                       including low-altitude level flight far from seeded
#                       airports and rows with missing altitude/vertical rate
FLIGHT_PHASE = """
    CASE
        WHEN on_ground THEN 'ground'
        WHEN geo_altitude > 7000 AND abs(vertical_rate) < 1 THEN 'cruise'
        WHEN geo_altitude < 3000 AND nearest_airport_km <= 30 THEN 'approach_departure'
        WHEN vertical_rate >= 1 THEN 'climb'
        WHEN vertical_rate <= -1 THEN 'descent'
        ELSE 'unknown'
    END
"""

@dp.table(name="gold_flight_phase_distribution")
def gold_flight_phase_distribution ():
    ssv = spark.read.table("silver_state_vector").select(
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "on_ground", "geo_altitude", "vertical_rate", "longitude", "latitude",
    )
    # one row per plane: its latest known position
    latest = (
        ssv.withColumn("_rn", row_number().over(
            Window.partitionBy("icao24").orderBy(col("time_position").desc(), col("last_contact").desc())
        ))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    # distance (haversine) to the closest seeded airport, so that the
    # approach/departure phase only applies to planes near an airport
    airports = spark.read.table("ref_airport").select(
        col("longitude").alias("airport_lon"),
        col("latitude").alias("airport_lat"),
    )
    nearest = (
        latest.select("icao24", "longitude", "latitude")
        .crossJoin(airports)
        .withColumn("airport_distance_km", expr(
            "6371 * 2 * asin(sqrt("
            "  pow(sin(radians(latitude - airport_lat) / 2), 2) +"
            "  cos(radians(airport_lat)) * cos(radians(latitude)) *"
            "  pow(sin(radians(longitude - airport_lon) / 2), 2)))"
        ))
        .groupBy("icao24")
        .agg(min("airport_distance_km").alias("nearest_airport_km"))
    )
    latest = (
        latest.join(nearest, "icao24")
        .withColumn("flight_phase", expr(FLIGHT_PHASE))
    )
    # operator from the referential via the callsign dimension; planes whose
    # callsign has no operator stay under 'unknown'
    csp = spark.read.table("silver_callsign_parsed").select(
        "callsign", "operator_icao", "operator_name",
    )
    per_plane = latest.join(csp, "callsign", "left")
    return (
        per_plane.groupBy(
            col("origin_country"),
            coalesce(col("operator_icao"), lit("unknown")).alias("operator_icao"),
            coalesce(col("operator_name"), lit("unknown")).alias("operator_name"),
        ).agg(
            count("icao24").alias("planes_total"),
            expr("count(case when flight_phase = 'ground' then 1 end)").alias("planes_ground"),
            expr("count(case when flight_phase = 'approach_departure' then 1 end)").alias("planes_approach_departure"),
            expr("count(case when flight_phase = 'climb' then 1 end)").alias("planes_climb"),
            expr("count(case when flight_phase = 'descent' then 1 end)").alias("planes_descent"),
            expr("count(case when flight_phase = 'cruise' then 1 end)").alias("planes_cruise"),
            expr("count(case when flight_phase = 'unknown' then 1 end)").alias("planes_unknown"),
        )
    )
