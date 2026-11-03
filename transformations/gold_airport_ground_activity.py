from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

# a plane is counted at an airport if it is currently on the ground and its
# latest position is within GROUND_RADIUS_KM of that airport (the closest
# seeded airport wins, so a plane is never counted twice)
GROUND_RADIUS_KM = 5

@dp.table(name="gold_airport_ground_activity")
def gold_airport_ground_activity ():
    ssv = spark.read.table("silver_state_vector").select(
        "icao24", "time_position", "last_contact",
        "on_ground", "longitude", "latitude",
    )
    # one row per plane: its latest known position, kept if on ground
    on_ground = (
        ssv.withColumn("_rn", row_number().over(
            Window.partitionBy("icao24").orderBy(col("time_position").desc(), col("last_contact").desc())
        ))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .filter(col("on_ground"))
    )
    airports = spark.read.table("ref_airport").select(
        "airport_code", "name",
        col("longitude").alias("airport_lon"),
        col("latitude").alias("airport_lat"),
    )
    # haversine distance to every seeded airport, then keep the closest one
    distances = (
        on_ground.crossJoin(airports)
        .withColumn("airport_distance_km", expr(
            "6371 * 2 * asin(sqrt("
            "  pow(sin(radians(latitude - airport_lat) / 2), 2) +"
            "  cos(radians(airport_lat)) * cos(radians(latitude)) *"
            "  pow(sin(radians(longitude - airport_lon) / 2), 2)))"
        ))
    )
    at_airport = (
        distances.withColumn("_ar", row_number().over(
            Window.partitionBy("icao24").orderBy(col("airport_distance_km").asc())
        ))
        .filter(col("_ar") == 1)
        .drop("_ar")
        .filter(col("airport_distance_km") <= GROUND_RADIUS_KM)
    )
    # planes on the ground per airport; seeded airports without ground
    # traffic near them stay in the table with a count of 0
    counts = at_airport.groupBy("airport_code").agg(
        count("icao24").alias("planes_on_ground"),
    )
    return (
        airports.join(counts, "airport_code", "left")
        .select(
            "airport_code",
            col("name").alias("airport_name"),
            col("airport_lon").alias("longitude"),
            col("airport_lat").alias("latitude"),
            coalesce(col("planes_on_ground"), lit(0)).alias("planes_on_ground"),
        )
    )
