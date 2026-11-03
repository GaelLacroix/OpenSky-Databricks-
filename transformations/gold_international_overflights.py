from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

# a plane overflies the country of the nearest seeded airport when it is
# within OVERFLIGHT_RADIUS_KM of it; planes far from every seeded airport
# have an unknown overflown country and are not signaled. The aircraft's
# own country is origin_country (derived by OpenSky from the ICAO24
# registration).
OVERFLIGHT_RADIUS_KM = 100

@dp.table(name="gold_international_overflights")
def gold_international_overflights ():
    ssv = spark.read.table("silver_state_vector").select(
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "on_ground", "latitude", "longitude",
    )
    # one row per plane: its latest known position, kept if flying and
    # carrying usable coordinates
    flying = (
        ssv.withColumn("_rn", row_number().over(
            Window.partitionBy("icao24").orderBy(col("time_position").desc(), col("last_contact").desc())
        ))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .filter(~coalesce(col("on_ground"), lit(False)))
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
    )
    airports = spark.read.table("ref_airport").select(
        "country",
        col("longitude").alias("airport_lon"),
        col("latitude").alias("airport_lat"),
    )
    # haversine distance to every seeded airport, keep the closest one
    # when it is close enough to anchor the overflown country
    over = (
        flying.crossJoin(airports)
        .withColumn("airport_distance_km", expr(
            "6371 * 2 * asin(sqrt("
            "  pow(sin(radians(latitude - airport_lat) / 2), 2) +"
            "  cos(radians(airport_lat)) * cos(radians(latitude)) *"
            "  pow(sin(radians(longitude - airport_lon) / 2), 2)))"
        ))
        .withColumn("_ar", row_number().over(
            Window.partitionBy("icao24").orderBy(col("airport_distance_km").asc())
        ))
        .filter(col("_ar") == 1)
        .drop("_ar")
        .filter(col("airport_distance_km") <= OVERFLIGHT_RADIUS_KM)
        .filter(col("origin_country") != col("country"))
    )
    # operator from the referential via the callsign dimension; planes whose
    # callsign has no operator stay under 'unknown'
    csp = spark.read.table("silver_callsign_parsed").select(
        "callsign", "operator_icao", "operator_name",
    )
    per_plane = over.join(csp, "callsign", "left")
    return (
        per_plane.groupBy(
            col("country").alias("overflown_country"),
            col("origin_country").alias("aircraft_country"),
            coalesce(col("operator_icao"), lit("unknown")).alias("operator_icao"),
            coalesce(col("operator_name"), lit("unknown")).alias("operator_name"),
        ).agg(
            count("icao24").alias("planes"),
        )
    )
