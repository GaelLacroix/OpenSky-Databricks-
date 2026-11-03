from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

# an alert is raised when a position reports an emergency squawk or has
# the special position indicator (spi) flag set; the table keeps the LATEST
# alerting position per plane, so an episode stays visible even after the
# aircraft returned to normal
EMERGENCY_SQUAWKS = ["7500", "7600", "7700"]

@dp.table(name="gold_alerts")
def gold_alerts ():
    ssv = spark.read.table("silver_state_vector").select(
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "squawk", "squawk_meaning", "spi",
        "latitude", "longitude", "geo_altitude", "velocity_kt",
    )
    alerting = (
        ssv.filter(
            col("squawk").isin(EMERGENCY_SQUAWKS)
            | coalesce(col("spi"), lit(False))
        )
        .withColumn("_rn", row_number().over(
            Window.partitionBy("icao24").orderBy(col("time_position").desc(), col("last_contact").desc())
        ))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .withColumn(
            "alert_type",
            when(
                col("squawk").isin(EMERGENCY_SQUAWKS) & coalesce(col("spi"), lit(False)),
                "emergency_squawk_and_spi",
            )
            .when(col("squawk").isin(EMERGENCY_SQUAWKS), "emergency_squawk")
            .otherwise("spi"),
        )
    )
    # operator context from the referential via the callsign dimension;
    # planes whose callsign has no operator stay under 'unknown'
    csp = spark.read.table("silver_callsign_parsed").select(
        "callsign", "operator_icao", "operator_name",
    )
    # location context: nearest seeded airport (haversine)
    airports = spark.read.table("ref_airport").select(
        "airport_code", "name",
        col("longitude").alias("airport_lon"),
        col("latitude").alias("airport_lat"),
    )
    nearest = (
        alerting.select("icao24", "latitude", "longitude")
        .crossJoin(airports)
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
        .select(
            "icao24",
            col("airport_code").alias("nearest_airport_code"),
            col("name").alias("nearest_airport_name"),
        )
    )
    return (
        alerting.join(csp, "callsign", "left")
        .join(nearest, "icao24", "left")
        .select(
            "icao24", "callsign",
            coalesce(col("operator_icao"), lit("unknown")).alias("operator_icao"),
            coalesce(col("operator_name"), lit("unknown")).alias("operator_name"),
            "origin_country", "alert_type",
            "squawk", "squawk_meaning", "spi",
            "time_position", "latitude", "longitude",
            "geo_altitude", "velocity_kt",
            "nearest_airport_code", "nearest_airport_name",
        )
    )
