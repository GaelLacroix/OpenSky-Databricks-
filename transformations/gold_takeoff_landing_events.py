from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

# a transition is detected between consecutive positions of the same
# aircraft: on_ground false->true is a landing, true->false a takeoff. The
# event time is the first observation in the new state, so with sparse
# data an event can only be as precise as the observation cadence allows.
# It is attributed to the nearest seeded airport when within
# EVENT_AIRPORT_RADIUS_KM, otherwise the airport stays 'unknown'.
EVENT_AIRPORT_RADIUS_KM = 10

@dp.table(name="gold_takeoff_landing_events")
def gold_takeoff_landing_events ():
    ssv = spark.read.table("silver_state_vector").select(
        "icao24", "callsign", "time_position", "on_ground",
        "latitude", "longitude",
    )
    w = Window.partitionBy("icao24").orderBy("time_position")
    transitions = (
        ssv.withColumn("_prev_ground", lag(coalesce(col("on_ground"), lit(False))).over(w))
        .filter(col("_prev_ground").isNotNull())
        .filter(coalesce(col("on_ground"), lit(False)) != col("_prev_ground"))
        .withColumn(
            "event_type",
            when(coalesce(col("on_ground"), lit(False)), "landing").otherwise("takeoff"),
        )
        .drop("_prev_ground")
    )
    # attribute each event to the nearest seeded airport when close enough
    airports = spark.read.table("ref_airport").select(
        "airport_code", "name",
        col("longitude").alias("airport_lon"),
        col("latitude").alias("airport_lat"),
    )
    nearest = (
        transitions.crossJoin(airports)
        .withColumn("airport_distance_km", expr(
            "6371 * 2 * asin(sqrt("
            "  pow(sin(radians(latitude - airport_lat) / 2), 2) +"
            "  cos(radians(airport_lat)) * cos(radians(latitude)) *"
            "  pow(sin(radians(longitude - airport_lon) / 2), 2)))"
        ))
        .withColumn("_ar", row_number().over(
            Window.partitionBy("icao24", "time_position").orderBy(col("airport_distance_km").asc())
        ))
        .filter(col("_ar") == 1)
        .drop("_ar")
        .filter(col("airport_distance_km") <= EVENT_AIRPORT_RADIUS_KM)
        .select(
            "icao24", "time_position",
            col("airport_code").alias("_airport_code"),
            col("name").alias("_airport_name"),
        )
    )
    # operator context from the referential via the callsign dimension;
    # planes whose callsign has no operator stay under 'unknown'
    csp = spark.read.table("silver_callsign_parsed").select(
        "callsign", "operator_icao", "operator_name",
    )
    per_event = (
        transitions.join(nearest, ["icao24", "time_position"], "left")
        .join(csp, "callsign", "left")
    )
    return per_event.select(
        "icao24", "callsign", "event_type",
        col("time_position").alias("event_time"),
        coalesce(col("_airport_code"), lit("unknown")).alias("airport_code"),
        coalesce(col("_airport_name"), lit("unknown")).alias("airport_name"),
        coalesce(col("operator_icao"), lit("unknown")).alias("operator_icao"),
        coalesce(col("operator_name"), lit("unknown")).alias("operator_name"),
    )
