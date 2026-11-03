from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

# a new flight session starts when consecutive positions of the same
# icao24 + callsign are separated by at least SESSION_GAP_MINUTES. The
# data's median position gap is ~2 minutes, so 30 minutes separates
# consecutive observations of a flight from a genuinely new one
SESSION_GAP_MINUTES = 30

@dp.table(name="gold_flight_session")
def gold_flight_session ():
    ssv = spark.read.table("silver_state_vector").select(
        "icao24", "callsign", "time_position",
        "latitude", "longitude", "geo_altitude", "velocity_kt",
    )
    # sessionize: a running counter that increments whenever the gap to
    # the previous position of the same icao24 + callsign exceeds the
    # threshold (the first position of a pair always starts a session)
    w = Window.partitionBy("icao24", "callsign").orderBy("time_position")
    df = (
        ssv.withColumn("_prev_tp", lag("time_position").over(w))
        .withColumn("_gap_s", unix_timestamp("time_position") - unix_timestamp("_prev_tp"))
        .withColumn(
            "_new_session",
            coalesce(col("_gap_s") >= SESSION_GAP_MINUTES * 60, lit(True)),
        )
        .withColumn("session_id", sum(col("_new_session").cast("int")).over(w))
    )
    # distance of each leg between consecutive positions inside a session
    w_session = Window.partitionBy("icao24", "callsign", "session_id").orderBy("time_position")
    legs = (
        df.withColumn("_prev_lat", lag("latitude").over(w_session))
        .withColumn("_prev_lon", lag("longitude").over(w_session))
        .withColumn("_leg_km", expr(
            "6371 * 2 * asin(sqrt("
            "  pow(sin(radians(latitude - _prev_lat) / 2), 2) +"
            "  cos(radians(_prev_lat)) * cos(radians(latitude)) *"
            "  pow(sin(radians(longitude - _prev_lon) / 2), 2)))"
        ))
    )
    return (
        legs.groupBy("icao24", "callsign", "session_id")
        .agg(
            min("time_position").alias("start_time"),
            max("time_position").alias("end_time"),
            (unix_timestamp(max("time_position")) - unix_timestamp(min("time_position"))).alias("duration_s"),
            round(sum("_leg_km"), 3).alias("distance_km"),
            round(max("geo_altitude"), 1).alias("max_altitude_m"),
            round(avg("velocity_kt"), 1).alias("avg_speed_kt"),
            count(lit(1)).alias("positions"),
        )
    )
