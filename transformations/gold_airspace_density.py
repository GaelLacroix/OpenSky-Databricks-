from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import Window

# vertical slices of the airspace, in meters of geometric altitude
ALTITUDE_BAND = """
    CASE
        WHEN geo_altitude IS NULL THEN 'unknown'
        WHEN geo_altitude < 3000 THEN 'below_3000m'
        WHEN geo_altitude < 6000 THEN '3000_6000m'
        WHEN geo_altitude < 9000 THEN '6000_9000m'
        WHEN geo_altitude < 12000 THEN '9000_12000m'
        ELSE 'above_12000m'
    END
"""

@dp.table(name="gold_airspace_density")
def gold_airspace_density ():
    ssv = spark.read.table("silver_state_vector").select(
        "icao24", "time_position", "last_contact",
        "on_ground", "geo_altitude", "latitude", "longitude",
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
    # 1x1 degree grid cells (FLOOR of the coordinates) crossed with the
    # altitude bands; cell centers are handy for map-style visualizations
    grid = flying.select(
        floor(col("latitude")).alias("grid_lat"),
        floor(col("longitude")).alias("grid_lon"),
        (floor(col("latitude")) + 0.5).alias("cell_center_lat"),
        (floor(col("longitude")) + 0.5).alias("cell_center_lon"),
        expr(ALTITUDE_BAND).alias("altitude_band"),
    )
    return (
        grid.groupBy("grid_lat", "grid_lon", "cell_center_lat", "cell_center_lon", "altitude_band")
        .agg(count(lit(1)).alias("planes"))
    )
