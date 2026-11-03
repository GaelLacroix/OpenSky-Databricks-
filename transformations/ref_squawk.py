from pyspark import pipelines as dp

# Seed referential of transponder (squawk) codes and their meaning.
# Extend this list as the referential grows.
SQUAWKS = [
    ("7500", "Unlawful interference (hijacking)"),
    ("7600", "Radio communication failure"),
    ("7700", "General emergency"),
    ("1200", "Visual flight rules, US standard"),
    ("7000", "Visual flight rules, ICAO standard"),
    ("1000", "Instrument flight rules, Europe default"),
    ("2000", "Instrument flight rules, default code"),
    ("0033", "Parachute drop operation"),
    ("0000", "Military operation, US"),
]

@dp.table(
    name="ref_squawk",
    schema="""
        squawk_code STRING PRIMARY KEY,
        meaning STRING
    """
)
def ref_squawk_seed ():
    return spark.createDataFrame(SQUAWKS, ["squawk_code", "meaning"])
