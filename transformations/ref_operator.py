from pyspark import pipelines as dp

# Seed reference data for the exercise: cargo operators first, then
# passenger operators. Extend this list as the referential grows.
OPERATORS = [
    ("UPS", "United Parcel Service", True),
    ("FDX", "FedEx Express", True),
    ("ABX", "ABX Air", True),
    ("ATN", "Air Transport International", True),
    ("GTI", "Grandstar Cargo International", True),
    ("CKS", "Kalitta Charters", True),
    ("DAL", "Delta Air Lines", False),
    ("AAL", "American Airlines", False),
    ("UAL", "United Airlines", False),
    ("SWA", "Southwest Airlines", False),
    ("JBU", "JetBlue Airways", False),
    ("ACA", "Air Canada", False),
    ("WJA", "WestJet", False),
]

@dp.table(
    name="ref_operator",
    schema="""
        operator_icao STRING PRIMARY KEY,
        name STRING,
        is_cargo BOOLEAN
    """
)
def ref_operator_seed ():
    return spark.createDataFrame(OPERATORS, ["operator_icao", "name", "is_cargo"])
