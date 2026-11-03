from pyspark import pipelines as dp

# Seed referential of airports with public coordinates. MEM and SDF are
# the FedEx and UPS hubs respectively, matching the operator referential.
# Covers the ~30 busiest US airports plus the cargo hubs and Toronto.
# Extend this list as the referential grows.
AIRPORTS = [
    ("ATL", "Hartsfield-Jackson Atlanta International Airport", -84.4277, 33.6407, "United States"),
    ("DFW", "Dallas/Fort Worth International Airport", -97.0403, 32.8998, "United States"),
    ("DEN", "Denver International Airport", -104.6737, 39.8561, "United States"),
    ("ORD", "Chicago O'Hare International Airport", -87.9073, 41.9742, "United States"),
    ("LAX", "Los Angeles International Airport", -118.4085, 33.9416, "United States"),
    ("CLT", "Charlotte Douglas International Airport", -80.9473, 35.2144, "United States"),
    ("MEM", "Memphis International Airport (FedEx hub)", -89.9767, 35.0424, "United States"),
    ("SDF", "Louisville Muhammad Ali International Airport (UPS hub)", -85.7364, 38.1811, "United States"),
    ("JFK", "John F. Kennedy International Airport", -73.7781, 40.6413, "United States"),
    ("YYZ", "Toronto Pearson International Airport", -79.6248, 43.6777, "Canada"),
    ("LAS", "Harry Reid International Airport", -115.1537, 36.0840, "United States"),
    ("PHX", "Phoenix Sky Harbor International Airport", -112.0101, 33.4352, "United States"),
    ("MCO", "Orlando International Airport", -81.3081, 28.4312, "United States"),
    ("SEA", "Seattle-Tacoma International Airport", -122.3088, 47.4502, "United States"),
    ("MIA", "Miami International Airport", -80.2870, 25.7959, "United States"),
    ("IAH", "George Bush Intercontinental Airport", -95.3368, 29.9902, "United States"),
    ("EWR", "Newark Liberty International Airport", -74.1745, 40.6895, "United States"),
    ("SFO", "San Francisco International Airport", -122.3790, 37.6213, "United States"),
    ("DTW", "Detroit Metropolitan Wayne County Airport", -83.3554, 42.2162, "United States"),
    ("PHL", "Philadelphia International Airport", -75.2424, 39.8744, "United States"),
    ("BOS", "Boston Logan International Airport", -71.0096, 42.3656, "United States"),
    ("LGA", "LaGuardia Airport", -73.8740, 40.7769, "United States"),
    ("MSP", "Minneapolis-Saint Paul International Airport", -93.2222, 44.8848, "United States"),
    ("FLL", "Fort Lauderdale-Hollywood International Airport", -80.1506, 26.0742, "United States"),
    ("IAD", "Washington Dulles International Airport", -77.4565, 38.9531, "United States"),
    ("DCA", "Ronald Reagan Washington National Airport", -77.0402, 38.8512, "United States"),
    ("SLC", "Salt Lake City International Airport", -111.9791, 40.7899, "United States"),
    ("BWI", "Baltimore/Washington International Airport", -76.6683, 39.1754, "United States"),
    ("MDW", "Chicago Midway International Airport", -87.7522, 41.7868, "United States"),
    ("HNL", "Daniel K. Inouye International Airport", -157.9254, 21.3245, "United States"),
    ("TPA", "Tampa International Airport", -82.5332, 27.9755, "United States"),
    ("PDX", "Portland International Airport", -122.5975, 45.5887, "United States"),
    ("HOU", "William P. Hobby Airport", -95.2767, 29.6454, "United States"),
    ("AUS", "Austin-Bergstrom International Airport", -97.6664, 30.1975, "United States"),
    ("BNA", "Nashville International Airport", -86.6774, 36.1263, "United States"),
]

@dp.table(
    name="ref_airport",
    schema="""
        airport_code STRING PRIMARY KEY,
        name STRING,
        longitude DOUBLE,
        latitude DOUBLE,
        country STRING
    """
)
def ref_airport_seed ():
    return spark.createDataFrame(AIRPORTS, ["airport_code", "name", "longitude", "latitude", "country"])
