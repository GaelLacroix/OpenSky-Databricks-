# Shared hard/soft data-quality rule definitions for the flight validation
# gate (flights_valid.py) and the monitoring KPIs (gold_data_quality_kpis.py).
# This module is deliberately free of dataset definitions so that both
# files can import it safely.

# Each entry is (rule_name, SQL predicate) that a raw flight record must
# satisfy to enter silver. Missing values (NULL position/velocity) are
# tolerated: only impossible or unusable values are quarantined.
RULES = [
    ("icao24_present", "icao24 != 'null'"),
    ("icao24_format", "icao24 RLIKE '^[0-9a-f]{6}$'"),
    ("longitude_range", "longitude IS NULL OR longitude BETWEEN -180 AND 180"),
    ("latitude_range", "latitude IS NULL OR latitude BETWEEN -90 AND 90"),
    ("velocity_nonneg", "velocity IS NULL OR velocity >= 0"),
    ("true_track_range", "true_track IS NULL OR true_track BETWEEN 0 AND 360"),
    ("position_before_contact", "time_position <= last_contact"),
    ("contact_before_ingest", "last_contact <= time_ingest"),
    ("squawk_format", "squawk is null OR squawk RLIKE '^[0-7]{4}$'"),
]

VALID_ROW = " AND ".join(f"({predicate})" for _, predicate in RULES)

# Soft quality checks: rows are NOT rejected or quarantined, but each
# violation is counted and reported under the rule's name in the
# pipeline's data-quality metrics.
WARN_RULES = [
    ("dq_negative_altitude", "geo_altitude < 0 OR baro_altitude < 0"),
    ("dq_ground_inconsistent", "(on_ground = true AND geo_altitude > 100) OR (on_ground = false AND velocity < 10)"),
    ("dq_alt_mismatch", "ABS(geo_altitude - baro_altitude) > 1500"),
    ("dq_missing_kinematics", "on_ground = false AND (velocity is null OR vertical_rate is null)"),
    # implausible values, one rule per condition (thresholds: 15000 m is
    # above any airliner's service ceiling, 350 m/s ~ 680 kt is faster than
    # any civil aircraft, 100 m/s ~ 19700 fpm of vertical rate is extreme)
    ("dq_implausible_altitude", "geo_altitude > 15000"),
    ("dq_implausible_speed", "velocity > 350"),
    ("dq_implausible_vertical_rate", "ABS(vertical_rate) > 100"),
    ("dq_stale_position", "unix_timestamp(last_contact) - unix_timestamp(time_position) > 60"),
]
