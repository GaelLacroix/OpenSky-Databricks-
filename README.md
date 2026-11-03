# OpenSky Live Flight Data Pipeline on Databricks

A Lakeflow Spark Declarative Pipeline that ingests **live aircraft state vectors** from the [OpenSky Network](https://opensky-network.org/) and turns them into analysis-ready tables, powering an AI/BI dashboard (**"Live sky"**) with five interactive pages: fleet overview with a live position map, operator activity, airport movements, flight trajectories, and data quality.

> The data tables produced by the pipeline live in the `workspace.opensky` schema of the author's Databricks workspace and are **not included in this repository** — only the pipeline source code is.

## Architecture

The pipeline follows a bronze → silver → gold medallion layout. All datasets are defined in `transformations/`:

### Ingestion (bronze)

- **`ingest_flights.py`** — streams live state vectors through a custom [pyspark-data-sources](https://github.com/databrickslabs/pyspark-data-sources) `OpenSkyDataSource` connector.
- **`flights_stats.py`** — quick ingestion counters (events, distinct aircraft, max velocity).

### Validation gate (bronze → silver)

- **`dq_rules.py`** — the single source of truth for data quality: hard rules (rows failing them are quarantined) and soft `dq_*` warn rules (rows pass but violations are counted). Both the gate and the KPI table import the same predicates so they can never drift.
- **`flights_valid.py`** — passes clean rows to silver; failed rows land in `flights_quarantine` with the comma-joined names of every failed rule and a rejection timestamp.

### Silver

- **`silver_state_vector.py`** — deduplicated positions, unit conversions (ft, kt, km/h, flight level), cleaning of the literal string "null" values in raw fields, and a LEFT join to the squawk referential.
- **`silver_callsign_parsed.py`** — one row per callsign: airline vs. country-registration classification via regex, operator ICAO + flight number extraction, enrichment from the operator referential.
- **`silver_dim_aircraft.py`** — one row per aircraft (`icao24` primary key) with first/last seen timestamps and last known callsign (via `max_by`).

### Gold

- **`gold_traffic_by_operator.py`** — per operator: distinct planes on ground / in air, average airborne speed, median altitude, global cargo/passenger ratio.
- **`gold_flight_phase_distribution.py`** — latest position per plane classified into ground / cruise / approach-departure (haversine proximity to a seeded airport) / climb / descent / unknown, grouped by country and operator.
- **`gold_airport_ground_activity.py`** — planes currently on the ground within 5 km of their closest seeded airport, per-airport counts (zeros included).
- **`gold_airspace_density.py`** — flying planes on a 1°×1° grid crossed with vertical altitude bands, for map-style density visualizations.
- **`gold_international_overflights.py`** — flying planes whose country differs from the country of the nearest seeded airport (within 100 km), grouped by overflown country, aircraft country and operator.
- **`gold_takeoff_landing_events.py`** — `on_ground` transitions detected with a window `lag()`, attributed to the nearest seeded airport within 10 km.
- **`gold_flight_session.py`** — flight sessionization: positions of the same `icao24` + callsign split into sessions whenever the observation gap exceeds 30 minutes, with per-session duration, haversine leg distance, max altitude and average speed.
- **`gold_data_quality_kpis.py`** — long-format monitoring table: per-rule reject ratios, warn-flag shares, enrichment-flag shares and per-column completeness of the silver fact table.

### Reference (seed) data

- **`ref_operator.py`** — 13 seeded cargo/passenger operators (FedEx, UPS, Delta, …).
- **`ref_squawk.py`** — standard transponder codes (7500/7600/7700, VFR/IFR codes, …).
- **`ref_airport.py`** — ~35 airports with public coordinates, including the MEM (FedEx) and SDF (UPS) hubs.

## Data quality highlights

- Expectations are written in **assert-good** form (`@dp.expect_all` on `flights_valid`), so detection-style predicates from `dq_rules.py` are inverted with `NOT COALESCE((predicate), false)`.
- NULL-tolerant rules: missing values are tolerated; only impossible or unusable values (bad `icao24`, out-of-range coordinates, negative velocity, impossible squawk, …) are quarantined.
- The quarantine table keeps *why* a row was rejected (`rule_name`, `rejected_at`), and `gold_data_quality_kpis` reports every rule's reject ratio — even for rules with zero rejects — alongside soft-flag and completeness KPIs.

## Stack

- Databricks Lakeflow Spark Declarative Pipelines (`pyspark.pipelines` / `@dp.table`, streaming tables + materialized views)
- [pyspark-data-sources](https://github.com/databrickslabs/pyspark-data-sources) `OpenSkyDataSource` for live ingestion
- Databricks AI/BI Genie dashboards for visualization
