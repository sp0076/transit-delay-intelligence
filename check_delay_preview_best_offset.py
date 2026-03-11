#!/usr/bin/env python3
"""
Check delay preview quality metrics.
Validates the delay distribution using best-offset alignment.
"""

import duckdb
from dotenv import load_dotenv
import os

load_dotenv()
db_path = os.getenv("DB_PATH", "./data/transit_delay.duckdb")
conn = duckdb.connect(db_path)

print("DELAY PREVIEW QUALITY CHECK (BEST-OFFSET ALIGNMENT)")
print("=" * 60)

# Total statistics
stats = conn.execute("""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT trip_id) as distinct_trips,
        COUNT(DISTINCT route_id) as distinct_routes,
        MIN(delay_minutes) as min_delay,
        MAX(delay_minutes) as max_delay,
        AVG(delay_minutes) as avg_delay,
        MEDIAN(delay_minutes) as median_delay
    FROM staging.delay_preview_best_offset
""").fetchall()[0]

print(f"\nOverall Statistics:")
print(f"  Total rows: {stats[0]}")
print(f"  Distinct trips: {stats[1]}")
print(f"  Distinct routes: {stats[2]}")
print(f"  Min delay (min): {stats[3]}")
print(f"  Max delay (min): {stats[4]}")
print(f"  Avg delay (min): {stats[5]:.2f}")
print(f"  Median delay (min): {stats[6]:.2f}")

# Outliers
outliers = conn.execute("""
    SELECT
        COUNT(CASE WHEN delay_minutes > 60 THEN 1 END) as over_60_min,
        COUNT(CASE WHEN delay_minutes < -10 THEN 1 END) as early_by_10_min
    FROM staging.delay_preview_best_offset
""").fetchall()[0]

print(f"\nOutliers:")
print(f"  Rows with delay > 60 min: {outliers[0]}")
print(f"  Rows with early arrival > 10 min: {outliers[1]}")

# Delay distribution
distribution = conn.execute("""
    SELECT
        CASE
            WHEN delay_minutes < -10 THEN 'Early >10min'
            WHEN delay_minutes < 0 THEN 'Early 0-10min'
            WHEN delay_minutes < 5 THEN 'On-time (0-5min)'
            WHEN delay_minutes < 15 THEN 'Minor delay (5-15min)'
            WHEN delay_minutes < 30 THEN 'Moderate (15-30min)'
            WHEN delay_minutes < 60 THEN 'Major (30-60min)'
            ELSE 'Severe >60min'
        END as delay_category,
        COUNT(*) as count
    FROM staging.delay_preview_best_offset
    GROUP BY delay_category
    ORDER BY delay_category
""").fetchall()

print(f"\nDelay Distribution:")
for row in distribution:
    print(f"  {row[0]}: {row[1]} rows")

# Sample worst delays
print(f"\nWorst delays (top 10):")
worst = conn.execute("""
    SELECT trip_id, route_id, stop_name, delay_minutes, scheduled_arrival_local_ts, actual_arrival_local_ts
    FROM staging.delay_preview_best_offset
    ORDER BY delay_minutes DESC
    LIMIT 10
""").fetchall()

for i, row in enumerate(worst, 1):
    print(f"  {i}. Trip {row[0]} (Route {row[1]}): {row[2]} → {row[3]:.0f} min delay")
    print(f"     Scheduled: {row[4]}, Actual: {row[5]}")

# Sample best (earliest) experience
print(f"\nBest experience (earliest, top 10):")
best = conn.execute("""
    SELECT trip_id, route_id, stop_name, delay_minutes, scheduled_arrival_local_ts, actual_arrival_local_ts
    FROM staging.delay_preview_best_offset
    ORDER BY delay_minutes ASC
    LIMIT 10
""").fetchall()

for i, row in enumerate(best, 1):
    print(f"  {i}. Trip {row[0]} (Route {row[1]}): {row[2]} → {row[3]:.0f} min")
    print(f"     Scheduled: {row[4]}, Actual: {row[5]}")

print("\n" + "=" * 60)
print("Check complete.")

conn.close()
