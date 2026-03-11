# MVP Delay Layer Documentation

## Overview

**Table**: `staging.delay_preview_best_offset`
**Status**: MVP approximation, ready for exploration dashboard
**Rows**: 1,056 (74 trips across 10 routes)
**Coverage**: Matched GTFS-Realtime trip subset (not full network)

---

## How It Works

### The Problem
GTFS-Realtime feeds for BART don't directly join to static GTFS data:
- Realtime `stop_id` values (e.g., 'R40-1', 'M10-2') don't match numeric static stop IDs
- Realtime `stop_sequence` is always 0 (useless)
- Realtime data represents the **remaining trip tail**, not the full schedule

### The Solution: Per-Trip Offset Alignment

1. **Trip Matching**
   - Match realtime trip_id to static trip_id (74 of 84 RT trips matched)
   - Unmatched trips are excluded from final layer

2. **Dynamic Offset Search**
   - For each matched trip, compute static schedule stops and RT updates stops
   - Generate candidate sequence offsets: 0 to (static_stops - rt_stops)
   - Example: If static has 25 stops and RT has 3, try offsets 0-22

3. **Offset Scoring**
   - Align RT and static schedules at each offset using: `static_seq = rt_seq + offset`
   - Score each offset by timing error: `MEDIAN(ABS(scheduled_time - actual_time))`
   - Example: offset=22 means RT represents stops 22-25 of the full trip

4. **Best Offset Selection**
   - Per trip, select offset minimizing median absolute timing error
   - Result: 52 of 74 trips use non-zero offsets (range 1-24 stops)
   - 22 trips use offset=0 (RT captured from trip start)

5. **Final Alignment**
   - Join realtime updates and static schedule using best offset
   - Compute delay: `delay_minutes = actual_arrival_local - scheduled_arrival_local`

---

## Data Quality Notes

### What This Is
- ✅ Realistic delay range: -16 to +6 minutes (believable for metro)
- ✅ Healthy distribution: 96.4% on-time (0-5 min delays)
- ✅ No extreme artifacts: 0 readings over 60 minutes
- ✅ Per-trip optimization: Not naive alignment

### What This Is NOT
- ❌ Ground truth: Optimized for timing fit, not independent validation
- ❌ Full network: 74 trips out of full network (trip_id match limited)
- ❌ Perfect stop mapping: Only matched RT trips are included
- ❌ Real-time (data is 2026-03-10 at time of build)

### Known Limitations
1. **Trip ID matching**: If RT trip_id ≠ static trip_id, row is excluded
2. **Offset approximation**: Best offset chosen by timing error minimization, not ground truth
3. **Stop sequence ambiguity**: No independent way to verify "offset 22 is correct"
4. **Partial trips**: RT feeds typically represent end-to-end trips, but some may be partial

---

## Column Definitions

| Column | Type | Notes |
|--------|------|-------|
| `trip_id` | VARCHAR | Matched realtime trip ID |
| `route_id` | VARCHAR | Route identifier (e.g., 'Yellow-S') |
| `direction_id` | INTEGER | 0 or 1 |
| `service_id` | VARCHAR | Calendar service identifier |
| `best_offset` | INTEGER | Selected sequence offset (0-24) |
| `rt_seq` | INTEGER | Realtime ordinal position (1-N) |
| `rt_stop_id` | VARCHAR | Realtime stop ID (e.g., 'R40-1') |
| `static_stop_id` | VARCHAR | Static GTFS stop ID |
| `stop_name` | VARCHAR | Human-readable stop name |
| `service_date_local` | DATE | Trip service date in America/Los_Angeles |
| `actual_arrival_local_ts` | TIMESTAMP | Actual arrival time (local) |
| `scheduled_arrival_local_ts` | TIMESTAMP | Scheduled arrival time (local) |
| `delay_minutes` | INTEGER | Computed delay (actual - scheduled) |

---

## Usage

### Safe Use Cases
- Exploring delays by route and stop
- Finding worst-performing routes/hours
- Correlating delays with weather
- Building exploration dashboard

### Unsafe Use Cases
- Claiming "BART delays are X minutes overall" without caveats
- Treating missing routes/trips as "zero delay"
- Using for SLA enforcement without independent validation
- Assuming offset=N is definitively correct

---

## Statistics

| Metric | Value |
|--------|-------|
| Total delay records | 1,056 |
| Distinct trips | 74 |
| Distinct routes | 10 |
| Min delay | -16 min |
| Max delay | +6 min |
| Median delay | 1 min |
| On-time (≤5 min) | 96.4% |
| Records over 60 min | 0 |

---

## Next Steps

1. **Aggregate**: Build KPI tables (route_performance, stop_performance, route_hour_performance)
2. **Enrich**: Join weather data to identify precipitation correlations
3. **Validate**: Add QA checks to prevent silent breakage
4. **Dashboard**: Build exploration interface for route/stop/hour performance

---

## References

- **Built by**: [build_best_offset_alignment_preview.py](build_best_offset_alignment_preview.py)
- **Validated by**: [check_best_offset_alignment_preview.py](check_best_offset_alignment_preview.py), [check_delay_preview_best_offset.py](check_delay_preview_best_offset.py)
- **Delay computed by**: [build_delay_preview_best_offset.py](build_delay_preview_best_offset.py)
