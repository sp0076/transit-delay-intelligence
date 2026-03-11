# Methodology and Limitations

## What This Project Measures

This project computes transit delay estimates for BART routes using GTFS-Realtime (RT) data
matched to GTFS static schedules. **This is exploratory analysis, not audited operational reporting.**

---

## Coverage

### Global RT Coverage

| Metric | Value |
|--------|-------|
| Total raw RT rows (with arrival timestamps) | 1,070 |
| Matched RT rows in delay layer | 1,056 |
| Unmatched RT rows | 14 |
| **Global RT row match rate** | **98.7%** |
| Raw distinct RT trip IDs | 84 |
| Matched distinct trip IDs | 74 |
| Unmatched trip IDs | 10 |
| **Trip ID match rate** | **88.1%** |

### Why Route-Level Coverage Looks 100%

Route KPI tables (`marts.route_performance`, `marts.stop_performance`, `marts.route_hour_performance`)
show `pct_rt_rows_matched = 100%` for every route that appears in them.

**This is not the same as full network coverage.**

The 10 unmatched RT trip IDs could not be joined to a route in `raw.gtfs_trips`.
Those trips are excluded entirely from route KPIs, not counted as missing coverage.
If excluded trips cluster in a route family (e.g. Antioch extension, special service),
that route is absent from the dashboard, not represented as low-coverage.

**Implication**: Do not interpret a route appearing in KPI tables as having 100% of its
real-world service measured. It means 100% of its *matched* trips are accounted for.

---

## Alignment Method

### Why Direct Join Failed

GTFS-Realtime stop_id values (e.g. `R40-1`, `M10-2`) do not match static GTFS stop_ids.
`stop_sequence` in RT data is always 0. Direct join on `(trip_id, stop_id, stop_sequence)` yields 0 matches.

### Offset-Based Sequence Alignment

For each RT trip that matched a static trip_id:

1. Assign ordinal sequence positions to both RT stop_updates and static stop_times (by arrival time order)
2. Search candidate offsets from 0 to `(static_count - rt_count)`
3. At each offset, align: `static_seq = rt_seq + offset`
4. Score each candidate by `MEDIAN(ABS(actual_arrival - scheduled_arrival))`
5. Select the offset that minimizes median absolute timing error

This reflects the observation that RT feeds capture the **remaining trip tail**, not the full schedule.

### Offset Distribution

| Offset bucket | % of delay layer rows |
|---------------|----------------------|
| 0 (from trip start) | 42.0% |
| 1–5 | 28.8% |
| 6–10 | 17.0% |
| 11+ | 12.2% |

Rows with high offsets are not noise. They are the result of the alignment algorithm and
mean the matched portion of the static schedule is the last N stops of the trip.

### Optimism Bias (Known)

Selecting offset by minimizing timing error means:

- The alignment is the *best fit available*, not independently validated ground truth
- Delay distributions will be tighter than naive alignment
- Reported sub-1-minute average delays reflect the method as much as actual transit performance
- This is appropriate for MVP exploration; it is not appropriate for SLA or contractual reporting

---

## Weather Enrichment

Weather joined from `raw.weather_hourly` on the local hour of `actual_arrival_local_ts`.

Current data spans a dry period (precipitation_mm = 0 for all matched rows).
**No wet/dry delay comparison is possible yet.** Ingesting data across multiple days
including rain events is required before weather sensitivity analysis is meaningful.

---

## What KPI Tables Are Safe to Use For

- Identifying which routes have higher measured delays *within matched coverage*
- Exploring stop-level patterns within those routes
- Understanding hour-of-day trends for the captured time window
- Building an exploration dashboard with appropriate caveats

## What KPI Tables Should Not Be Used For

- Claiming "BART on-time performance is X% overall"
- Network-wide benchmarking without noting 88.1% trip coverage
- Wet/dry delay comparison (no rain data yet)
- SLA calculations or contractual performance claims

---

## Excluded Trips

Stored in `staging.unmatched_rt_trip_ids` with columns:
- `trip_id`: RT trip ID that could not be matched to static
- `raw_row_count`: Number of RT rows for this trip
- `first_poll_ts` / `last_poll_ts`: Poll window for this trip

Inspect this table to check whether excluded trips cluster in a specific service family
before interpreting route-level KPIs as representative.

---

## Required Dashboard Labeling

Any dashboard or report derived from these KPI tables must include:

> "Based on matched GTFS-RT trips only (88.1% of raw trip IDs, 98.7% of RT rows).
> Alignment uses per-trip best-offset method to approximate arrival matching.
> This is exploratory analysis, not audited operational performance reporting."
