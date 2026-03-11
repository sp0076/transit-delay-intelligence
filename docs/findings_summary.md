# Findings Summary and Business Recommendations

## What the current MVP can support

- Route-level delay ranking on matched trips
- Stop-level delay hotspot identification
- Hour/day reliability patterns (when enough history exists)
- Weather context overlays (limited by current dry-only slices)
- Heuristic next-24h risk highlighting

## Key findings (current snapshot)

- Delay performance is route-dependent; top routes differ by average delay vs tail-risk (P95).
- Some routes show high average delay; others show lower average but worse extreme spikes.
- Current weather slice is mostly dry, so wet-weather inference is weak.
- Coverage is matched-trip only; unmatched RT trip IDs are excluded from route-level attribution.

## Business recommendations

1. **Prioritize reliability interventions on routes with high average + high P95 delay**
	- These routes hurt both typical rider experience and worst-case reliability.

2. **Target hotspot stops for dwell-time and headway controls**
	- Use stop-level delay hotspots to prioritize operational attention.

3. **Plan staffing and control-room focus around worst hour/day windows**
	- Use `marts.hour_day_reliability` to schedule proactive interventions.

4. **Integrate event and disruption feeds before operationalizing decisions**
	- Current event/alert tables are schema-ready, but need real feed population.

5. **Treat current risk view as triage, not forecast certainty**
	- It is heuristic and should be paired with human operational judgment.

## Decision caveats

- Do not use current KPI values as audited full-network SLA metrics.
- Always show match coverage alongside KPI charts.
- Communicate that best-offset alignment is an approximation.
