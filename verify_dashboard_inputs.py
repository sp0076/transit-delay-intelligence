#!/usr/bin/env python3
import duckdb

conn = duckdb.connect('./data/transit_delay.duckdb', read_only=True)
print('route_performance_rows', conn.execute('select count(*) from marts.route_performance').fetchone()[0])
print('stop_performance_rows', conn.execute('select count(*) from marts.stop_performance').fetchone()[0])
print('route_hour_performance_rows', conn.execute('select count(*) from marts.route_hour_performance').fetchone()[0])
print('weather_impact_rows', conn.execute('select count(*) from marts.weather_impact').fetchone()[0])
print('route_weather_impact_rows', conn.execute('select count(*) from marts.route_weather_impact').fetchone()[0])
print('coverage_cols_present', conn.execute("""
select count(*)
from information_schema.columns
where table_schema='marts'
  and table_name='route_performance'
  and column_name in ('coverage_rows','coverage_trips','pct_rt_rows_matched')
""").fetchone()[0])
print('rainy_rows', conn.execute('select count(*) from staging.delay_weather_enriched where is_raining').fetchone()[0])
print('distinct_routes_in_stop_table', conn.execute('select count(distinct route_id) from marts.stop_performance').fetchone()[0])
print('yellow_s_rows_in_stop_table', conn.execute("select count(*) from marts.stop_performance where route_id='Yellow-S'").fetchone()[0])
conn.close()
