
-- analytics/duckdb/queries/top_users.sql
SELECT user_id, SUM(total_watch_seconds) AS total
FROM read_parquet('lake/watch_agg/dt=*/part-*.parquet')
GROUP BY user_id
ORDER BY total DESC
LIMIT 20;
