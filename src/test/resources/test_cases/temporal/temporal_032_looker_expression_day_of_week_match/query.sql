-- For this case the actual granularity should be week, but due to the DAY_OF_WEEK function it's recognized as day granularity
-- For the purpose of our rewrite we might get a FN if we have a week level MV but otherwise it's fine
WITH alert_event AS (
       SELECT *, action as derived_action, policy as derived_policy, transaction_id as derived_transaction_id
      FROM (
       SELECT A.* ,
        SPLIT(A.organization_unit, '/')  AS OU,
        CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM "redshift_poc_iceberg"."alert_v3" AS A
        WHERE A.ns_tenant_id=2683)
    )
SELECT
    alert_event.alert_type  AS "alert_event.alert_type",
                (MOD((DAY_OF_WEEK(alert_event.timestamp) % 7) - 1 + 7, 7)) AS "alert_event.event_timestamp_day_of_week_index",
    COUNT(*) AS "alert_event.event_count"
FROM alert_event
WHERE alert_event.ns_tenant_id = 2683
GROUP BY
    1,
    2
ORDER BY
    2 DESC
LIMIT 500