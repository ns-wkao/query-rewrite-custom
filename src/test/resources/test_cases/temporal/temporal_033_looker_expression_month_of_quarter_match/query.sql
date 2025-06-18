-- For this case the actual granularity should be quarter, but due to the MONTH function it's recognized as month granularity
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
                CASE
        WHEN (MONTH(alert_event.timestamp)) IN (1,4,7,10) THEN 1
        WHEN (MONTH(alert_event.timestamp)) IN (2,5,8,11) THEN 2
        ELSE 3
      END
     AS "alert_event.event_timestamp_month_of_quarter",
    COUNT(*) AS "alert_event.event_count"
FROM alert_event
WHERE alert_event.ns_tenant_id = 2683
GROUP BY
    1,
    2
ORDER BY
    2 DESC
LIMIT 500