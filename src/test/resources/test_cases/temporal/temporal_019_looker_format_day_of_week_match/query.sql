WITH alert_event AS (
       SELECT *, action as derived_action, policy as derived_policy, transaction_id as derived_transaction_id
      FROM (
       SELECT A.* ,
        -- Derive OU levels
        SPLIT(A.organization_unit, '/')  AS OU,
        -- Derive app_instance concatenation for app/instance rbac scope query
        CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM "redshift_poc_iceberg"."alert_v3" AS A
        -- Get only one tenant's data and add a date filter to limit the size of the table
        WHERE A.ns_tenant_id=2683)
    )
SELECT
    alert_event.alert_type  AS "alert_event.alert_type",
        (DATE_FORMAT(alert_event.timestamp,'%W')) AS "alert_event.event_timestamp_day_of_week",
    COUNT(*) AS "alert_event.event_count"
FROM alert_event
WHERE alert_event.ns_tenant_id = 2683
GROUP BY
    1,
    2
ORDER BY
    2 DESC
LIMIT 500