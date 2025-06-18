WITH alert_event AS (
       SELECT *, action as derived_action, policy as derived_policy, transaction_id as derived_transaction_id
      FROM (
       SELECT A.* ,
        SPLIT(A.organization_unit, '/')  AS OU,
        CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM "redshift_poc_iceberg"."alert_v3" AS A
        WHERE A.ns_tenant_id=2683
        AND ((( A.timestamp ) >= ((DATE_ADD('week', -3, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))) AND ( A.timestamp ) < ((DATE_ADD('week', 4, DATE_ADD('week', -3, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))))))
    )
SELECT
    alert_event.alert_type  AS "alert_event.alert_type",
    COUNT(*) AS "alert_event.event_count"
FROM alert_event

WHERE ((alert_event.timestamp ) >= ((DATE_ADD('week', -3, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))) AND (alert_event.timestamp ) < ((DATE_ADD('week', 4, DATE_ADD('week', -3, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))) AND (alert_event.ns_tenant_id ) = 2683 AND (
           1=1
          AND
           1=1
          AND
           1=1
          AND
           1=1
          AND
           1=1
          AND
1 = 1
         )
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 500