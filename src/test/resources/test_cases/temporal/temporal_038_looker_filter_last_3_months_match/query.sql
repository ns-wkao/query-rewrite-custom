WITH alert_event AS (
       SELECT *, action as derived_action, policy as derived_policy, transaction_id as derived_transaction_id
      FROM (
       SELECT A.* ,
        SPLIT(A.organization_unit, '/')  AS OU,
        CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM "redshift_poc_iceberg"."alert_v3" AS A
        WHERE A.ns_tenant_id=2683
        AND ((( A.timestamp ) >= ((DATE_ADD('month', -2, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))) AND ( A.timestamp ) < ((DATE_ADD('month', 3, DATE_ADD('month', -2, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))))

    )
SELECT
    alert_event.alert_type  AS "alert_event.alert_type",
    COUNT(*) AS "alert_event.event_count"
FROM alert_event

WHERE ((( alert_event.timestamp ) >= ((DATE_ADD('month', -2, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))) AND ( alert_event.timestamp ) < ((DATE_ADD('month', 3, DATE_ADD('month', -2, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))) AND (alert_event.ns_tenant_id ) = 2683 AND (-- For userip field, apply CIDR range filter if specified
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