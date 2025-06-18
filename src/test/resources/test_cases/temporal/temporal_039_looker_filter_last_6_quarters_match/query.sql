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
        WHERE A.ns_tenant_id=2683
        AND ((( A.timestamp ) >= ((DATE_ADD('month', -15, DATE_TRUNC('QUARTER', DATE_TRUNC('QUARTER', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))) AND ( A.timestamp ) < ((DATE_ADD('month', 18, DATE_TRUNC('QUARTER', DATE_ADD('month', -15, DATE_TRUNC('QUARTER', DATE_TRUNC('QUARTER', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))))))

    )
SELECT
    alert_event.alert_type  AS "alert_event.alert_type",
    COUNT(*) AS "alert_event.event_count"
FROM alert_event


WHERE ((( alert_event.timestamp ) >= ((DATE_ADD('month', -15, DATE_TRUNC('QUARTER', DATE_TRUNC('QUARTER', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))) AND ( alert_event.timestamp ) < ((DATE_ADD('month', 18, DATE_TRUNC('QUARTER', DATE_ADD('month', -15, DATE_TRUNC('QUARTER', DATE_TRUNC('QUARTER', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))))) AND (alert_event.ns_tenant_id ) = 2683 AND (-- For userip field, apply CIDR range filter if specified
           1=1
          AND
          -- For srcip field apply CIDR range filter if specified
           1=1
          AND
          -- For dstip field apply CIDR range filter if specified
           1=1
          AND
          -- For period over period analysis so we only show the current and previous period
           1=1
          AND
          -- Pass in the is_to_date filter to either apply or not apply the above condition
           1=1
          AND
          -- End period over period analysis conditions

           -- No RBAC

          -- Retention
1 = 1
         )
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 500