-- raw sql results do not include filled-in values for 'alert_event.event_timestamp_week'


WITH alert_event AS (

        SELECT *,
        -- For malware alerts overrite the action with the one found in the matching policy alert ENG-125246
        CASE WHEN alert_type = 'Malware' THEN policy_alerts.policy_action  ELSE alerts.action END AS derived_action,
        -- For malware alerts overrite the policy with the one found in the matching policy alert ENG-125246
        CASE WHEN alert_type = 'Malware' THEN policy_alerts.policy_name ELSE alerts.policy END AS derived_policy,
        alerts.transaction_id as derived_transaction_id
        FROM ((SELECT A.* ,
        -- Derive OU levels
        SPLIT(A.organization_unit, '/')  AS OU,
        -- Derive app_instance concatenation for app/instance rbac scope query
        CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM "redshift_poc_iceberg"."alert_v3" AS A
        -- Get only one tenant's data and limit by date condition
        WHERE A.ns_tenant_id=2683
        AND ((( A.timestamp ) >= ((DATE_ADD('day', -89, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( A.timestamp ) < ((DATE_ADD('day', 90, DATE_ADD('day', -89, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))
        ) as alerts

        LEFT OUTER JOIN

        -- Self-join to get the alerts with matching transaction ids
        (SELECT alert_type as policy_alert_type, transaction_id, action as policy_action, policy as policy_name
          FROM  "redshift_poc_iceberg"."alert_v3"
          -- Get only one tenant's data and add a date filter to limit the size of the tables we are joining
          WHERE transaction_id IS NOT NULL AND ns_tenant_id=2683
          AND ((alert_type = 'policy' AND malware_name IS NOT NULL) OR (alert_type IN ('ips', 'ctep') AND action in ('block', 'blocked')))
          AND ((( timestamp ) >= ((DATE_ADD('day', -89, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( timestamp ) < ((DATE_ADD('day', 90, DATE_ADD('day', -89, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))
        ) AS policy_alerts
        ON policy_alerts.transaction_id = alerts.transaction_id

        )

    )
SELECT
    (DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(alert_event.timestamp) % 7) - 1 + 7, 7)), alert_event.timestamp)), '%Y-%m-%d')) AS "alert_event.event_timestamp_week",
    COALESCE(SUM(COALESCE(TRY( alert_event.numbytes   / 1073741824.0), 0) ), 0) AS "alert_event.sum_total_bytes_gb",
    COALESCE(SUM(COALESCE(TRY( alert_event.file_size  / 1073741824.0), 0) ), 0) AS "alert_event.sum_file_size_gb"
FROM alert_event


WHERE ((( alert_event.timestamp ) >= ((DATE_ADD('day', -89, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( alert_event.timestamp ) < ((DATE_ADD('day', 90, DATE_ADD('day', -89, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))) AND (alert_event.derived_action
    ) = 'block' AND (alert_event.ns_tenant_id ) = 2683 AND (-- For userip field, apply CIDR range filter if specified
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
    1 DESC
LIMIT 500