WITH alert_event AS (

        SELECT *,
        CASE WHEN alert_type = 'Malware' THEN policy_alerts.policy_action  ELSE alerts.action END AS derived_action,
        CASE WHEN alert_type = 'Malware' THEN policy_alerts.policy_name ELSE alerts.policy END AS derived_policy,
        alerts.transaction_id as derived_transaction_id
        FROM ((SELECT A.* ,
        SPLIT(A.organization_unit, '/')  AS OU,
        CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM "redshift_poc_iceberg"."alert_v3" AS A
        WHERE A.ns_tenant_id=2683
        AND ((( A.timestamp ) >= ((DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( A.timestamp ) < ((DATE_ADD('day', 7, DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))
        ) as alerts

        LEFT OUTER JOIN

        (SELECT alert_type as policy_alert_type, transaction_id, action as policy_action, policy as policy_name
          FROM  "redshift_poc_iceberg"."alert_v3"
          WHERE transaction_id IS NOT NULL AND ns_tenant_id=2683
          AND ((alert_type = 'policy' AND malware_name IS NOT NULL) OR (alert_type IN ('ips', 'ctep') AND action in ('block', 'blocked')))
          AND ((( timestamp ) >= ((DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( timestamp ) < ((DATE_ADD('day', 7, DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))
        ) AS policy_alerts
        ON policy_alerts.transaction_id = alerts.transaction_id

        )

    )
  ,  app_category AS (
      SELECT * FROM "redshift_poc_iceberg"."appcategory" WHERE ns_tenant_id IN (2683, -1))
SELECT
    coalesce(app_category.app_category_current_name, alert_event.appcategory) AS "alert_event.appcategory",
    COALESCE(SUM(alert_event.count ), 0) AS "alert_event.event_count",
    COUNT(DISTINCT alert_event.site) AS "alert_event.distinct_site_count"
FROM alert_event


LEFT JOIN app_category ON
                  alert_event.ns_category_id = app_category.category_id
WHERE ((( alert_event.timestamp ) >= ((DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( alert_event.timestamp ) < ((DATE_ADD('day', 7, DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))) AND (alert_event.derived_action
    ) = 'block' AND (alert_event.ns_tenant_id ) = 2683 AND (
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
         ) AND (coalesce(app_category.app_category_current_name, alert_event.appcategory)) IS NOT NULL
GROUP BY
    1
ORDER BY
    3 DESC
LIMIT 15