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
        AND ((( A.timestamp ) >= ((DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( A.timestamp ) < ((DATE_ADD('day', 7, DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))))

    )
SELECT
    alert_event.alert_type  AS "alert_event.alert_type",
        (DATE_FORMAT(alert_event.timestamp, '%Y-%m-%d')) AS "alert_event.event_timestamp_date",
        (DAY_OF_MONTH(alert_event.timestamp)) AS "alert_event.event_timestamp_day_of_month",
        (DATE_FORMAT(alert_event.timestamp,'%W')) AS "alert_event.event_timestamp_day_of_week",
        (DAY_OF_YEAR(alert_event.timestamp)) AS "alert_event.event_timestamp_day_of_year",
        (MOD((DAY_OF_WEEK(alert_event.timestamp) % 7) - 1 + 7, 7)) AS "alert_event.event_timestamp_day_of_week_index",
        (DATE_FORMAT(alert_event.timestamp,'%Y-%m-%d %H')) AS "alert_event.event_timestamp_hour",
        (HOUR(alert_event.timestamp)) AS "alert_event.event_timestamp_hr_hour_of_day",
        (DATE_FORMAT(alert_event.timestamp,'%Y-%m-%d %H')) AS "alert_event.event_timestamp_hr_hour",
        (DATE_FORMAT(alert_event.timestamp,'%Y-%m-%d %H:%i')) AS "alert_event.event_timestamp_minute",
        (DATE_FORMAT(alert_event.timestamp, '%M')) AS "alert_event.event_timestamp_month_name",
        (DATE_FORMAT(alert_event.timestamp,'%Y-%m')) AS "alert_event.event_timestamp_month",
        (DATE_FORMAT(DATE_TRUNC('QUARTER', alert_event.timestamp),'%Y-%m')) AS "alert_event.event_timestamp_quarter",
    CASE
        WHEN (MONTH(alert_event.timestamp)) IN (1,4,7,10) THEN 1
        WHEN (MONTH(alert_event.timestamp)) IN (2,5,8,11) THEN 2
        ELSE 3
      END
     AS "alert_event.event_timestamp_month_of_quarter",
        (MONTH(alert_event.timestamp)) AS "alert_event.event_timestamp_month_num",
        (DATE_FORMAT(alert_event.timestamp, '%Y-%m-%d %T')) AS "alert_event.event_timestamp_time",
        (DATE_FORMAT(CAST(alert_event.timestamp AS TIMESTAMP) , '%Y-%m-%d %T')) AS "alert_event.event_time",
        (DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(alert_event.timestamp) % 7) - 1 + 7, 7)), alert_event.timestamp)), '%Y-%m-%d')) AS "alert_event.event_timestamp_week",
        (WEEK_OF_YEAR(alert_event.timestamp)) AS "alert_event.event_timestamp_week_of_year",
        (YEAR(alert_event.timestamp)) AS "alert_event.event_timestamp_year",
    COUNT(*) AS "alert_event.event_count"
FROM alert_event


WHERE ((( alert_event.timestamp ) >= ((DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))) AND ( alert_event.timestamp ) < ((DATE_ADD('day', 7, DATE_ADD('day', -6, CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))) AND (alert_event.ns_tenant_id ) = 2683 AND (-- For userip field, apply CIDR range filter if specified
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
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20
ORDER BY
    2 DESC
LIMIT 500