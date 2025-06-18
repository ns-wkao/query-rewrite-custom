WITH
  alert_event AS (
    SELECT
      *,
      action derived_action,
      policy derived_policy,
      transaction_id derived_transaction_id
    FROM
      (
        SELECT
          A.*,
          SPLIT(A.organization_unit, '/') OU,
          CONCAT(A.app, A.instance_id) ns_app_instance
        FROM
          redshift_poc_iceberg.alert_event_daily_sum_detailed A
        WHERE
          (A.ns_tenant_id = 2683)
      )
  )
SELECT
  alert_event.alert_type "alert_event.alert_type",
  DATE_FORMAT(alert_event.TIMESTAMP, '%M') "alert_event.event_timestamp_month_name",
  COUNT(*) "alert_event.event_count"
FROM
  alert_event
WHERE
  (alert_event.ns_tenant_id = 2683)
GROUP BY
  1,
  2
ORDER BY
  2 DESC
LIMIT
  500