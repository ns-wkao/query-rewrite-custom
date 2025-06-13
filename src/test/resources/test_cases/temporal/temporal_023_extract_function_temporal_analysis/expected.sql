SELECT
  EXTRACT(
    HOUR
    FROM
      TIMESTAMP
  ) hour_of_day,
  ns_tenant_id,
  alert_type,
  SUM("count") total_count
FROM
  redshift_poc_iceberg.alert_event_hourly_sum
WHERE
  (
    (ns_tenant_id = 2683)
    AND (
      EXTRACT(
        HOUR
        FROM
          TIMESTAMP
      ) = 15
    )
  )
GROUP BY
  1,
  ns_tenant_id,
  alert_type
ORDER BY
  hour_of_day ASC