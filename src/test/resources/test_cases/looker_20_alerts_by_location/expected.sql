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
          (
            (A.ns_tenant_id = 2683)
            AND (
              (A.TIMESTAMP >= TIMESTAMP '2025-01-03')
              AND (A.TIMESTAMP < TIMESTAMP '2025-01-17')
            )
          )
      )
  )
SELECT
  alert_event.src_location "alert_event.src_city",
  (
    CASE
      WHEN (
        (
          (
            CASE
              WHEN (
                (src_geo_location_lookup.lat IS NOT NULL)
                AND (src_geo_location_lookup.lng IS NOT NULL)
              ) THEN src_geo_location_lookup.lat
              ELSE ROUND(alert_event.slc_latitude, 4)
            END
          ) IS NOT NULL
        )
        AND (
          (
            CASE
              WHEN (
                (src_geo_location_lookup.lat IS NOT NULL)
                AND (src_geo_location_lookup.lng IS NOT NULL)
              ) THEN src_geo_location_lookup.lng
              ELSE ROUND(alert_event.slc_longitude, 4)
            END
          ) IS NOT NULL
        )
      ) THEN CAST(
        CONCAT(
          COALESCE(
            CAST(
              (
                CASE
                  WHEN (
                    (src_geo_location_lookup.lat IS NOT NULL)
                    AND (src_geo_location_lookup.lng IS NOT NULL)
                  ) THEN src_geo_location_lookup.lat
                  ELSE ROUND(alert_event.slc_latitude, 4)
                END
              ) AS VARCHAR
            ),
            ''
          ),
          ', ',
          COALESCE(
            CAST(
              (
                CASE
                  WHEN (
                    (src_geo_location_lookup.lat IS NOT NULL)
                    AND (src_geo_location_lookup.lng IS NOT NULL)
                  ) THEN src_geo_location_lookup.lng
                  ELSE ROUND(alert_event.slc_longitude, 4)
                END
              ) AS VARCHAR
            ),
            ''
          )
        ) AS VARCHAR
      )
      ELSE null
    END
  ) "alert_event.src_geo_location",
  COALESCE(SUM(alert_event.count), 0) "alert_event.event_count"
FROM
  (
    alert_event
    LEFT JOIN "redshift_poc_iceberg"."geo_location_lookup" src_geo_location_lookup ON (
      (
        alert_event.src_location = src_geo_location_lookup.city_ascii
      )
      AND (
        alert_event.src_region = src_geo_location_lookup.state_ascii
      )
      AND (
        alert_event.src_country IN (src_geo_location_lookup.iso2, 'unknown')
      )
    )
  )
WHERE
  (
    (
      (alert_event.TIMESTAMP >= TIMESTAMP '2025-01-03')
      AND (alert_event.TIMESTAMP < TIMESTAMP '2025-01-17')
    )
    AND (alert_event.ns_tenant_id = 2683)
    AND (
      (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
    )
    AND (alert_event.src_location IS NOT NULL)
  )
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT
  100