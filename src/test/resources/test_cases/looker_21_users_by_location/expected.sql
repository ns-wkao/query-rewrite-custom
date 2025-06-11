WITH
  page_event AS (
    SELECT
      A.*,
      SPLIT(A.organization_unit, '/') OU
    FROM
      redshift_poc_iceberg.page_event_30min_sum A
    WHERE
      (
        (A.ns_tenant_id = 2683)
        AND (
          (A.TIMESTAMP >= TIMESTAMP '2025-01-03')
          AND (A.TIMESTAMP < TIMESTAMP '2025-01-17')
        )
      )
  ),
  app_info AS (
    SELECT
      *
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              app_id
            ORDER BY
              ns_tenant_id DESC
          ) appinfo_row_rank
        FROM
          "redshift_poc_iceberg"."appinfo"
        WHERE
          (ns_tenant_id IN (2683, -1))
      )
    WHERE
      (appinfo_row_rank = 1)
  )
SELECT
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
              ELSE ROUND(page_event.slc_latitude, 4)
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
              ELSE ROUND(page_event.slc_longitude, 4)
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
                  ELSE ROUND(page_event.slc_latitude, 4)
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
                  ELSE ROUND(page_event.slc_longitude, 4)
                END
              ) AS VARCHAR
            ),
            ''
          )
        ) AS VARCHAR
      )
      ELSE null
    END
  ) "page_event.src_geo_location",
  page_event.src_location "page_event.src_city",
  COALESCE(SUM(page_event.count), 0) "page_event.event_count",
  COUNT(DISTINCT page_event.ur_normalized) "page_event.distinct_user_count",
  COUNT(
    DISTINCT COALESCE(app_info.app_current_name, page_event.app)
  ) "page_event.distinct_app_count"
FROM
  (
    (
      page_event
      LEFT JOIN app_info ON (page_event.app = app_info.appinfo_app)
    )
    LEFT JOIN "redshift_poc_iceberg"."geo_location_lookup" src_geo_location_lookup ON (
      (
        page_event.src_location = src_geo_location_lookup.city_ascii
      )
      AND (
        page_event.src_region = src_geo_location_lookup.state_ascii
      )
      AND (
        page_event.src_country IN (src_geo_location_lookup.iso2, 'unknown')
      )
    )
  )
WHERE
  (
    (
      (page_event.TIMESTAMP >= TIMESTAMP '2025-01-03')
      AND (page_event.TIMESTAMP < TIMESTAMP '2025-01-17')
    )
    AND (page_event.ns_tenant_id = 2683)
    AND (
      (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
    )
    AND (page_event.src_location IS NOT NULL)
  )
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT
  500