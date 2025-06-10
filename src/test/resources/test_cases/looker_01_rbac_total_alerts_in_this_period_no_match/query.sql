WITH
  alert_event AS (
    SELECT
      *,
      action AS derived_action,
      policy AS derived_policy,
      transaction_id AS derived_transaction_id
    FROM
      (
        SELECT
          A.*,
          SPLIT(A.organization_unit, '/') AS OU,
          CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM
          "redshift_poc_iceberg"."alert_v3" AS A
        WHERE
          A.ns_tenant_id = 2683
          AND (
            (
              (A.timestamp) >= (TIMESTAMP '2025-01-03')
              AND (A.timestamp) < (TIMESTAMP '2025-01-17')
            )
          )
      )
  )
SELECT
  COALESCE(
    CAST(
      (
        SUM(
          DISTINCT (
            CAST(
              FLOOR(COALESCE(alert_event.count, 0) * (1000000 * 1.0)) AS DECIMAL(38, 0)
            )
          ) + (
            CAST(
              FROM_BASE(
                SUBSTR(
                  TO_HEX(
                    MD5(
                      CAST(CAST(alert_event.ns_id AS VARCHAR) AS VARBINARY)
                    )
                  ),
                  1,
                  14
                ),
                16
              ) AS DECIMAL(38, 0)
            ) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(
              FROM_BASE(
                SUBSTR(
                  TO_HEX(
                    MD5(
                      CAST(CAST(alert_event.ns_id AS VARCHAR) AS VARBINARY)
                    )
                  ),
                  17,
                  10
                ),
                16
              ) AS DECIMAL(38, 0)
            )
          )
        ) - SUM(
          DISTINCT (
            CAST(
              FROM_BASE(
                SUBSTR(
                  TO_HEX(
                    MD5(
                      CAST(CAST(alert_event.ns_id AS VARCHAR) AS VARBINARY)
                    )
                  ),
                  1,
                  14
                ),
                16
              ) AS DECIMAL(38, 0)
            ) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(
              FROM_BASE(
                SUBSTR(
                  TO_HEX(
                    MD5(
                      CAST(CAST(alert_event.ns_id AS VARCHAR) AS VARBINARY)
                    )
                  ),
                  17,
                  10
                ),
                16
              ) AS DECIMAL(38, 0)
            )
          )
        )
      ) AS DOUBLE
    ) / CAST((1000000 * 1.0) AS DOUBLE),
    0
  ) AS "alert_event.event_count"
FROM
  alert_event
  LEFT JOIN "redshift_poc_iceberg"."usergroups" AS groups ON alert_event.ns_tenant_id = groups.ns_tenant_id
  AND (LOWER(alert_event.user)) = (LOWER(groups.group_user))
  CROSS JOIN UNNEST (groups.groupnames) AS T (usergroups)
WHERE
  (
    (
      (alert_event.timestamp) >= (TIMESTAMP '2025-01-03')
      AND (alert_event.timestamp) < (TIMESTAMP '2025-01-17')
    )
  )
  AND (alert_event.alert_type) IN (
    'Compromised Credential',
    'DLP',
    'Malware',
    'Security Assessment',
    'anomaly',
    'malsite',
    'policy'
  )
  AND ((UPPER((usergroups)) = UPPER('web')))
  AND (alert_event.ns_tenant_id) = 2683
  AND (
    1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
  )
LIMIT
  500