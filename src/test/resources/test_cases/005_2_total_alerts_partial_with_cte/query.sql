WITH alert_event AS (
       SELECT *, action as derived_action, policy as derived_policy, transaction_id as derived_transaction_id
      FROM (
       SELECT A.* ,
        SPLIT(A.organization_unit, '/')  AS OU,
        CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM "redshift_poc_iceberg"."alert_v3" AS A
        WHERE A.ns_tenant_id=2683
        AND ((( A.timestamp ) >= (TIMESTAMP '2025-01-03') AND ( A.timestamp ) < (TIMESTAMP '2025-01-17'))))
    )
SELECT
    alert_event.alert_type  AS "alert_event.alert_type"
FROM alert_event