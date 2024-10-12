{{
  config (
    materialized = 'incremental'
    , unique_key = 'account_id_hashed'
    , alias = project_prefix(model.name)
  )
}}

WITH event_aggregates AS (
  SELECT
    account_id_hashed
    , MAX(CASE WHEN event_type = 'created' THEN event_timestamp ELSE NULL END) AS created_ts
    , MAX(CASE WHEN event_type = 'closed' THEN event_timestamp ELSE NULL END) AS closed_ts
    , MAX(CASE WHEN event_type = 'reopened' THEN event_timestamp ELSE NULL END) AS reopened_ts
    , GREATEST(
        MAX(CASE WHEN event_type = 'created' THEN event_timestamp END),
        MAX(CASE WHEN event_type = 'closed' THEN event_timestamp END),
        MAX(CASE WHEN event_type = 'reopened' THEN event_timestamp END)
      ) AS latest_update_ts
  FROM {{ ref('stg_account_event_history') }}
  GROUP BY account_id_hashed
)
, filtered_events AS (
  SELECT *
  FROM event_aggregates
  {% if is_incremental() %}
    WHERE account_id_hashed NOT IN (SELECT account_id_hashed FROM {{ this }})
      OR (
          account_id_hashed IN (SELECT account_id_hashed FROM {{ this }})
          AND latest_update_ts > (SELECT MAX(latest_update_ts) FROM {{ this }} a WHERE a.account_id_hashed = account_id_hashed)
      )
  {% endif %}
)
, final AS (
  SELECT
    e.account_id_hashed
    , h.user_id_hashed
    , h.account_type
    , CASE 
        WHEN e.reopened_ts IS NOT NULL THEN 'reopened'
        WHEN e.closed_ts IS NOT NULL THEN 'closed'
        WHEN e.closed_ts IS NULL THEN 'open'
      END AS account_status
    , CASE 
        WHEN e.closed_ts IS NULL THEN TRUE
        WHEN e.reopened_ts IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS is_open
    , e.created_ts
    , e.closed_ts
    , e.reopened_ts
    , e.latest_update_ts

    , TO_JSON_STRING(
        STRUCT(
          CURRENT_TIMESTAMP() AS load_timestamp,
          'dim_account daily' AS pipeline_name,
          ARRAY['stg_account_event_history', 'stg_account_hub'] AS source_tables
        )
      ) AS technical_metadata
  FROM filtered_events e
  LEFT JOIN {{ ref('stg_account_hub') }} h 
  ON e.account_id_hashed = h.account_id_hashed
)

SELECT *
FROM final
