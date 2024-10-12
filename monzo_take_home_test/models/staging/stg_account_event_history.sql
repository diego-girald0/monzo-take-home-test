{{
  config (
    materialized = 'incremental'
    , unique_key = 'account_id_hashed'
    , alias = project_prefix(model.name)
  )
}}

WITH created AS (
    SELECT 
        account_id_hashed
        , created_ts AS event_timestamp
        , 'created' AS event_type
        , TO_JSON_STRING(
            STRUCT(
                CURRENT_TIMESTAMP() AS load_timestamp,
                'dim_account daily' AS pipeline_name,
                'monzo_datawarehouse.account_created' AS source_table
            )
        ) AS technical_metadata
    FROM {{ ref('account_created') }}
    {% if is_incremental() %}
        WHERE created_ts > (SELECT MAX(event_timestamp) FROM {{ this }} WHERE event_type = 'created')
    {% endif %}
)

, closed AS (
    SELECT 
        account_id_hashed
        , closed_ts AS event_timestamp
        , 'closed' AS event_type
        , TO_JSON_STRING(
            STRUCT(
                CURRENT_TIMESTAMP() AS load_timestamp,
                'dim_account daily' AS pipeline_name,
                'monzo_datawarehouse.account_closed' AS source_table
            )
        ) AS technical_metadata
    FROM {{ ref('account_closed') }}
    {% if is_incremental() %}
        WHERE closed_ts > (SELECT MAX(event_timestamp) FROM {{ this }} WHERE event_type = 'closed')
    {% endif %}
)

, reopened AS (
    SELECT 
        account_id_hashed
        , reopened_ts AS event_timestamp
        , 'reopened' AS event_type
        , TO_JSON_STRING(
            STRUCT(
                CURRENT_TIMESTAMP() AS load_timestamp,
                'dim_account daily' AS pipeline_name,
                'monzo_datawarehouse.account_reopened' AS source_table
            )
        ) AS technical_metadata
    FROM {{ ref('account_reopened') }}
    {% if is_incremental() %}
        WHERE reopened_ts > (SELECT MAX(event_timestamp) FROM {{ this }} WHERE event_type = 'reopened')
    {% endif %}
)

, all_events AS (
    SELECT * FROM created
    UNION ALL
    SELECT * FROM closed
    UNION ALL
    SELECT * FROM reopened
)
, final AS (
    SELECT 
        CONCAT(account_id_hashed, '-', event_type, '-', CAST(event_timestamp AS STRING)) AS unique_event_id
        , account_id_hashed
        , event_type
        , event_timestamp
        , technical_metadata
    FROM all_events
    )

SELECT *
FROM final
