{{
  config (
    materialized = 'view'
    , alias = project_prefix(model.name)
  )
}}

WITH final AS (
    SELECT DISTINCT 
        account_id_hashed
        , user_id_hashed
        , account_type
        , TO_JSON_STRING(
            STRUCT(
                CURRENT_TIMESTAMP() AS load_timestamp,
                'dim_account daily' AS pipeline_name,
                'monzo_datawarehouse.account_created' AS source_table
            )
        ) AS technical_metadata
    FROM {{ ref('account_created') }}
)

SELECT *
FROM final