{{
  config (
    materialized = 'incremental'
    , unique_key = 'user_daily_transaction_id'
    , alias = project_prefix(model.name)
  )
}}

WITH account_transactions AS (
    SELECT
        t.account_id_hashed
        , t.date AS transaction_date
        , t.transactions_num
        , c.user_id_hashed
    FROM {{ ref('account_transactions') }} t
    JOIN {{ ref('account_created') }} c
        ON t.account_id_hashed = c.account_id_hashed
    {% if is_incremental() %}
        WHERE c.user_id_hashed NOT IN (SELECT user_id_hashed FROM {{ this }})
        OR (
            c.user_id_hashed IN (SELECT user_id_hashed FROM {{ this }})
            AND t.date > (SELECT MAX(transaction_date) FROM {{ this }} WHERE user_id_hashed = c.user_id_hashed)
        )
    {% endif %}
)
, user_transactions AS (
    SELECT
        user_id_hashed
        , transaction_date
        , SUM(transactions_num) AS total_transactions 
    FROM account_transactions 
    GROUP BY 1,2
)
, final AS (
    SELECT
        CONCAT(user_id_hashed, '-', CAST(transaction_date AS STRING)) AS user_daily_transaction_id
        , user_id_hashed
        , transaction_date
        , total_transactions
        , TO_JSON_STRING(
            STRUCT(
            CURRENT_TIMESTAMP() AS load_timestamp,
            'fact_7d_active_users' AS pipeline_name,
            ARRAY['monzo_datawarehouse.account_transactions', 'monzo_datawarehouse.account_created'] AS source_tables
            )
        ) AS technical_metadata
    FROM user_transactions
)

SELECT *
FROM final
