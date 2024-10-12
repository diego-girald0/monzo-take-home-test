{{
  config (
    materialized = 'incremental'
    , unique_key = 'date_key'
    , alias = project_prefix(model.name)
  )
}}

-- Current dataset only has transactions up to 2020-08-12
{% set lookback_start_date = var('lookback_start_date', '2020-08-12') %}

WITH date_range AS (
    SELECT
        DATE_SUB('{{ lookback_start_date }}', INTERVAL day DAY) AS date_key
    FROM UNNEST(GENERATE_ARRAY(0, 6)) AS day
)
, user_activity AS (
    SELECT
        user_id_hashed
        , transaction_date
    FROM {{ ref('stg_user_transactions') }}
)
, open_accounts AS (
    SELECT
        h.user_id_hashed
        , e.event_type
        , e.event_timestamp
    FROM {{ ref('dim_account') }} h
    JOIN {{ ref('stg_account_event_history') }} e
    ON h.account_id_hashed = e.account_id_hashed
)
, activity_summary AS (
    SELECT
        d.date_key
        , COUNT(DISTINCT u.user_id_hashed) AS active_users_count
        , COUNT(DISTINCT oa.user_id_hashed) AS open_users_count
    FROM date_range d
    LEFT JOIN user_activity u
        ON u.transaction_date BETWEEN DATE_SUB(d.date_key, INTERVAL 6 DAY) AND d.date_key
    LEFT JOIN open_accounts oa
        ON oa.event_timestamp <= TIMESTAMP(d.date_key) -- Status date is before or on the date_key
        AND (
            (oa.event_type = 'created' AND TIMESTAMP(d.date_key) >= oa.event_timestamp) OR  -- Created accounts
            (oa.event_type = 'reopened' AND TIMESTAMP(d.date_key) >= oa.event_timestamp)   -- Reopened accounts
        )

    WHERE NOT EXISTS ( -- Exclude accounts that have been closed before the date_key
            SELECT 1
            FROM open_accounts e
            WHERE e.user_id_hashed = oa.user_id_hashed
            AND e.event_type = 'closed'
            AND e.event_timestamp < TIMESTAMP(d.date_key)
        )
    GROUP BY 1
)
, final AS (
    SELECT 
        s.date_key
        , s.active_users_count
        , s.open_users_count
        , s.active_users_count / NULLIF(s.open_users_count, 0) AS `7d_active_users`
        , TO_JSON_STRING(
            STRUCT(
            CURRENT_TIMESTAMP() AS load_timestamp,
            'fact_7d_active_users' AS pipeline_name,
            ARRAY['dim_account', 'stg_user_transactions', 'stg_account_event_history'] AS source_tables
            )
        ) AS technical_metadata
    FROM activity_summary s
)

SELECT *
FROM final
