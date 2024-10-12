{{
  config (
    materialized = 'ephemeral'
    )
}}

SELECT
    account_id_hashed
    , user_id_hashed
    , account_type
    , created_ts

FROM {{ source('monzo_datawarehouse','account_created') }}