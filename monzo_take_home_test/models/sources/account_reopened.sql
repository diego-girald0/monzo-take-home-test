{{
  config (
    materialized = 'ephemeral'
    )
}}

SELECT
    account_id_hashed
    , reopened_ts

FROM {{ source('monzo_datawarehouse','account_reopened') }}