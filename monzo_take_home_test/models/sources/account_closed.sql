{{
  config (
    materialized = 'ephemeral'
    )
}}

SELECT
    account_id_hashed
    , closed_ts

FROM {{ source('monzo_datawarehouse','account_closed') }}