{{
  config (
    materialized = 'ephemeral'
    )
}}

SELECT
    account_id_hashed
    , date
    , transactions_num

FROM {{ source('monzo_datawarehouse','account_created') }}