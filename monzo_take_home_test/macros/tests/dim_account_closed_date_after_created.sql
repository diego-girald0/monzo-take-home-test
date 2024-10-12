{% test dim_account_closed_date_after_created(model) %}

SELECT account_id_hashed
FROM {{ model }}
WHERE closed_ts IS NOT NULL
AND closed_ts < created_ts

{% endtest %}
