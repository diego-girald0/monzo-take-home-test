{% test dim_account_status_conflict(model) %}

SELECT account_id_hashed
FROM {{ model }}
WHERE account_status = 'open' AND closed_ts IS NOT NULL

{% endtest %}
