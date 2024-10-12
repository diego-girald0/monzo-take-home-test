{% test dim_account_reopened_after_closed(model) %}

SELECT account_id_hashed
FROM {{ model }}
WHERE reopened_ts IS NOT NULL
AND reopened_ts < closed_ts

{% endtest %}
