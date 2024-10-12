{% test dim_account_reopened_requires_closed(model) %}

SELECT account_id_hashed
FROM {{ model }}
WHERE reopened_ts IS NOT NULL
AND closed_ts IS NULL

{% endtest %}
