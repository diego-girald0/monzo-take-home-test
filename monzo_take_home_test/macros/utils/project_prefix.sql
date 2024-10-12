{% macro project_prefix(model_name) %}
    {{ return(var('project_prefix') ~ '_' ~ model_name) }}
{% endmacro %}
