{% macro codigo_para_texto(coluna) %}
    UPPER(SUBSTR(TRIM(REPLACE({{ coluna }}, '_', ' ')), 1, 1))
    || LOWER(SUBSTR(TRIM(REPLACE({{ coluna }}, '_', ' ')), 2))
{% endmacro %}
