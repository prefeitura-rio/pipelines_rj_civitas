{% macro data_operacional_atual() %}
    {#- Data do dia operacional atual (07:00 às 07:00, America/Sao_Paulo): antes das 07:00, ainda é ontem. -#}
    DATE(DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 7 HOUR))
{% endmacro %}
