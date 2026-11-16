{% macro inicio_dia_operacional(dias_atras=0) %}
    {#- 07:00 (America/Sao_Paulo) do dia operacional de `dias_atras` dias antes do atual. -#}
    TIMESTAMP(
        DATETIME(DATE_SUB({{ data_operacional_atual() }}, INTERVAL {{ dias_atras }} DAY), TIME '07:00:00'),
        'America/Sao_Paulo'
    )
{% endmacro %}
