{% macro dedup_airbyte_cdc(relation, key='id') %}
    SELECT * EXCEPT (_dedup_ordem)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ key }}
                ORDER BY _ab_cdc_lsn DESC, _airbyte_extracted_at DESC
            ) AS _dedup_ordem
        FROM {{ relation }}
    )
    WHERE _dedup_ordem = 1
        AND _ab_cdc_deleted_at IS NULL
{% endmacro %}
