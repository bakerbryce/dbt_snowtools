{% macro rebuild_stream(source_relation,stream_relation) %}
    {# Set stream rebuild sql statement #}
    {% set sql_create_or_replace_stream %}
        create or replace stream {{ stream_relation }}
        on table {{ source_relation }}
    {% endset %}

    {# Execute #}
    {% do run_query(sql_create_or_replace_stream) %}
{% endmacro %}