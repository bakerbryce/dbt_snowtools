{% macro stream_is_stale(source_relation,stream_relation) %}
    {# Set the sql statement to get stream details #}
    {% set sql_get_stream_detail %}
        show streams like '{{ stream_relation.identifier }}' in schema {{ stream_relation.database ~ "." ~ stream_relation.schema}}
    {% endset %}


    {# Check if the stream is stale #}
    {% set result = run_query(sql_get_stream_detail) %}
    {% set result_value = result.columns['stale'].values()[0] %}
    {% set is_stale = true if result_value|lower == 'true' else false %}


    {# Rebuild stream if needed #}
    {% if is_stale %}
        {# Output the exception details #}
        {{ log("INFO: Stale stream detected. Rebuilding: " ~ stream_relation, info=true) }}
        
        {# Rebuild stream #}
        {% do rebuild_stream(source_relation,stream_relation) %}
    {% endif %}

    {{ return(is_stale) }}
{% endmacro %}