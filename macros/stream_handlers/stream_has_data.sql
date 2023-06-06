{% macro stream_has_data(relation) %}
    {# Run the check query #}
    {% set stream_check_results = run_query("select SYSTEM$STREAM_HAS_DATA('" ~ relation ~ "') as stream_has_data") %}
    
    {# Parse result as boolean #}
    {% set stream_check_result_value = stream_check_results.columns[0].values()[0] %}
    {% set stream_has_data = true if stream_check_result_value|lower == 'true' else false %}

    {# Return result #}
    {{ return(stream_has_data) }}
{% endmacro %}
