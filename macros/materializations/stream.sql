{% materialization stream, adapter='snowflake' %}
    {# Validate model config #}
        {% set config_stream_source_name = config.require('stream_source_name') %}
        {% set config_stream_source_table_name = config.require('stream_source_table_name') %}
    
    {# Get the stream source from config #}
        {# Parse the stream_source config value #}
        {% set source_name = config.get('stream_source_name') %}
        {% set source_table_name = config.get('stream_source_table_name') %}

        {# Get the associated relation from the relation cache #}
        {% set source_relation = source(source_name, source_table_name) %}



    {# Create the new relation object #}
        {# Use the model alias with suffix as the relation identifier. Database and schema are inherited from the model config #}
        {% set identifier = model['alias'] %}


        {# Create the relation using available properties #}
        {% set target_relation = api.Relation.create(database=database
                                                    ,schema=schema
                                                    ,identifier=identifier
                                                    ,type='table') %}

        {# Set SQL command to execute for creation of stream object #}
            {% set sql %}
                create stream if not exists {{ target_relation }}
                on table {{ source_relation }}
            {% endset%}



    {# Execute statements against the database #}
        {# Run pre-hooks as defined on the model; hooks that do not need to be run inside the transaction are executed first #}
            {{ run_hooks(pre_hooks, inside_transaction=false) }}
            {{ run_hooks(pre_hooks, inside_transaction=true) }}

        {# Build and execute the statement to be executed using the relation and model SQL #}
            {% call statement('main') %}
                {{ sql }}
            {% endcall %}
    
        {# Run post-hooks as defined on the model; hooks that need to be run inside the transaction are executed first #}
            {{ run_hooks(post_hooks, inside_transaction=true) }}
        
        {# Commit the open transactions #}
            {{ adapter.commit() }}

        {# Execute post-hooks outside of the transaction #}
            {{ run_hooks(post_hooks, inside_transaction=false) }}



    {# Update the relation cache by returning the manipulated relation #}
        {{ return({'relations': [target_relation]}) }}

{% endmaterialization%}
