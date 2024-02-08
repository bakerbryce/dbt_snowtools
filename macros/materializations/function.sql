{% materialization function, adapter='snowflake' %}

    {# Set the construct type to use when defining the materialization type #}
    {% set construct_type = 'function' %}

    
    {# Use the model alias as the relation identifier. Database and schema are inherited from the model config #}
    {%- set identifier = model['alias'] -%}

    {# Create the relation using available properties #}
    {%- set target_relation = api.Relation.create(database=database,
                                                  schema=schema,
                                                  identifier=identifier
                                                  ) -%}


    {# Get any existing relations with the same name (regardless of arguments), then drop them #}
        
        {# Get all relations in the database that match on construct name #}
        {%- set source_relations = edw.get_constructs_by_pattern(construct_type=construct_type
                                                                ,database=database
                                                                ,schema_pattern=schema
                                                                ,name_pattern=identifier ~ '%') -%}


        {# Drop any existing relations #}
        {% for relation in source_relations %}
            {% call statement('drop_relation') %}
                drop {{ construct_type }} if exists {{ relation }}
            {% endcall %}        
        {% endfor %}


    {# Run pre-hooks as defined on the model; hooks that do not need to be run inside the transaction are executed first #}
    {{ run_hooks(pre_hooks, inside_transaction=false) }}
    {{ run_hooks(pre_hooks, inside_transaction=true) }}


    {#- Build and execute the statement to be executed using the relation and model SQL -#}
    {% call statement('main') -%}
        create or replace {{ construct_type }} {{ target_relation }}
        {{ sql }}
    {% endcall -%}


    {# Run post-hooks as defined on the model; hooks that need to be run inside the transaction are executed first #}
    {{ run_hooks(post_hooks, inside_transaction=true) }}
    
    {# Commit the open transactions #}
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=false) }}



    {# Update the relation cache by returning the manipulated relation #}
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}