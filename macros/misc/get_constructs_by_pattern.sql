{% macro get_constructs_by_pattern(construct_type, schema_pattern, name_pattern, exclude='',database=target.database) %}
    {{ return(adapter.dispatch('get_constructs_by_pattern', 'edw')(construct_type, schema_pattern, name_pattern, exclude, database)) }}
{% endmacro %}

{% macro default__get_constructs_by_pattern(construct_type, schema_pattern, name_pattern, exclude='', database=target.database) %}

    {# Set SQL statement to return constructs with argument datatypes #}
    {% set get_constructs_sql %}
        with cte_constructs as (
            select  '{{ construct_type }}' as "construct_type"
                    ,{{ construct_type }}_catalog as "construct_database"
                    ,{{ construct_type }}_schema as "construct_schema"
                    ,{{ construct_type }}_name as "construct_name"
                    ,regexp_replace(argument_signature, '\\(?([,]\\s*)?\\w*\\s*(\\w*)\\)?', '\\1\\2') as "construct_signature"
            from {{ database }}.information_schema.{{ construct_type }}s {# Pluralize the type with 's' suffix #}
        )

        select *
        from cte_constructs
        where 1=1
        and "construct_schema" ilike '{{ schema_pattern }}'
        and "construct_name" ilike '{{ name_pattern }}'
        and "construct_name" not ilike '{{ exclude }}'
    {% endset %}

    
    {# Execute the SQL statement and populate result set to variable #}
    {%- call statement('get_constructs', fetch_result=True) %}
        {{ get_constructs_sql }}
    {%- endcall -%}

    {%- set construct_list = load_result('get_constructs') -%}
    


    {# Populate construct results in a new variable as relations for return #}
    {%- if construct_list and construct_list['table'] -%}
        {%- set construct_relations = [] -%}
        
        {%- for row in construct_list['table'] -%}
            {%- set construct_relation = api.Relation.create(
                                                database=database,
                                                schema=row.construct_schema,
                                                identifier=row.construct_name ~ '(' ~ row.construct_signature ~ ')'
                ) -%}
            
            {%- do construct_relations.append(construct_relation) -%}
        {%- endfor -%}

        {{ return(construct_relations) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}
