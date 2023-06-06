{# Auto-generated Bronze stream #}

{# Model configuration #}
{{ config(materialized='stream',
          stream_source_name='source_name',
          stream_source_table_name='source_table_name') }}

{# Model definition #}
--depends_on: {{ source('source_name','source_table_name') }}
