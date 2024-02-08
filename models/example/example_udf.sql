{#- Model configuration overrides -#}
{{ config(materialized='function') }}

{#- Construct parameters. Leave parentheses empty if no parameters will be passed. -#}
(date_arg date)

{# Construct config header -#}
returns table (
  COLUMN1 VARCHAR,
  COLUMN2 VARCHAR
)
as

{# Construct definition #}
$$
select
  COLUMN1,
  COLUMN2
from {{ ref('some_model') }}
$$  
