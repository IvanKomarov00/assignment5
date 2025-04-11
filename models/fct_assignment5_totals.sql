{{ 
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
  ) 
}}

-- Debug: max_partition = '{{ dbt.max_partition }}'


WITH daily_views_by_title AS (
    SELECT 
        DATE(datehour) AS date,
        title,
        SUM(views) AS views,
        CURRENT_TIMESTAMP() AS insert_time
    FROM {{ source('test_dataset', 'assignment5_input') }}
    WHERE wiki IN ('uk', 'uk.m')
    
    {% if is_incremental() and dbt.max_partition %}
      AND DATE(datehour) >= DATE_SUB(DATE('{{ dbt.max_partition }}'), INTERVAL 1 DAY)
    {% endif %}

    GROUP BY date, title
)

SELECT * FROM daily_views_by_title

