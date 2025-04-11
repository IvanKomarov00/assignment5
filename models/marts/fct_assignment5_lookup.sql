{{ 
  config(
    materialized='incremental',
    unique_key='title'
  ) 
}}

SELECT
  title,
  MIN(DATE(datehour)) AS min_date,
  MAX(DATE(datehour)) AS max_date,
  SUM(views) AS total_views,
  CURRENT_TIMESTAMP() AS insert_time
FROM {{ source('test_dataset', 'assignment5_input') }}
WHERE wiki IN ('uk', 'uk.m')
{% if is_incremental() %}
  AND DATE(datehour) >= (
    SELECT DATE_SUB(MAX(max_date), INTERVAL 1 DAY)
    FROM {{ this }}
  )
{% endif %}
GROUP BY title