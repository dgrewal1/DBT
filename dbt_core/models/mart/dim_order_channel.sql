
{{ config(
    materialized='table'
    ) 
}}

select 
    order_channel_id
   ,order_channel
   ,CURRENT_DATE as updated_at
   ,CURRENT_DATE as created_at
   ,SESSION_USER() as updated_by
   ,SESSION_USER() as created_by
FROM (
    SELECT DISTINCT
        CAST(ABS(FARM_FINGERPRINT(CAST(order_channel as STRING))) AS INT) as order_channel_id
        ,order_channel
    FROM {{ ref("stg_transactions") }}  st
 UNION ALL 
    
    SELECT -1
           ,'UNKWOWN'
    )
ORDER BY 1