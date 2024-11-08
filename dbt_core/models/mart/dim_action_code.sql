{{ config(
    materialized='table'
    ) 
}}

SELECT 
     action_code_id
    ,action_code
    ,CURRENT_DATE as updated_at
    ,CURRENT_DATE as created_at
    ,SESSION_USER() as updated_by
    ,SESSION_USER() as created_by
FROM (

    SELECT DISTINCT
    CAST(ABS(FARM_FINGERPRINT(CAST(action_code as STRING))) AS INT) as action_code_id
    ,action_code
    FROM {{ ref("stg_transactions") }}  st
 
 UNION ALL 
    SELECT -1
       , 'UNKWOWN'
    )
order by 1