{{ config(
    materialized='table'
    ) 
}}

SELECT DISTINCT 
     storage_location_id
    ,storage_location 
    ,CURRENT_DATE as updated_at
    ,CURRENT_DATE as created_at
    ,SESSION_USER() as updated_by
    ,SESSION_USER() as created_by
FROM(
    select DISTINCT 
    CAST(ABS(FARM_FINGERPRINT(CAST(from_storage_location as STRING))) as INT) as storage_location_id
    ,from_storage_location as storage_location
    FROM {{ ref("stg_transactions") }}  st
 UNION ALL 
    select DISTINCT 
     CAST(ABS(FARM_FINGERPRINT(CAST(to_storage_location as STRING))) AS INT) as storage_location_id
    ,to_storage_location storage_location  
    FROM {{ ref("stg_transactions") }}  st
UNION ALL 
    SELECT -1,
        'UNKWOWN'
)