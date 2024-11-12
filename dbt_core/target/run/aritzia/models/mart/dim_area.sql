
  
    

    create or replace table `aritzia-440802`.`pkg_mart`.`dim_area`
      
    
    

    OPTIONS()
    as (
      

SELECT DISTINCT 
     area_id
    ,area_code 
    ,CURRENT_DATE as updated_at
    ,CURRENT_DATE as created_at
    ,SESSION_USER() as updated_by
    ,SESSION_USER() as created_by
FROM(
    select DISTINCT 
    CAST(ABS(FARM_FINGERPRINT(CAST(from_area_code as STRING))) AS INT) as area_id
    ,from_area_code AS area_code
    FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
 UNION ALL 
    select DISTINCT 
    ABS(FARM_FINGERPRINT(CAST(to_area_code as STRING))) as area_id
    ,to_area_code AS area_code  
  FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
  
 UNION ALL 
    
    SELECT -1,
        'UNKWOWN'
    
)
order by 1
    );
  