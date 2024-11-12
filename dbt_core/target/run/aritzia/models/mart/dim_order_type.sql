
  
    

    create or replace table `aritzia-440802`.`pkg_mart`.`dim_order_type`
      
    
    

    OPTIONS()
    as (
      

SELECT 
   order_type_id
  ,order_type
  ,order_type_category
  ,CURRENT_DATE as updated_at
  ,CURRENT_DATE as created_at
  ,SESSION_USER() as updated_by
  ,SESSION_USER() as created_by
FROM(
SELECT DISTINCT
   CAST(ABS(FARM_FINGERPRINT(CONCAT(CAST(order_type as STRING),CAST(order_type_category as STRING) ))) AS INT) as order_type_id
  ,order_type
  ,order_type_category
 -- ,order_category
 FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
 UNION ALL 
    
    SELECT -1
        ,'UNKNOWN'
        ,'UNKNOWN'
    )
ORDER BY 1
    );
  