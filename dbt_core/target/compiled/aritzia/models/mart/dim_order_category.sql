

select 
    order_category_id
   ,order_category
   ,CURRENT_DATE as updated_at
   ,CURRENT_DATE as created_at
   ,SESSION_USER() as updated_by
   ,SESSION_USER() as created_by
FROM (
    SELECT DISTINCT
        CAST(ABS(FARM_FINGERPRINT(CAST(order_category as STRING))) AS INT) as order_category_id
        ,order_category
    FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
 UNION ALL 
    
    SELECT -1
           ,'UNKWOWN'
    )
ORDER BY 1