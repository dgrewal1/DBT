
  
    

    create or replace table `aritzia-440802`.`pkg_mart`.`dim_orders`
      
    
    

    OPTIONS()
    as (
      

SELECT DISTINCT
   ABS(FARM_FINGERPRINT(CAST(order_number as STRING))) as order_key
  ,order_number
  ,CURRENT_DATE as updated_at
  ,CURRENT_DATE as created_at
  ,SESSION_USER() as updated_by
  ,SESSION_USER() as created_by
 FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
    );
  