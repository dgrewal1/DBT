
  
    

    create or replace table `aritzia-440802`.`pkg_mart`.`dim_users`
      
    
    

    OPTIONS()
    as (
      

SELECT 
     user_id
    ,CURRENT_DATE as updated_at
    ,CURRENT_DATE as created_at
    ,SESSION_USER() as updated_by
    ,SESSION_USER() as created_by
 FROM (
    SELECT DISTINCT
        user_id
    FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st

    UNION ALL

    SELECT '-1' as user_id
) ORDER BY 1
    );
  