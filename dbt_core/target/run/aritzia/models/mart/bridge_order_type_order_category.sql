
  
    

    create or replace table `aritzia-440802`.`pkg_mart`.`bridge_order_type_order_category`
      
    
    

    OPTIONS()
    as (
      

SELECT 
     ot.order_type_id
    ,oc.order_category_id
FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
INNER JOIN  `aritzia-440802`.`pkg_mart`.`dim_order_type`  ot
    on st.order_type = ot.order_type
INNER JOIN  `aritzia-440802`.`pkg_mart`.`dim_order_category`  oc
    on st.order_category = oc.order_category
order by 1,2
    );
  