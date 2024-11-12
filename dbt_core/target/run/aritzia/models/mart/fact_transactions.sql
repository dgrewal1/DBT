
  
    

    create or replace table `aritzia-440802`.`pkg_mart`.`fact_transactions`
      
    
    

    OPTIONS()
    as (
      

SELECT 
   transaction_id
  ,warehouse_id
  ,st.transaction_date
  ,transaction_timestamp
  ,transaction_hour
  ,ot.order_type_id
  ,och.order_channel_id
  ,du.user_id
  ,dac.action_code_id
  ,da.article_id
  ,order_number
  ,order_line_item
  ,quantity
  ,inventory_detail_number
  ,ar.area_id as from_area_id
  ,str.storage_location_id as from_storage_location_id
  ,ar2.area_id as to_area_id
  ,str2.storage_location_id as to_storage_location_id
  ,CURRENT_DATE as updated_at
  ,CURRENT_DATE as created_at
  ,SESSION_USER() as updated_by
  ,SESSION_USER() as created_by
FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_date`  d 
  ON st.transaction_date = CAST(d.full_date as DATE)
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_order_type`  ot
  ON COALESCE(st.order_type,'UNKNOWN') = ot.order_type
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_order_channel`  och
  ON COALESCE(st.order_channel,'UNKNOWN') = och.order_channel
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_users`  du 
  ON COALESCE(st.user_id,'UNKNOWN') = du.user_id
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_action_code`  dac
  ON st.action_code = dac.action_code
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_articles`  da
  ON st.article = da.material_id and da.material_id <> -1
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_area`  ar
  ON COALESCE(st.from_area_code,'UNKNOWN') = ar.area_code
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_storage`  str
  ON COALESCE(st.from_storage_location,'UNKNOWN') = str.storage_location
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_area`  ar2
  ON COALESCE(st.to_area_code,'UNKNOWN') = ar2.area_code
LEFT JOIN `aritzia-440802`.`pkg_mart`.`dim_storage`  str2
  ON COALESCE(st.to_storage_location,'UNKNOWN') = str2.storage_location


    );
  