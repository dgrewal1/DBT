

with
    transactions as (
        SELECT 
            transaction_id
            ,warehouse_id
            ,transaction_timestamp
            ,CAST(date as DATE) as transaction_date
            ,hour as transaction_hour
            ,order_type
            ,order_type_category
            ,order_channel
            ,user_id
            ,action_code
            ,CASE WHEN article !='?' THEN CAST(article AS INT) ELSE 1 END as article
            ,SAFE_CAST(quantity as INT) as quantity
            ,order_number
            ,order_line_item
            ,order_category
            ,ship_line_id
            ,gift_flag
            ,from_area_code
            ,to_area_code
            ,from_storage_location
            ,to_storage_location
            ,inventory_detail_number
            ,CURRENT_DATE as updated_at
            ,CURRENT_DATE as created_at
            ,SESSION_USER() as updated_by
            ,SESSION_USER() as created_by
            --,ps.packing_time as estimated_packing_time,
        from `aritzia-440802`.`pkg`.`transactions`
    )

SELECT *
from transactions