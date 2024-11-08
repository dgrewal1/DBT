
{{ config(
    
    materialized='table'
    ) 
}}


SELECT DISTINCT 
      user_id
      ,ROUND(AVG(packing_efficiency) OVER(PARTITION BY user_id ),2) as avg_packing_efficieny
      ,ROUND(SUM(actual_packing_time) OVER(PARTITION BY user_id )/60,2) as total_packing_time_by_user
      ,COUNT(DISTINCT order_number) OVER(PARTITION BY user_id ) total_orders_packed_by_user   
      ,COUNT(*) OVER(PARTITION BY user_id ORDER BY user_id) total_articles_packed_by_user   
FROM {{ ref("fact_transaction_packing_efficiency") }}  ft
order by 1