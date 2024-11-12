{{ config(
    
    materialized='table'
    ) 
}}

with pkoclose_transactions as 
(
    SELECT 
        transaction_id
        ,warehouse_id
        ,transaction_date
        ,transaction_timestamp
        ,transaction_hour
        ,user_id
        ,a.article_department
        ,CASE WHEN a.estimated_packing_time IS NOT NULL THEN estimated_packing_time
              ELSE (SELECT ROUND(avg(estimated_packing_time),2) 
                    FROM {{ ref("dim_articles") }} WHERE estimated_packing_time is not null
                  )
         END as estimated_packing_time
        ,order_number
        ,order_line_item
        ,quantity
        ,SUM(CAST(quantity AS INT)) OVER(PARTITION BY user_id, order_number) as total_quantity_order
    FROM {{ ref("fact_transactions") }}  ft
    LEFT JOIN {{ ref("dim_articles") }}  a
        ON ft.article_id = a.article_id
    LEFT JOIN {{ ref("dim_action_code") }}  ac
        ON ft.action_code_id = ac.action_code_id
    WHERE ac.action_code = 'PKOCLOSE'
    ORDER BY order_number,transaction_timestamp
)
,
--identify the first packing_end_time for each order and then use it as the end time for the previous order group
  packing_start_time_ as (
SELECT   transaction_id,
         order_number
        ,order_line_item
        ,transaction_timestamp AS  packing_end_time
        ,article_department
        ,estimated_packing_time as estimated_packing_time
        ,user_id
        ,row_number() OVER(PARTITION BY user_id,order_number ORDER BY transaction_timestamp,order_line_item) as rn
        ,quantity
FROM pkoclose_transactions
--WHERE USER_ID= 'USER13'
ORDER BY transaction_timestamp,1
)
--SELECT * FROM packing_start_time_ order by 7,4

--get packing start time of each order,  first order for each user will have no packing start time so MAX(8AM, pack_end_time - 3 min)
, packing_end_time as (
  SELECT user_id
        ,order_number
        ,COALESCE(LAG(packing_end_time) OVER(PARTITION BY user_id ORDER BY packing_end_time,order_number)
                    , CASE WHEN TIMESTAMP_SUB(packing_end_time, INTERVAL 2 MINUTE) < CAST('2021-11-02 08:00:00' AS TIMESTAMP)
                           THEN CAST('2021-11-02 08:00:00' AS TIMESTAMP)
                           ELSE TIMESTAMP_SUB(packing_end_time, INTERVAL 2 MINUTE)
                       END ) as packing_start_time  
        ,packing_end_time
  FROM packing_start_time_
  WHERE rn=1
  ORDER BY packing_start_time
)

--select * from packing_end_time ORDER BY 1,4

--use total quantity per order and quantity to include weight per order
, actual_packing_time_cte as (
select t.transaction_id
       ,t.order_number
       ,t.article_department
       ,t.user_id
       ,t.quantity
       ,t.total_quantity_order
       --,ROUND(CAST(t.quantity AS INT) / t.total_quantity_order,2) as item_weight
       ,pet.packing_start_time
       ,pet.packing_end_time
       --,DATE_DIFF(pet.packing_end_time,pet.packing_start_time, SECOND) as diff_packing_end_start_time
       ,ROUND(DATE_DIFF(pet.packing_end_time,pet.packing_start_time, SECOND) * ROUND(CAST(t.quantity AS INT) / t.total_quantity_order,2),2) as actual_packing_time
       ,t.estimated_packing_time 
       ,t.estimated_packing_time * quantity as total_estimated_packing_time 
       , ROUND(SAFE_DIVIDE(t.estimated_packing_time * quantity, 
           (ROUND(DATE_DIFF(pet.packing_end_time,pet.packing_start_time, SECOND) * ROUND(CAST(t.quantity AS INT) / t.total_quantity_order,2),2))) 
           ,2) as packing_efficiency
FROM pkoclose_transactions t 
INNER JOIN packing_end_time pet 
  ON  t.user_id = pet.user_id AND t.order_number = pet.order_number 
)
--select * from actual_packing_time_cte ORDER BY 8

select 
       --*
       user_id
      ,transaction_id
      ,article_department
      ,order_number
      ,packing_start_time
      ,packing_end_time
      ,quantity
      ,total_quantity_order
      ,actual_packing_time
      ,estimated_packing_time
      ,total_estimated_packing_time
      ,packing_efficiency
      ,ROUND(SUM(actual_packing_time) OVER(PARTITION BY order_number ORDER BY order_number)/60,2) as total_packing_time_order   
from actual_packing_time_cte
order by user_id,packing_end_time
