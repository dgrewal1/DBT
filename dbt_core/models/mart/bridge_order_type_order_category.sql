
{{ config(
    materialized='table'
    ) 
}}

SELECT 
     ot.order_type_id
    ,oc.order_category_id
FROM {{ ref("stg_transactions") }}  st
INNER JOIN  {{ ref("dim_order_type") }}  ot
    on st.order_type = ot.order_type
INNER JOIN  {{ ref("dim_order_category") }}  oc
    on st.order_category = oc.order_category
order by 1,2