

with combined_articles as (
    SELECT DISTINCT
        COALESCE(material,-1) as material_id
        ,COALESCE(department,derived_department) as article_department
        ,packing_time as estimated_packing_time
    from  `aritzia-440802`.`pkg_mart`.`stg_articles` a
    LEFT JOIN `aritzia-440802`.`pkg_mart`.`stg_packing_time_standards` pts
        ON a.department = pts.derived_department
    UNION ALL
    SELECT DISTINCT 
        CAST(article as INT) as material_id
       ,'UNKNOWN' as article_department
       ,NULL as estimated_packing_time
    FROM `aritzia-440802`.`pkg_mart`.`stg_transactions`  st
    LEFT JOIN `aritzia-440802`.`pkg_mart`.`stg_articles` a
        on st.article = CAST(a.material  AS INT)
    WHERE article IS NOT NULL
    and a.material IS NULL --to get only distinct articles
)

SELECT DISTINCT 
        CAST(COALESCE(ABS(FARM_FINGERPRINT(CAST(material_id as STRING))),-1) AS INT) AS article_id
        ,material_id
        ,article_department
        ,estimated_packing_time
        ,CURRENT_DATE as updated_at
        ,CURRENT_DATE as created_at
        ,SESSION_USER() as updated_by
        ,SESSION_USER() as created_by
from combined_articles
ORDER BY 1,2