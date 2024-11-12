

with
    staging as (
        select
            ABS(FARM_FINGERPRINT(CONCAT(CAST(material as STRING),CAST(department as STRING)))) as article_id
            ,material
            ,department
            ,CURRENT_DATE as updated_at
            ,CURRENT_DATE as created_at
            ,SESSION_USER() as updated_by
            ,SESSION_USER() as created_by
        from `aritzia-440802`.`pkg`.`articles`
    )

select *
from staging