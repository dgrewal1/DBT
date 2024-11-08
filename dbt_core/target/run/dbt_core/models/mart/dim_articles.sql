
  
    

    create or replace table `aritzia-440802`.`staging`.`dim_articles`
      
    
    

    OPTIONS()
    as (
      

with combined_articles as (
    select 
         material
        ,COALESCE(department,derived_department) as department
        ,packing_time
        ,current_datetime as created_at
        ,current_datetime as updated_at
        ,SESSION_USER() as created_by
        ,SESSION_USER() as updated_by
    from  `aritzia-440802`.`staging`.`stg_articles` a
    FULL JOIN `aritzia-440802`.`staging`.`stg_packing_time_standards` pts
        ON a.department = pts.derived_department
        
)


select * from combined_articles
    );
  