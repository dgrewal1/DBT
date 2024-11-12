

  create or replace view `aritzia-440802`.`pkg_mart`.`stg_packing_time_standards`
  OPTIONS()
  as with packing_time_standards as (
    select BASICMOVE_STANDARD,BASICPROC_STANDARD,BASICSCAN_STANDARD,BASICWRAP_STANDARD, BASICPACK_STANDARD,BELT_STANDARD,BLOUSE_STANDARD
                        ,COAT_STANDARD,DRESS_STANDARD,FOOTWEAR_STANDARD,HEADWEAR_STANDARD,HOSIERY_STANDARD,JACKET_STANDARD,NECKWEAR_STANDARD,PANT_STANDARD
                        ,SKIRT_STANDARD,SWEATER_STANDARD,TSHIRT_STANDARD,UNDERWEAR_STANDARD,BAG_STANDARD,GIFTITEM_STANDARD,HANDWEAR_STANDARD,SMALLGOOD_STANDARD
                        ,JEWELRY_STANDARD,SWIMWEAR_STANDARD/1.0 as SWIMWEAR_STANDARD 
     from `aritzia-440802`.`pkg_mart`.`packing_time_standards`
    )

 SELECT item
 ,CASE WHEN CONCAT(UPPER(SUBSTR(SPLIT(item, '_')[SAFE_OFFSET(0)], 1, 1)), LOWER(SUBSTR(SPLIT(item, '_')[SAFE_OFFSET(0)], 2))) LIKE '%Tshirt%' THEN 'T-shirt'
      ELSE CONCAT(UPPER(SUBSTR(SPLIT(item, '_')[SAFE_OFFSET(0)], 1, 1)), LOWER(SUBSTR(SPLIT(item, '_')[SAFE_OFFSET(0)], 2)))
  END 
    AS  derived_department
        ,packing_time
        ,CURRENT_DATE as updated_at
        ,CURRENT_DATE as created_at
        ,SESSION_USER() as updated_by
        ,SESSION_USER() as created_by
 FROM packing_time_standards 
UNPIVOT(packing_time FOR item IN (BASICMOVE_STANDARD,BASICPROC_STANDARD,BASICSCAN_STANDARD,BASICWRAP_STANDARD, BASICPACK_STANDARD,BELT_STANDARD,BLOUSE_STANDARD
                        ,COAT_STANDARD,DRESS_STANDARD,FOOTWEAR_STANDARD,HEADWEAR_STANDARD,HOSIERY_STANDARD,JACKET_STANDARD,NECKWEAR_STANDARD,PANT_STANDARD
                        ,SKIRT_STANDARD,SWEATER_STANDARD,TSHIRT_STANDARD,UNDERWEAR_STANDARD,BAG_STANDARD,GIFTITEM_STANDARD,HANDWEAR_STANDARD,SMALLGOOD_STANDARD
                        ,JEWELRY_STANDARD,SWIMWEAR_STANDARD));

