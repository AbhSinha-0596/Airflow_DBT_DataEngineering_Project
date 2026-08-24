{{ config( severity = "warn" )}}

SELECT 1 
FROM 
    {{ ref('obt_b') }} AS obt
WHERE
    obt.order_id IS NULL
OR 
    obt.product_id IS NULL
OR
    obt.employee_id IS NULL
OR 
    obt.store_id IS NULL
OR 
    obt.order_item_id IS NULL
OR 
    obt.customer_id IS NULL
{#-
Goal: high-level objective of the model
Granularity: level of detail of the model
Assumptions/Caveats:
   - assumptions/caveats if any
-#}
