SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS product_category_name_english,
    CAST(p.product_weight_g AS DOUBLE) AS product_weight_g,
    CAST(p.product_length_cm AS DOUBLE) AS product_length_cm,
    CAST(p.product_height_cm AS DOUBLE) AS product_height_cm,
    CAST(p.product_width_cm AS DOUBLE) AS product_width_cm,
    CAST(p.product_photos_qty AS INTEGER) AS product_photos_qty
FROM {{ source('silver', 'products') }} p
LEFT JOIN {{ source('silver', 'product_category_translation') }} t
    ON p.product_category_name = t.product_category_name
