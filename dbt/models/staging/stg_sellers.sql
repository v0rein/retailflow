SELECT
    seller_id,
    CAST(seller_zip_code_prefix AS VARCHAR) AS seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('silver', 'sellers') }}
