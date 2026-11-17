SELECT
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS VARCHAR) AS customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('silver', 'customers') }}
