WITH date_spine AS (
    SELECT CAST(d AS DATE) AS date_id
    FROM generate_series(
        DATE '2016-01-01',
        DATE '2020-12-31',
        INTERVAL 1 DAY
    ) AS t(d)
)
SELECT
    date_id,
    CAST(EXTRACT(YEAR FROM date_id) AS INTEGER) AS year_num,
    CAST(EXTRACT(MONTH FROM date_id) AS INTEGER) AS month_num,
    CAST(EXTRACT(DAY FROM date_id) AS INTEGER) AS day_num,
    CAST(EXTRACT(QUARTER FROM date_id) AS INTEGER) AS quarter_num,
    CAST(EXTRACT(DOW FROM date_id) AS INTEGER) AS day_of_week,
    CAST(EXTRACT(WEEK FROM date_id) AS INTEGER) AS week_num,
    STRFTIME(date_id, '%Y-%m') AS year_month,
    CASE WHEN EXTRACT(DOW FROM date_id) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine
