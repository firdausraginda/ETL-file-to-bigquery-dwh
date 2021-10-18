SELECT
    PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date,
    SAFE_CAST(min AS FLOAT64) AS min,
    SAFE_CAST(max AS FLOAT64) AS max,
    SAFE_CAST(normal_min AS FLOAT64) AS normal_min,
    SAFE_CAST(normal_max AS FLOAT64) AS normal_max
FROM
    `dummy-329203.project_1_staging.temperature_degreef`