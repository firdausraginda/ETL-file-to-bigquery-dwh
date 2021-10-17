SELECT
    PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date,
    SAFE_CAST(precipitation AS FLOAT64) AS precipitation,
    SAFE_CAST(precipitation_normal AS FLOAT64) AS precipitation_normal,
FROM
    `dummy-329203.project_1_staging.precipitation_inch`