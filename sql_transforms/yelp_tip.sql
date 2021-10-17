SELECT
    concat(user_id, '-', business_id) as tip_id,
    user_id,
    business_id,
    text,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date) AS date,
    SAFE_CAST(compliment_count AS INT64) AS compliment_count
FROM
    `dummy-329203.project_1_staging.yelp_tip`