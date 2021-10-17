SELECT
    review_id,
    user_id,
    business_id,
    SAFE_CAST(stars AS INT64) AS stars,
    SAFE_CAST(useful AS INT64) AS useful,
    SAFE_CAST(funny AS INT64) AS funny,
    SAFE_CAST(cool AS INT64) AS cool,
    text,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date) AS date
FROM
    `dummy-329203.project_1_staging.yelp_review`