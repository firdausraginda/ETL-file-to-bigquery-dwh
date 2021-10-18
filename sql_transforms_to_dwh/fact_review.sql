WITH
    yelp_review_extract_date AS (
    SELECT
        *,
        EXTRACT(DATE FROM date) AS date_date
    FROM
        `dummy-329203.project_1_ods.yelp_review`
    ),
    yelp_tip_extract_date AS (
    SELECT
        *,
        EXTRACT(DATE FROM date) AS date_date
    FROM
        `dummy-329203.project_1_ods.yelp_tip`
    )

SELECT
    CASE
        WHEN yr.review_id IS NULL THEN yt.tip_id
        ELSE yr.review_id
    END AS fact_table_id,

    CASE
        WHEN yr.user_id IS NULL THEN yt.user_id
        ELSE yr.user_id
    END AS user_id,

    CASE
        WHEN yr.business_id IS NULL THEN yt.business_id
        ELSE yr.business_id
    END AS business_id,

    CASE
        WHEN yr.date_date IS NULL THEN yt.date_date
        ELSE yr.date_date
    END AS date,

    yt.compliment_count,

    yr.stars,
    yr.useful,
    yr.funny,
    yr.cool,

    yr.text AS review_text,
    yt.text AS tip_text
FROM
    yelp_review_extract_date yr
FULL OUTER JOIN
    yelp_tip_extract_date yt 
USING (user_id, business_id, date_date)