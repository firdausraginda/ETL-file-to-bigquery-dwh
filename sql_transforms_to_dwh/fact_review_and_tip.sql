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

    yr.date AS review_date,
    yt.date AS tip_date,

    yt.compliment_count,

    yr.stars,
    yr.useful,
    yr.funny,
    yr.cool,

    yr.text AS review_text,
    yt.text AS tip_text
FROM
    `dummy-329203.project_1_ods.yelp_review` yr
FULL OUTER JOIN
    `dummy-329203.project_1_ods.yelp_tip` yt 
USING (user_id, business_id)