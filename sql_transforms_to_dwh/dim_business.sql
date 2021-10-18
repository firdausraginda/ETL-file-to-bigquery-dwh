SELECT
    yb.*,
    yc.date AS checkin_date
FROM
    `dummy-329203.project_1_ods.yelp_business` yb
LEFT JOIN 
    `dummy-329203.project_1_ods.yelp_checkin` yc
USING (business_id)