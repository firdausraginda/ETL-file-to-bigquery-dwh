SELECT
    CASE
        WHEN pi.date IS NULL THEN td.date
        ELSE pi.date
    END AS date,
    
    pi.precipitation,
    pi.precipitation_normal,

    td.min AS temperature_min,
    td.max AS temperature_max,
    td.normal_min AS temperature_normal_min,
    td.normal_max AS temperature_normal_max
    
FROM
    `dummy-329203.project_1_ods.precipitation_inch` pi
FULL OUTER JOIN
    `dummy-329203.project_1_ods.temperature_degreef` td
USING (date)