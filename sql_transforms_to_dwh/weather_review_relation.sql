WITH
    join_table as (
    SELECT
        CASE
            WHEN fr.date IS NULL THEN dw.date
            ELSE fr.date
        END AS date,

        fr.stars,
        fr.useful,
        fr.funny,
        fr.cool,

        dw.precipitation,
        dw.temperature_min,
        dw.temperature_max

    FROM
        `dummy-329203.project_1_dwh.fact_review` fr
    FULL OUTER JOIN
        `dummy-329203.project_1_dwh.dim_weather` dw
    USING (DATE)
    )

select 
    date_trunc(date, MONTH) AS month,
    sum(stars) AS stars,
    sum(useful) AS useful,
    sum(funny) AS funny,
    sum(cool) AS cool,
    sum(stars) + sum(useful) + sum(funny) + sum(cool) AS total_review_score,
    sum(precipitation) AS precipitation,
    sum(temperature_min) AS temperature_min,
    sum(temperature_max) AS temperature_max
from join_table
WHERE
    stars is not null
    or useful is not null 
    or funny is not null
    or cool is not null
group by 1
order by 1