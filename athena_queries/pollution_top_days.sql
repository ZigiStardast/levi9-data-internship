-- Explorative query (top dani po zagađenju, bez duplikata)
WITH base AS (
  SELECT DISTINCT
    date(substr(time_date, 1, 10)) AS day,
    location_name                  AS city,
    measurement_pm10Atmo,
    measurement_pm25Atmo,
    measurement_pm100Atmo,
    tourist_estimate
  FROM tara_pollution_enriched
),
per_day_city AS (
  SELECT
    day,
    city,
    SUM(COALESCE(measurement_pm25Atmo, 0) * tourist_estimate) AS pollution_total_visitor_pm25
  FROM base
  GROUP BY
    day,
    city
)
SELECT
  day,
  city,
  pollution_total_visitor_pm25
FROM per_day_city
WHERE city = 'Tătărași Sud, Iași, Romania'
ORDER BY
  pollution_total_visitor_pm25 DESC
LIMIT 10;

/* OUTPUT EXAMPLE
#	day	city	pollution_total_visitor_pm25
1	2022-03-31	Tătărași Sud, Iași, Romania	168814.72
2	2022-04-30	Tătărași Sud, Iași, Romania	148046.16
3	2022-04-27	Tătărași Sud, Iași, Romania	146679.26
4	2022-04-28	Tătărași Sud, Iași, Romania	144015.68000000002
5	2022-04-29	Tătărași Sud, Iași, Romania	134275.96
6	2022-04-26	Tătărași Sud, Iași, Romania	122536.51000000001
7	2022-04-25	Tătărași Sud, Iași, Romania	108079.38
8	2022-04-21	Tătărași Sud, Iași, Romania	98066.95000000001
9	2022-04-22	Tătărași Sud, Iași, Romania	93202.83
10	2022-04-24	Tătărași Sud, Iași, Romania	90803.44
*/