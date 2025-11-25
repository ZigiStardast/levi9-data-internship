WITH base AS (
  SELECT DISTINCT
    date(substr(time_date, 1, 10)) AS day,  
    location_name                  AS city,
    measurement_pm10Atmo,
    measurement_pm25Atmo,
    measurement_pm100Atmo,
    tourist_estimate
  FROM tara_pollution_enriched
)
SELECT
  day,
  city,
  SUM(COALESCE(measurement_pm10Atmo, 0)  * tourist_estimate) AS pollution_total_visitor_pm10,
  SUM(COALESCE(measurement_pm25Atmo, 0)  * tourist_estimate) AS pollution_total_visitor_pm25,
  SUM(COALESCE(measurement_pm100Atmo, 0) * tourist_estimate) AS pollution_total_visitor_pm100
FROM base
WHERE day = DATE '2022-05-02'  
GROUP BY
  day,
  city
ORDER BY
  city;

/* OUTPUT EXAMPLE
#	day	city	pollution_total_visitor_pm10	pollution_total_visitor_pm25	pollution_total_visitor_pm100
1	2022-05-02	Tătărași Sud, Iași, Romania	0.0	31437.75	32863.42999999999
*/


