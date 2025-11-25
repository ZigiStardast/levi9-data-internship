-- Korelacija: da li vetar/kiša utiču na izloženost turista PM2.5?
WITH base_pollution AS (
  SELECT DISTINCT
    date(substr(time_date, 1, 10)) AS day,
    location_name                  AS city,
    measurement_pm25Atmo,
    tourist_estimate
  FROM tara_pollution_enriched
),
per_day_pollution AS (
  SELECT
    day,
    city,
    SUM(COALESCE(measurement_pm25Atmo, 0) * tourist_estimate) AS pollution_total_visitor_pm25
  FROM base_pollution
  GROUP BY
    day,
    city
),
hourly_weather AS (
  SELECT
    date(substr(time_date, 1, 10)) AS day,        
    location_name                  AS city,
    substr(time_date, 12, 8)       AS hh_mm_ss,     
    AVG(weather_wind_speed)        AS weather_wind_speed_hourly,
    AVG(weather_precipitation)     AS weather_precipitation_hourly
  FROM tara_weather_enriched
  GROUP BY
    date(substr(time_date, 1, 10)),
    location_name,
    substr(time_date, 12, 8)
),
per_day_weather AS (
  SELECT
    day,
    city,
    AVG(weather_wind_speed_hourly)                 AS avg_wind_speed,
    SUM(COALESCE(weather_precipitation_hourly, 0)) AS total_precipitation
  FROM hourly_weather
  GROUP BY
    day,
    city
),
joined AS (
  SELECT
    p.day,
    p.city,
    p.pollution_total_visitor_pm25,
    w.avg_wind_speed,
    w.total_precipitation
  FROM per_day_pollution p
  JOIN per_day_weather w
    ON p.day = w.day
   AND p.city = w.city
  WHERE p.city = 'Tătărași Sud, Iași, Romania'
)
SELECT
  corr(pollution_total_visitor_pm25, avg_wind_speed)      AS corr_exposure_wind,
  corr(pollution_total_visitor_pm25, total_precipitation) AS corr_exposure_rain
FROM joined;

/* OUTPUT EXAMPLE
#	corr_exposure_wind	corr_exposure_rain
1	-0.0057002344795891065	0.26770851785910105
*/