/* 4.	Make a time series chart computing the number of tips each day for the last 3 months of 2012 */

SELECT 
    extract(day FROM cast(from_iso8601_timestamp(pickup_datetime) AS date)) AS day, 
    count(tip_amount) as numtipday
FROM "bd_nyc"."trips"
WHERE 
    tip_amount > 0 AND
    extract(year FROM cast(from_iso8601_timestamp(pickup_datetime) AS date)) = 2012 AND
    extract(month FROM cast(from_iso8601_timestamp(pickup_datetime) AS date)) BETWEEN 10 AND 12
GROUP BY 
    extract(day FROM cast(from_iso8601_timestamp(pickup_datetime) AS date))
ORDER BY  
    day ASC; 
