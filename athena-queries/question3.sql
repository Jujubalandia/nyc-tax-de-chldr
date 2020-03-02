/* 3.	Make a histogram of the monthly distribution over 4 years of rides paid with cash */ 


SELECT 
    extract(year FROM cast(from_iso8601_timestamp(pickup_datetime) AS date)) AS ano, 
    extract(month FROM cast(from_iso8601_timestamp(pickup_datetime) AS date)) AS mes,
    count(payment_type) as num
FROM "bd_nyc"."trips"
WHERE 
    LOWER(payment_type) = 'cash'
GROUP BY 
    extract(year FROM cast(from_iso8601_timestamp(pickup_datetime) AS date)), 
    extract(month FROM cast(from_iso8601_timestamp(pickup_datetime) AS date))
ORDER BY  
    ano, mes ASC;