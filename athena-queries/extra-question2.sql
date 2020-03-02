/* Make a latitude and longitude map view of pickups and dropoffs in the year 2010  */ 

SELECT 
   pickup_longitude, pickup_latitude
FROM "bd_nyc"."trips"
WHERE 
   pickup_longitude IS NOT NULL AND 
   pickup_latitude IS NOT NULL AND
   extract(year FROM cast(from_iso8601_timestamp(pickup_datetime) AS date)) = 2010