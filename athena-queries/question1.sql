/* 1.	What is the average distance traveled by trips with a maximum of 2 passengers */ 

SELECT 
    ROUND(SUM(trip_distance)/COUNT(trip_distance),2) as avg_distance 
FROM "bd_nyc"."trips" 
WHERE 
    passenger_count <= 2 AND 
    trip_distance > 0;