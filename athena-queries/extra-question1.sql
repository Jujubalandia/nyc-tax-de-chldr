/* 	What is the average trip time on Saturdays and Sundays */

/* SATURDAY 6  SUNDAY 7 */  
SELECT 
    SUM(date_diff('minute', from_iso8601_timestamp(pickup_datetime), from_iso8601_timestamp(dropoff_datetime)))/COUNT(dropoff_datetime) AS avg_duration
FROM "bd_nyc"."trips"
WHERE 
    extract(dow FROM date (cast(from_iso8601_timestamp(pickup_datetime) AS date))) BETWEEN 6 AND 7;
