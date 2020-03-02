/* 2.	Which are the 3 biggest vendors based on the total amount of money raised */

SELECT 
    vendor_id, CAST(SUM(total_amount) AS DECIMAL(15, 2)) AS money_raised
FROM "bd_nyc"."trips"
WHERE 
    total_amount > 0 AND 
    vendor_id IS NOT NULL
GROUP BY  
    vendor_id
ORDER BY  
    money_raised DESC LIMIT 3;