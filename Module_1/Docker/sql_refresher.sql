
-- Using implicit joins
SELECT 	
	 tpep_dropoff_datetime,
	 tpep_pickup_datetime, 
	 zdo."Zone", 
	 total_amount, 
	 CONCAT(zdo."Borough", ' / ',zdo."Zone" ) AS "dropoff_loc",
	 CONCAT(zpu."Borough", ' / ',zpu."Zone" ) AS "pickup_loc"
FROM 
	yellow_taxi_trips t
	zones zdo,
	zones zpu
WHERE 
	t."DOLocationID" = zdo."LocationID" AND
	t."PULocationID" = zpu."LocationID" 
LIMIT 100;

-- using left join [Pick all data from trips table even if it is missing matching data from the zones table ]

SELECT 	
	 tpep_dropoff_datetime,
	 tpep_pickup_datetime, 
	 zdo."Zone", 
	 total_amount, 
	 CONCAT(zdo."Borough", ' / ',zdo."Zone" ) AS "dropoff_loc",
	 CONCAT(zpu."Borough", ' / ',zpu."Zone" ) AS "pickup_loc"
FROM 
	yellow_taxi_trips t 
	LEFT JOIN zones zdo ON t."DOLocationID" = zdo."LocationID" 
	LEFT JOIN zones zpu ON t."PULocationID" = zpu."LocationID" 
LIMIT 100;

-- Group by
SELECT 	
	 CAST(tpep_dropoff_datetime AS DATE) as "day",
	 COUNT(1) AS "count"
	 FROM yellow_taxi_trips
GROUP BY "day"
ORDER BY "count" DESC;

SELECT 	
	 CAST(tpep_dropoff_datetime AS DATE) as "day",
	 COUNT(1) AS "count",
	 MAX(passenger_count)
	 FROM yellow_taxi_trips
GROUP BY "day"
ORDER BY "count" DESC;

SELECT 	
	 CAST(tpep_dropoff_datetime AS DATE) as "day",
	 COUNT(1) AS "count",
	 MAX(passenger_count),
	 MAX(total_amount)
	 FROM yellow_taxi_trips
GROUP BY "day"
ORDER BY "count" DESC;

SELECT 	
	 CAST(tpep_dropoff_datetime AS DATE) as "day",
	 COUNT(1) AS "num_of_trips",
	 "DOLocationID",
	 MAX(passenger_count),
	 MAX(total_amount)
	 FROM yellow_taxi_trips
GROUP BY 1,3
ORDER BY "num_of_trips" DESC, "DOLocationID" DESC ;
