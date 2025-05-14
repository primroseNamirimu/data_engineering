SELECT * FROM green_taxi_trips WHERE DATE_PART('month',lpep_dropoff_datetime::DATE) = 11  LIMIT 100;


-- Question 3
-- Vertical
SELECT 
	CASE
		WHEN trip_distance <= 1 THEN 'UP TO 1'
		WHEN trip_distance > 1 AND trip_distance <= 3 THEN 'between_1_and_3'
		WHEN trip_distance > 3 AND trip_distance <= 7 THEN 'between_3_and_7'
		WHEN trip_distance > 7 AND trip_distance <= 10 THEN 'between_7_and_10'
		ELSE  'over 10' 
	END AS segments,
		COUNT (1) AS num_trips
		
	FROM green_taxi_trips
	WHERE lpep_pickup_datetime >= '2019-10-01' AND 
	lpep_pickup_datetime < '2019-11-01' AND
	lpep_dropoff_datetime >= '2019-10-01' AND 
	lpep_dropoff_datetime < '2019-11-01'
	group by segments;
	
-- Horizontal
SELECT 
	COUNT(CASE WHEN trip_distance <= 1 THEN 1 ELSE NULL END) AS "UP TO 1",
	COUNT(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE NULL END) AS "between_1_and_3" ,
	COUNT(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE NULL END) AS "between_3_and_7" ,
	COUNT(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE NULL END) AS "between_7_and_10" ,
	COUNT(CASE WHEN trip_distance > 10 THEN 1 ELSE NULL END) AS "over 10" 
	
	FROM green_taxi_trips
	WHERE lpep_pickup_datetime >= '2019-10-01' AND 
	lpep_pickup_datetime < '2019-11-01' AND
	lpep_dropoff_datetime >= '2019-10-01' AND 
	lpep_dropoff_datetime < '2019-11-01';


SELECT * FROM green_taxi_trips LIMIT 200;
-- Question 4
SELECT CAST(lpep_pickup_datetime AS DATE) l, MAX(trip_distance) AS "lt" FROM green_taxi_trips GROUP BY l ORDER BY "lt" DESC LIMIT 1;

-- Question 4
SELECT 
	taxi_zones."Zone", round(SUM(total_amount)::numeric,2) AS "sum"
	FROM green_taxi_trips 
	INNER JOIN taxi_zones 
	ON taxi_zones."LocationID" = green_taxi_trips."PULocationID"
	WHERE lpep_pickup_datetime::date = '2019-10-18' GROUP BY green_taxi_trips."PULocationID", taxi_zones."Zone" ORDER BY "sum" DESC LIMIT 3;


-- Question 5
SELECT 
	taxi_zones."Zone", round(MAX(tip_amount)::numeric,2) AS "larget_tip"
	FROM green_taxi_trips 
	INNER JOIN taxi_zones 
	ON taxi_zones."LocationID" = green_taxi_trips."DOLocationID"
	WHERE taxi_zones."Zone" = 'East Harlem North' AND DATE_PART('month',lpep_dropoff_datetime::DATE) = 10
	GROUP BY green_taxi_trips."DOLocationID", taxi_zones."Zone" 
	ORDER BY "larget_tip" DESC LIMIT 3;












