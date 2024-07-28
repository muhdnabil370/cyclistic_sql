CREATE DATABASE cyclistic_sql

use cyclistic_sql

SELECT * FROM [jan23]

SELECT COUNT(*) FROM jan23
SELECT COUNT(*) FROM feb23
SELECT COUNT(*) FROM total_dataset_2023



/* combine all file into one file */
SELECT * INTO total_dataset_2023
FROM jan23
UNION ALL
SELECT * FROM feb23

INSERT INTO total_dataset_2023
SELECT * FROM dec23


/* update data type for ride_length
 convert from time to VARCHAR*/
SELECT
	ride_id,
	started_at,
	ended_at,
	CAST(DATEDIFF(MINUTE,started_at,ended_at) AS VARCHAR) AS ride_length, 
	DATEPART(DAY,started_at) AS ride_date,
	DATEPART(MONTH,started_at) AS ride_month,
	DATEPART(YEAR,started_at) AS ride_year,
	DATEPART(HOUR,started_at) AS start_time,
	DATEPART(HOUR,ended_at) AS end_time,
	DATEPART(WEEKDAY,started_at) AS Day_of_week,
	start_station_name,
	start_station_id,
	end_station_name,
	end_station_id,
	start_lat,
	start_lng,
	end_lat,
	end_lng,
	member_casual
FROM 
	total_dataset_2023
ORDER BY
		ride_id DESC;

/* Count distinct in temporary table*/
WITH distinct_total_dataset_2023 AS
	(
	SELECT
		DATEPART(MONTH,started_at) AS period,
		COUNT(DISTINCT ride_id) AS ride_total
	FROM
		total_dataset_2023
	GROUP BY
		DATEPART(MONTH,started_at)
	)
SELECT * FROM distinct_total_dataset_2023

/*CREATE NEW TABLE FOR total_dataset_2023*/
SELECT
	ride_id,
	started_at,
	ended_at,
	CAST(DATEDIFF(MINUTE,started_at,ended_at) AS VARCHAR) AS ride_length, 
	DATEPART(DAY,started_at) AS ride_date,
	DATEPART(MONTH,started_at) AS ride_month,
	DATEPART(YEAR,started_at) AS ride_year,
	DATEPART(HOUR,started_at) AS start_time,
	DATEPART(HOUR,ended_at) AS end_time,
	DATEPART(WEEKDAY,started_at) AS Day_of_week,
	start_station_name,
	start_station_id,
	end_station_name,
	end_station_id,
	start_lat,
	start_lng,
	end_lat,
	end_lng,
	member_casual
INTO
	new_total_dataset
FROM 
	total_dataset_2023

/*update format for 'day_of week form float to string'*/

SELECT
	ride_id,
	started_at,
	ended_at,
	CAST(ride_length AS int) AS ride_length,
	ride_date,
	ride_month,
	ride_year,
	start_time,
	end_time,
	CAST(Day_of_week AS varchar) AS Day_of_week,
	start_station_name,
	start_station_id,
	end_station_name,
	end_station_id,
	start_lat,
	start_lng,
	end_lat,
	end_lng,
	member_casual
FROM 
	new_total_dataset

/*Total trips: member vs casual */

SELECT
	TotalTrips,
	TotalMemberTrips,
	TotalCasualTrips,
	ROUND((TotalMemberTrips *1.0/TotalTrips)* 100,2) AS MemberPercentage,
	ROUND((TotalCasualTrips *1.0/TotalTrips)* 100,2) AS CasualPercentage
FROM
	(
	SELECT
		COUNT(ride_id) AS TotalTrips,
		SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END) AS TotalMemberTrips,
		SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END) AS TotalCasualTrips
	FROM
		new_total_dataset
	) AS Trips_member_and_casual

/*Average Ride Length*/

SELECT
	(
	SELECT
		AVG(CAST(ride_length AS float))
	FROM
		new_total_dataset
	) AS AvgRideLength_Overall,
	(
	SELECT
		AVG(CAST(ride_length AS float))
	FROM
		new_total_dataset
	WHERE
		member_casual =	'member'
	) AS AvgRideLength_Member,
	(
	SELECT
		AVG(CAST(ride_length AS float))
	FROM
		new_total_dataset
	WHERE
		member_casual = 'casual'
	) AS AvgRideLength_Casual

/* Max Ride Length: member and casual */

SELECT TOP 2
	member_casual,
	MAX(ride_length) AS ride_length_MAX
FROM
	new_total_dataset
GROUP BY
	member_casual
ORDER BY
	ride_length_MAX DESC


SELECT TOP 100
	member_casual,
	ride_length
FROM
	new_total_dataset
WHERE
	member_casual = 'casual'
ORDER BY
	ride_length DESC

/* Median Ride Length : member vs casual*/

WITH RankedId AS (
SELECT
	ride_id,
	ride_length,
	member_casual,
	ROW_NUMBER() OVER(PARTITION BY member_casual ORDER BY ride_length) as rn,
	COUNT(*) OVER(PARTITION BY member_casual) as cnt
FROM
	new_total_dataset
WHERE
	ride_length IS NOT NULL
),
median AS(
SELECT
	AVG(CAST(ride_length AS float)) AS median_ride_length,
	member_casual
FROM
	RankedId
WHERE
	(rn = cnt / 2 AND cnt % 2 = 1) OR  -- Odd number of rows, middle one
    (rn IN (cnt / 2, (cnt / 2) + 1) AND cnt % 2 = 0)  -- Even number of rows, average of two middle ones
GROUP BY
	member_casual
)
SELECT TOP 2
	median_ride_length,
	member_casual
FROM
	median
ORDER BY
	median_ride_length DESC

/* Rides per day: member and casual*/

WITH DayOfWeekRank AS (
	SELECT
		member_casual,
		Day_of_week,
		COUNT(*) AS day_count,
		ROW_NUMBER() OVER (PARTITION BY member_casual ORDER BY COUNT(*) DESC) AS rn
	FROM
		new_total_dataset
	GROUP BY
		member_casual,
		Day_of_week
)
SELECT TOP 2
	member_casual,
	Day_of_week AS mode_day_of_week
FROM
	DayOfWeekRank
WHERE
	rn = 1
ORDER BY
	member_casual DESC;


/* Average ride length per day of week*/

SELECT TOP 7
	day_of_week,
	AVG(CAST(ride_length AS float)) AS average_ride_length
FROM
	new_total_dataset
GROUP BY
	Day_of_week
ORDER BY
	average_ride_length DESC

/* How about median ride length per day of week ?*/

WITH RankedRides AS (
	SELECT
		ride_id,
		Day_of_week,
		ride_length,
		ROW_NUMBER() OVER (PARTITION BY Day_of_week ORDER BY ride_length) AS rn,
		COUNT(*) OVER (PARTITION BY Day_of_week) AS cnt
	FROM
		new_total_dataset
	WHERE
		ride_length IS NOT NULL
),
Median AS (
	SELECT
		Day_of_week,
		CAST(AVG(CAST(ride_length AS float)) AS DECIMAL(10,2)) AS median_ride_length
	FROM
		RankedRides
	WHERE
		rn IN (cnt / 2,(cnt /2) + 1)
	GROUP BY
		Day_of_week

)
SELECT TOP 7
	median_ride_length,
	Day_of_week
FROM
	Median
ORDER BY
	Day_of_week DESC

/*Looking at AVG ride length per day of week for casual and annual*/
SELECT TOP 14
	member_casual,
	Day_of_week,
	AVG(CAST(ride_length AS float)) AS average_ride_length
FROM
	new_total_dataset
GROUP BY
	Day_of_week,
	member_casual
ORDER BY
	average_ride_length DESC

/*Looking at median ride length per day*/
WITH RankedRides AS (
SELECT
	ride_id,
	member_casual,
	Day_of_week,
	ride_length,
	ROW_NUMBER() OVER (PARTITION BY day_of_week ORDER BY ride_length) AS rn,
	COUNT(*) OVER (PARTITION BY day_of_week) As cnt
FROM
	new_total_dataset
WHERE
	member_casual = 'casual' AND
	ride_length IS NOT NULL
),
Median as(
	SELECT
		day_of_week,
		member_casual,
		AVG(CAST(ride_length AS float)) AS median_ride_length
	FROM
		RankedRides
	WHERE
		rn IN (cnt / 2,(cnt /2) + 1)
	GROUP BY
	Day_of_week,
	member_casual
)
SELECT TOP 7
	median_ride_length,
	member_casual,
	Day_of_week
FROM
	Median
ORDER BY
	median_ride_length DESC

/*Looking at total number of trips per day_of_week*/

SELECT TOP 7
	Day_of_week,
	COUNT(DISTINCT ride_id) AS TotalTrips
FROM
	new_total_dataset
GROUP BY
	Day_of_week
ORDER BY
	TotalTrips DESC

/*Looking at total number of trips per day for casual and member*/

SELECT TOP 7
	Day_of_week,
	COUNT(ride_id) AS TotalTrips,
	SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END) AS MemberTrips,
	SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END) AS CasualTrips
FROM
	new_total_dataset
GROUP BY
	Day_of_week
ORDER BY
	TotalTrips DESC

/*Start station : member vs casual
Looking at start station counts
*/

SELECT
	start_station_name,
	SUM(CASE WHEN ride_id = ride_id AND start_station_name = start_station_name THEN 1 ELSE 0 END) AS total,
	SUM(CASE WHEN member_casual = 'member' AND start_station_name = start_station_name THEN 1 ELSE 0 END) AS member,
	SUM(CASE WHEN member_casual = 'casual' AND start_station_name = start_station_name THEN 1 ELSE 0 END) AS casual
FROM
	new_total_dataset
GROUP BY
	start_station_name
ORDER BY
	total DESC

/*Defining MIN and MAX value for lat and long*/

SELECT
	MAX(start_lat) AS start_lat_max,
	MIN(start_lat) AS start_lat_min,
	MAX(start_lng) AS start_lng_max,
	MIN(start_lng) AS start_lng_min,
    MAX(end_lat) AS end_lat_max,
    MAX(end_lat) AS end_lat_min,
    MAX(end_lng) AS end_lng_max,
    MIN(end_lng) AS end_lng_min
FROM
	new_total_dataset
