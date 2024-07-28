
/* CREATE VIEW TO STORE DATA FOR VISUALIZATION*/

--CREATE VIEW FOR TOTAL TRIPS BY ANNUAL MEMBERS

CREATE VIEW TotalTripsMember AS 
WITH TotalTripsOverall AS (
    SELECT COUNT(ride_id) AS TotalTrips_Overall
    FROM new_total_dataset
    WHERE member_casual = 'member'
)
SELECT
    bt.day_of_week,
    bt.member_casual,
    COUNT(bt.ride_id) AS TotalTrips,
    (COUNT(bt.ride_id) * 1.0 / tt.TotalTrips_Overall) * 100 AS PercentageOfTotal
FROM
    new_total_dataset AS bt
CROSS JOIN
    TotalTripsOverall AS tt
WHERE
    bt.member_casual = 'member'
GROUP BY
    bt.day_of_week,
    bt.member_casual,
    tt.TotalTrips_Overall;

--CREATE VIEW FOR TOTAL TRIPS BY CASUAL RIDERS

CREATE VIEW TotalTripsCasual AS
WITH TotalTrips_Overall AS (
	SELECT
		COUNT(ride_id) AS TotalTrips_Overall
	FROM
		new_total_dataset
	WHERE
		member_casual = 'casual'
)
SELECT
	day_of_week,
	member_casual,
	COUNT(ride_id) AS TotalTrips,
	(COUNT(ride_id)*1.0/ tt.TotalTrips_Overall)*100 AS PercentageOfTotal
FROM
	new_total_dataset
CROSS JOIN
	TotalTrips_Overall AS tt
WHERE
	member_casual = 'casual'
GROUP BY
	day_of_week,
	member_casual,
	TotalTrips_Overall

--SELECT * 
--FROM TotalTripsCasual
--ORDER BY TotalTrips DESC

--SELECT * 
--FROM TotalTripsMember
--ORDER BY TotalTrips DESC