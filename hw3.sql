
CREATE EXTERNAL DATA SOURCE cse344blobcarrier
WITH (  TYPE = BLOB_STORAGE,
       LOCATION = 'https://flightsblobcse344.blob.core.windows.net/hm3'
);

bulk insert Carriers from 'carriers.csv'
with (ROWTERMINATOR = '0x0a',
DATA_SOURCE = 'cse344blobcarrier', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
FIRSTROW=1,TABLOCK);

bulk insert Months from 'months.csv'
with (ROWTERMINATOR = '0x0a',
DATA_SOURCE = 'cse344blobcarrier', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
FIRSTROW=1,TABLOCK);

bulk insert Weekdays from 'weekdays.csv'
with (ROWTERMINATOR = '0x0a',
DATA_SOURCE = 'cse344blobcarrier', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
FIRSTROW=1,TABLOCK);


bulk insert Flights from 'flights-small.csv'
with (ROWTERMINATOR = '0x0a',
DATA_SOURCE = 'cse344blobcarrier', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
FIRSTROW=1,TABLOCK);

-- problem 1
-- For each origin city, find the destination city (or cities) with the longest direct flight.
-- By direct flight, we mean a flight with no intermediate stops. Judge the longest flight in
-- time, not distance. (15 points)
-- Name the output columns origin_city, dest_city,
-- and time representing the the flight time between them.
-- Do not include duplicates of the same origin/destination city pair.
-- Order the result by origin_city and then dest_city (ascending, i.e. alphabetically).
-- [Output relation cardinality: 334 rows]

select distinct F.origin_city, F.dest_city, F1.time 
from FLIGHTS F, (select F.origin_city, max(F.actual_time) as time from FLIGHTS F group by F.origin_city) as F1
where F.actual_time = F1.time and F.origin_city = F1.origin_city
order by F.origin_city, F.dest_city;


-- problem 2
-- Find all origin cities that only serve flights shorter than 3 hours.
-- You can assume that flights with NULL actual_time are not 3 hours or more. (15 points)
-- Name the output column city and sort them. List each city only once in the result.
-- [Output relation cardinality: 109]

select distinct F.origin_city as city
from FLIGHTS F
WHERE 3*60 > ALL (select F1.actual_time from FLIGHTS F1 where F.origin_city = F1.origin_city)
order by city;


-- problem 3
-- For each origin city, find the percentage of departing flights shorter than 3 hours.
-- For this question, treat flights with NULL actual_time values as longer than 3 hours. (15 points)
-- Name the output columns origin_city and percentage
-- Order by percentage value, ascending. Be careful to handle cities without any flights shorter than 3 hours.
-- We will accept either 0 or NULL as the result for those cities.
-- Report percentages as percentages not decimals (e.g., report 75.25 rather than 0.7525).
-- [Output relation cardinality: 327]

select F.origin_city, (select count(*) from FLIGHTS F1 where F.origin_city = F1.origin_city and F1.actual_time< 60*3)*100.0/count(*) as percentage 
from FLIGHTS F
group by F.origin_city
order by percentage;



-- problem 4
-- List all cities that cannot be reached from Seattle though a direct flight but can be reached with
-- one stop (i.e., with any two flights that go through an intermediate city).
-- Do not include Seattle as one of these destinations (even though you could get back with two flights).
-- (15 points)
-- Name the output column city. Order the output ascending by city.
-- [Output relation cardinality: 256]

select distinct F1.dest_city as city
from FLIGHTS F, FLIGHTS F1
where F.origin_city = 'Seattle WA' and F.dest_city = F1.origin_city and F1.dest_city != 'Seattle WA' and F1.dest_city not in (select F2.dest_city from FLIGHTS F2 where F2.origin_city = 'Seattle WA')
order by city;



-- problem 5
-- List all cities that cannot be reached from Seattle through a direct flight nor with one stop
-- (i.e., with any two flights that go through an intermediate city). Warning: this query might take a while to execute.
-- We will learn about how to speed this up in lecture. (15 points)
-- Name the output column city. Order the output ascending by city.
-- (You can assume all cities to be the collection of all origin_city or all dest_city)
-- (Note: Do not worry if this query takes a while to execute. We are mostly concerned with the results)
-- [Output relation cardinality: 3 or 4, depending on what you consider to be the set of all cities]
-- [42s]

select distinct F.dest_city as city 
from FLIGHTS F
where F.dest_city not in (select distinct F2.dest_city from FLIGHTS F1, FLIGHTS F2 where F1.origin_city = 'Seattle WA' and F1.dest_city = F2.origin_city) and F.dest_city not in (select F3.dest_city from FLIGHTS F3 where F3.origin_city = 'Seattle WA')
order by city;



-- problem 6
-- List the names of carriers that operate flights from Seattle to San Francisco, CA.
-- Return each carrier's name only once. Use a nested query to answer this question. (7 points)
-- Name the output column carrier. Order the output ascending by carrier.
-- [Output relation cardinality: 4]

select C.name as carrier
from CARRIERS C
where C.cid in (select distinct F.carrier_id from FLIGHTS F where F.origin_city = 'Seattle WA' and F.dest_city = 'San Francisco CA');


-- problem 7
-- Express the same query as above, but do so without using a nested query. Again, name the output column
-- carrier and order ascending. (8 points)

select distinct C.name as carrier
from CARRIERS C, FLIGHTS F
where C.cid = F.carrier_id and F.origin_city = 'Seattle WA' and F.dest_city = 'San Francisco CA';
