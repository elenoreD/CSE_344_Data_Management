create table FLIGHTS (fid int, 
         month_id int,        -- 1-12
         day_of_month int,    -- 1-31 
         day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
         carrier_id varchar(7), 
         flight_num int,
         origin_city varchar(34), 
         origin_state varchar(47), 
         dest_city varchar(34), 
         dest_state varchar(46), 
         departure_delay int, -- in mins
         taxi_out int,        -- in mins
         arrival_delay int,   -- in mins
         canceled int,        -- 1 means canceled
         actual_time int,     -- in mins
         distance int,        -- in miles
         capacity int, 
         price int,          -- in $       
         PRIMARY KEY (fid),
         FOREIGN KEY (carrier_id) REFERENCES CARRIERS(cid) ON DELETE CASCADE ,
         FOREIGN KEY (month_id) REFERENCES MONTHS(mid) ON DELETE CASCADE , 
         FOREIGN KEY (day_of_week_id) REFERENCES WEEKDAYS(did)
         ON DELETE CASCADE      
         );
         
create table CARRIERS (cid varchar(7), 
                    name varchar(83),
                    PRIMARY KEY (cid));



create table MONTHS (mid int, 
                    month varchar(9),
                    PRIMARY KEY (mid));


create table WEEKDAYS (did int, 
                    day_of_week varchar(9),
                    PRIMARY KEY (did));

-- .mode csv FLIGHTS
-- .import flights-small.csv FLIGHTS

-- problem 1
-- List the distinct flight numbers of all flights from Seattle to Boston by Alaska Airlines Inc. on Mondays.
-- Also notice that, in the database, the city names include the state. So Seattle appears as
-- Seattle WA. Please use the flight_num column instead of fid.
-- Name the output column flight_num.
-- [Hint: Output relation cardinality: 3 rows]
select distinct F.flight_num 
from FLIGHTS F ,CARRIERS C, WEEKDAYS W 
where F.carrier_id = C.cid and F.day_of_week_id = W.did and F.origin_city = 'Seattle WA' and F.dest_city = 'Boston MA' and C.name ='Alaska Airlines Inc.' and W.day_of_week = 'Monday';

-- problem2
-- Find all itineraries from Seattle to Boston on July 15th. Search only for itineraries that have one stop (i.e., flight 1: Seattle -> [somewhere], flight2: [somewhere] -> Boston).
-- Both flights must depart on the same day (same day here means the date of flight) and must be with the same carrier. It's fine if the landing date is different from the departing date (i.e., in the case of an overnight flight). You don't need to check whether the first flight overlaps with the second one since the departing and arriving time of the flights are not provided.
-- The total flight time (actual_time) of the entire itinerary should be fewer than 7 hours
-- (but notice that actual_time is in minutes).
-- For each itinerary, the query should return the name of the carrier, the first flight number,
-- the origin and destination of that first flight, the flight time, the second flight number,
-- the origin and destination of the second flight, the second flight time, and finally the total flight time.
-- Only count flight times here; do not include any layover time.
-- Name the output columns name as the name of the carrier, f1_flight_num, f1_origin_city, f1_dest_city, f1_actual_time, f2_flight_num, f2_origin_city, f2_dest_city, f2_actual_time, and actual_time as the total flight time. List the output columns in this order.
-- [Output relation cardinality: 1472 rows]

select count(*) from (select C.name, F1.flight_num as f1_flight_num, F1.origin_city as f1_origin_city, F1.dest_city as f1_dest_city, F1.actual_time as f1_actual_time, F2.flight_num as f2_flight_num, F2.origin_city as f2_origin_city, F2.dest_city as f2_dest_city, F2.actual_time as f2_actual_time, (F1.actual_time + F2.actual_time) as actual_time 
from CARRIERS C, FLIGHTS F1, FLIGHTS F2, MONTHS M
where C.cid = F1.carrier_id and  F1.carrier_id = F2.carrier_id and M.mid = F1.month_id and F1.month_id = F2.month_id and F1.origin_city = 'Seattle WA' and F2.dest_city = 'Boston MA' and F1.dest_city = F2.origin_city and M.month = 'July' and F1.day_of_month ='15' and F2.day_of_month =F1.day_of_month and F1.actual_time + F2.actual_time <7*60);

-- problem 3
-- Find the day of the week with the longest average arrival delay.
-- Return the name of the day and the average delay.
-- Name the output columns day_of_week and delay, in that order. (Hint: consider using LIMIT. Look up what it does!)
-- [Output relation cardinality: 1 row]

select W.day_of_week as day_of_week, avg(F.arrival_delay) as delay 
from WEEKDAYS W, FLIGHTS F
where F.day_of_week_id = W.did 
group by F.day_of_week_id
order by delay desc
limit 1;


-- problem 4
-- Find the names of all airlines that ever flew more than 1000 flights in one day
-- (i.e., a specific day/month, but not any 24-hour period).
-- Return only the names of the airlines. Do not return any duplicates
-- (i.e., airlines with the exact same name).
-- Name the output column name.
-- [Output relation cardinality: 12 rows]

select distinct C.name 
from CARRIERS C, FLIGHTS F
where count(fid)>1000 
group by C.name,
