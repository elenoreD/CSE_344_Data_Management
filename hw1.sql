-- problem 1
create table Edges (Source INT, Destination INT);
insert into Edges values (10,5); 
insert into Edges values (6,25);
insert into Edges values (1,3);
insert into Edges values (4,4);

select * from Edges;
select Source from Edges;
select * from Edges where Source > Destination;
insert into Edges values ('-1','2000');


-- problem 2
create table MyRestaurants (names varchar (10), types varchar (10), distance int, lastvisitdate date, whetheryoulike int);

-- problem 3

insert into MyRestaurants values ('restaurant1', 'a', 5, '2020-05-24', 1);
insert into MyRestaurants values ('restaurant2', 'b', 5, '2020-05-24', 0);
insert into MyRestaurants values ('restaurant3', 'c', 5, '2020-05-24', null);
insert into MyRestaurants values ('restaurant4', 'd', 5, '2020-05-24', 1);
insert into MyRestaurants values ('restaurant5', 'e', 5, '2020-05-24', 1);

-- problem 4
--  .mode csv
--  .mode list
--  .mode column
--  .width 15
--  .headers on

-- problem 5
select names,distance from MyRestaurants where distance <= 20 order by names;

-- problem 6
select names from MyRestaurants where whetheryoulike = 1 and lastvisitdate < lastvisitdate('now', '-3 month');

-- problem 7
select names from MyRestaurants where distance < 10;


-- problem 7


