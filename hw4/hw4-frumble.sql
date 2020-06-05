-- Part 4: Mr. Frumble Relationship Discovery & Normalization (35 points)
-- Mr. Frumble (a great character for small kids who always gets into trouble) designed a simple database to record projected monthly sales in his small store. He never took a database class, so he came up with the following schema:
-- Sales(name, discount, month, price)
-- He inserted his data into a database, then realized that there was something wrong with it: it was difficult to update.
-- He hires you as a consultant to fix his data management problems.
-- He gives you this file mrFrumbleData.txt and says: "Fix it for me!".
-- Help him by normalizing his database.
-- Unfortunately you cannot sit down and talk to Mr. Frumble to find out what functional dependencies make sense in his business.
-- Instead, you will reverse engineer the functional dependencies from his data instance.


-- 1. Create a table in the database and load the data from the provided file into that table; use SQLite or any other relational DBMS if your choosing.
-- Turn in your create table statement. The data import statements are optional (you don't need to include these).
create table mrFrumbleData (name varchar(7), 
                    discount int,
                    month varchar(3),
                    price int,
                    PRIMARY KEY (name,discount,month,price));



-- 2. Find all nontrivial functional dependencies in the database.
-- This is a reverse engineering task, so expect to proceed in a trial and error fashion. Search first for the simple dependencies, say name → discount then try the more complex ones, like name, discount → month, as needed. To check each functional dependency you have to write a SQL query.
-- Your challenge is to write this SQL query for every candidate functional dependency that you check, such that:
-- a. the query's answer is always short (say: no more than ten lines - remember that 0 results can be instructive as well)
-- b. you can determine whether the FD holds or not by looking at the query's answer.
-- Try to be clever in order not to check too many dependencies, but don't miss potential relevant dependencies.
-- For example, if you have A → B and C → D, you do not need to derive AC → BD as well.
-- Please write a SQL query for each functional dependency you find along with a comment describing the functional dependency.
-- Please also include a SQL query for at least one functional dependency that does not exist in the dataset along with a comment mentioning that the functional dependency does not exist.
-- Remember, short queries are preferred.


-- 3. Decompose the table into Boyce-Codd Normal Form (BCNF), and create SQL tables for the decomposed schema. Create keys and foreign keys where appropriate.
-- For this question turn in the SQL commands for creating the tables.

create table FRUMBLE (fid int PRIMARY KEY, 
                    name varchar(7),
                    FOREIGN KEY (discount) REFERENCES DISCOUNT(did) ON DELETE CASCADE),
                    FOREIGN KEY (month) REFERENCES MONTH(mid) ON DELETE CASCADE),
                    FOREIGN KEY (price) REFERENCES PRICE(pid) ON DELETE CASCADE));

create table NAME (nid INTEGER PRIMARY KEY, 
                    name varchar(7));

create table DISCOUNT (did int NOT NULL AUTO_INCREMENT,
                    discount int,
                    PRIMARY KEY (did));

create table MONTH (mid int NOT NULL AUTO_INCREMENT, 
                    month varchar(3),
                    PRIMARY KEY (mid));

create table PRICE (pid int NOT NULL AUTO_INCREMENT, 
                    price int,
                    PRIMARY KEY (pid));

-- 4. Populate your BCNF tables from Mr. Frumble's data.
-- For this you need to write SQL queries that load the tables you created in question 3 from the table you created in question 1.
-- Here, turn in the SQL queries that load the tables, and count the size of the tables after loading them (obtained by running SELECT COUNT(*) FROM Table).

insert into NAME(name) select distinct name from mrFrumbleData;