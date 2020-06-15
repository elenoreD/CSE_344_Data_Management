create table USERS (username varchar(20),
                    password varbinary(20),
                    salt varbinary(20),
                    balance int, 
                    PRIMARY KEY (username));

create table RESERVATIONS (reservation_id int IDENTITY(1,1) not null,
                        user_name varchar(20),
                        flight_id1 int,
                        flight_id2 int,
                        isPaid bit,
                        isCancelled bit,
                        PRIMARY KEY (reservation_id),
                        FOREIGN KEY (user_name) REFERENCES USERS(username) ON DELETE CASCADE,
                        FOREIGN KEY (flight_id1) REFERENCES FLIGHTS(fid),
                        FOREIGN KEY (flight_id2) REFERENCES FLIGHTS(fid) ON DELETE CASCADE);

-- CREATE INDEX F_origin ON FLIGHTS(origin_city);
-- CREATE INDEX F_dest ON FLIGHTS(dest_city);
-- CREATE INDEX F_origin_dest ON FLIGHTS(origin_city, dest_city);