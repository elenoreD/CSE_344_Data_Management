create EXTERNAL table USERS (username varchar(20),
                    password varbinary(20),
                    balance int) WITH (DATA_SOURCE = CSE344_EXTERNAL);

create EXTERNAL table RESERVATIONS (reservation_id int not null,
                        user_name varchar(20),
                        flight_id1 int,
                        flight_id2 int,
                        isPaid bit,
                        isCancelled bit) WITH (DATA_SOURCE = CSE344_EXTERNAL);

CREATE INDEX F_origin ON FLIGHTS(origin_city);
CREATE INDEX F_dest ON FLIGHTS(dest_city);
CREATE INDEX F_origin_dest ON FLIGHTS(origin_city, dest_city);