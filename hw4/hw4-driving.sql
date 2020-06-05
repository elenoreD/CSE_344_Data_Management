-- License plate can have letters and numbers; driverID and Social Security contain only numbers;
-- maxLiability is a real number; year, phone, capacity are integers; everything else are strings.
-- a. (10 points) Translate the diagram above by writing the SQL CREATE TABLE statements to represent this E/R diagram.
-- Include all key constraints; you should specify both primary and foreign keys.
-- Make sure that your statements are syntactically correct (you might want to check them using SQLite, Azure SQL Server, or another relational database).
-- b. (5 points) Which relation in your relational schema represents the relationship "insures" in the E/R diagram? Why is that your representation?
-- c. (5 points) Compare the representation of the relationships "drives" and "operates" in your schema, and explain why they are different.

create table InsuranceCo (name varchar(255), 
                    phone int,
                    PRIMARY KEY (name));

create table Insures (licensePlate varchar(255),
                name varchar(255), 
                maxLiability REAL,
                PRIMARY KEY (licensePlate));  

create table Vehicle (licensePlate varchar(255),
                year int, 
                PRIMARY KEY (licensePlate)); 

create table Car (licensePlate varchar(255),
                make varchar(255), 
                PRIMARY KEY (licensePlate));

create table Truck (licensePlate varchar(255),
                capacity int, 
                PRIMARY KEY (licensePlate));

create table Person (ssn int,
                name varchar(255), 
                PRIMARY KEY (ssn),
                FOREIGN KEY (licensePlate) REFERENCES Vehicle(licensePlate) ON DELETE CASCADE);     

create table Driver (ssn int,
            name varchar(255),
            driverID int, 
            PRIMARY KEY (ssn));              

create table NonProfessionalDriver (ssn int,
            name varchar(255),
            driverID int, 
            PRIMARY KEY (ssn));  

create table ProfessionalDriver (ssn int,
            name varchar(255),
            driverID int,
            medicalHistory varchar(255),
            PRIMARY KEY (ssn),
            FOREIGN KEY (licensePlate) REFERENCES Truck(licensePlate) ON DELETE CASCADE); 

create table Drives (licensePlate varchar(255),
            ssn int, 
            PRIMARY KEY (ssn, licensePlate),
            FOREIGN KEY (ssn) REFERENCES NonProfessionalDriver(ssn) ON DELETE CASCADE,
            FOREIGN KEY (licensePlate) REFERENCES Car(licensePlate) ON DELETE CASCADE);  
