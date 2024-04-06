-- Queries to create tables in PostgreSQL database

-- circuit table
CREATE TABLE circuit (
    circuitid SERIAL,
    circuitref VARCHAR,
    name VARCHAR,
    location VARCHAR,
    country VARCHAR,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    alt VARCHAR,
    url VARCHAR,
    PRIMARY KEY (circuitid)
);

-- constructor table
CREATE TABLE constructor (
    constructorid SERIAL,
    constructorref VARCHAR,
    name VARCHAR,
    nationality VARCHAR,
    url VARCHAR,
    PRIMARY KEY (constructorid)
);

-- driver table
CREATE TABLE driver (
    driverid SERIAL,
    driverref VARCHAR,
    number VARCHAR,
    code VARCHAR,
    forename VARCHAR,
    surname VARCHAR,
    dob DATE,
    nationality VARCHAR,
    url VARCHAR,
    PRIMARY KEY (driverid)
);

-- races table
CREATE TABLE race (
    raceid SERIAL,
    year INTEGER,
    round INTEGER,
    circuitid INTEGER,
    name VARCHAR,
    date DATE,
    time VARCHAR,
    url VARCHAR,
    fp1_date VARCHAR,
    fp1_time VARCHAR,
    fp2_date VARCHAR,
    fp2_time VARCHAR,
    fp3_date VARCHAR,
    fp3_time VARCHAR,
    quali_date VARCHAR,
    quali_time VARCHAR,
    sprint_date VARCHAR,
    sprint_time VARCHAR,
    PRIMARY KEY (raceid),
    CONSTRAINT races_fk
    FOREIGN KEY (circuitid)
    REFERENCES circuit (circuitid)
);

-- status table
CREATE TABLE status (
    statusid SERIAL,
    status VARCHAR,
    PRIMARY KEY (statusid)
);

-- race_result table
CREATE TABLE race_result (
    resultid SERIAL,
    raceid INTEGER REFERENCES race (raceid),
    driverid INTEGER REFERENCES driver (driverid),
    constructorid INTEGER REFERENCES constructor (constructorid),
    number VARCHAR,
    grid INTEGER,
    position VARCHAR,
    positiontext VARCHAR,
    positionorder INTEGER,
    points FLOAT,
    laps INTEGER,
    time VARCHAR,
    milliseconds VARCHAR,
    fastestlap VARCHAR,
    rank VARCHAR,
    fastestlaptime VARCHAR,
    fastestlapspeed VARCHAR,
    statusid INTEGER REFERENCES status (statusid),
    PRIMARY KEY (resultid)
);

-- sprint_result table
CREATE TABLE sprint_result (
    sprintresultid SERIAL,
    raceid INTEGER REFERENCES race (raceid),
    driverid INTEGER REFERENCES driver (driverid),
    constructorid INTEGER REFERENCES constructor (constructorid),
    number VARCHAR,
    grid INTEGER,
    position VARCHAR,
    positiontext VARCHAR,
    positionorder INTEGER,
    points FLOAT,
    laps INTEGER,
    time VARCHAR,
    milliseconds VARCHAR,
    fastestlap VARCHAR,
    fastestlaptime VARCHAR,
    statusid INTEGER REFERENCES status (statusid),
    PRIMARY KEY (sprintresultid)
);
