CREATE TABLE test(
    a VARCHAR(15),
    b INT
);

DROP TABLE IF EXISTS zipcode;
DROP TABLE IF EXISTS station;
DROP TABLE IF EXISTS measurement;
DROP TABLE IF EXISTS incident;
DROP TABLE IF EXISTS closest_station;

CREATE TABLE station (
    stnId CHAR(12) PRIMARY KEY,
    latitude NUMERIC(5, 3),
    longitude NUMERIC(5, 3)
);

CREATE TABLE measurement (
    stnId CHAR(12),
    datetime TIMESTAMP,
    value VARCHAR(8),
    PRIMARY KEY (stnId, datetime)
);

CREATE TABLE incident (
    city CHAR(128),
    state CHAR(2),
    zipcode CHAR(5),
    date Date
);

CREATE TABLE closest_station (
    zipcode CHAR(5) REFERENCES zipcode (zip),
    stnId CHAR(12)
);

GRANT ALL PRIVILEGES ON TABLE station, measurement, incident, closest_station TO dbms_project_user;