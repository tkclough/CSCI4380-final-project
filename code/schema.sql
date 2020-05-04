CREATE TABLE test(
    a VARCHAR(15),
    b INT
);

DROP TABLE IF EXISTS measurement;
DROP TABLE IF EXISTS incident_address;
DROP TABLE IF EXISTS closest_station;
DROP TABLE IF EXISTS basic_incident;

CREATE TABLE measurement (
    stnId CHAR(12),
    datetime TIMESTAMP,
    value INTEGER,
    PRIMARY KEY (stnId, datetime)
);

CREATE TABLE measurement_skipped_rows (
    lineno INTEGER,
    filename VARCHAR(128)
);

CREATE TABLE incident_address (
    STATE CHAR(2),
    FDID CHAR(5),
--    INC_DATE INTEGER,
    INC_NO VARCHAR(16),
--    EXP_NO INTEGER,
--    LOC_TYPE SMALLINT,
--    NUM_MILE INTEGER,
--    STREET_PRE VARCHAR(64),
--    STREETNAME VARCHAR(128),
--    STREETSUF VARCHAR(8),
--    APT_NO INTEGER,
    CITY VARCHAR(128),
--    STATE_ID INTEGER,
    ZIP5 CHAR(5)
--    ZIP4 CHAR(4),
--    X_STREET VARCHAR(128)
);

CREATE TABLE basic_incident (
--    STATE CHAR(2),
--    FDID CHAR(5),
    INC_DATE Date,
    INC_NO VARCHAR(16),
--    EXP_NO INTEGER,
--    VERSION NUMERIC(5, 3),
--    DEPT_STA VARCHAR(16),
--    INC_TYPE INTEGER,
--    ADD_WILD CHAR(1),
--    AID CHAR(1),
--    ALARM BIGINT,
--    ARRIVAL BIGINT,
--    INC_CONT BIGINT,
--    LU_CLEAR BIGINT,
--    SHIFT CHAR(1),
--    ALARMS VARCHAR(16),
--    DISTRICT VARCHAR(32),
--    ACT_TAK1 CHAR(3),
--    ACT_TAK2 CHAR(3),
--    ACT_TAK3 CHAR(3),
--    APP_MOD CHAR(1),
--    SUP_APP INTEGER,
--    EMS_APP INTEGER,
--    OTH_APP INTEGER,
--    SUP_PER INTEGER,
--    EMS_PER INTEGER,
--    OTH_PER INTEGER,
--    RESOU_AID CHAR(1),
    PROP_LOSS INTEGER
--    CONT_LOSS INTEGER,
--    PROP_VAL INTEGER,
--    CONT_VAL INTEGER,
--    FF_DEATH SMALLINT,
--    OTH_DEATH SMALLINT,
--    FF_INJ SMALLINT,
--    OTH_INJ SMALLINT,
--    DET_ALERT CHAR(1),
--    HAZ_REL CHAR(2),
--    MIXED_USE CHAR(2),
--    PROP_USE CHAR(4),
--    CENSUS VARCHAR(8)
);

CREATE TABLE closest_station (
    zipcode CHAR(5),
    stnId CHAR(12)
);

GRANT ALL PRIVILEGES ON TABLE measurement, measurement_skipped_rows, incident_address, closest_station, basic_incident TO dbms_project_user;