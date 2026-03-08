DROP TABLE IF EXISTS stop_events;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS line_stops;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS lines;

CREATE TABLE lines (
    line_name VARCHAR(50) PRIMARY KEY,
    vehicle_type VARCHAR(10) NOT NULL
);

CREATE TABLE stops (
    stop_name VARCHAR(100) PRIMARY KEY,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6)
);

CREATE TABLE line_stops (
    line_name VARCHAR(50),
    stop_name VARCHAR(100),
    sequence INT,
    time_offset INT,
    PRIMARY KEY (line_name, sequence),
    FOREIGN KEY (line_name) REFERENCES lines(line_name),
    FOREIGN KEY (stop_name) REFERENCES stops(stop_name)
);

CREATE TABLE trips (
    trip_id VARCHAR(20) PRIMARY KEY,
    line_name VARCHAR(50),
    scheduled_departure TIMESTAMP,
    vehicle_id VARCHAR(50),
    FOREIGN KEY (line_name) REFERENCES lines(line_name)
);

CREATE TABLE stop_events (
    trip_id VARCHAR(20),
    stop_name VARCHAR(100),
    scheduled TIMESTAMP,
    actual TIMESTAMP,
    passengers_on INT,
    passengers_off INT,
    PRIMARY KEY (trip_id, stop_name, scheduled),
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    FOREIGN KEY (stop_name) REFERENCES stops(stop_name)
);