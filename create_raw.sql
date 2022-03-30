DROP DATABASE IF EXISTS db_flight CASCADE;
CREATE DATABASE db_flight
LOCATION '/db_flight';
USE db_flight;

-- airports.csv
DROP TABLE IF EXISTS tb_airport;
CREATE EXTERNAL TABLE IF NOT EXISTS tb_airport(
    iata_code STRING,
    airport STRING,
    city STRING,
    state STRING,
    country STRING,
    latitude STRING,
    longitude STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    "separatorChar"="\,",
    "quoteChar"='\"'
)
STORED AS TEXTFILE
LOCATION '/db_flight/raw/tb_airport'
TBLPROPERTIES("skip.header.line.count"="1");

-- airlines.csv
DROP TABLE IF EXISTS tb_airline;
CREATE EXTERNAL TABLE IF NOT EXISTS tb_airline(
    airport_code STRING,
    airport_name STRING,
    time_label STRING,
    time_month STRING,
    time_month_name STRING,
    time_year STRING,
    statistics_of_delays_carrier STRING,
    statistics_of_delays_late_aircraft STRING,
    statistics_of_delays_national_aviation_system STRING,
    statistics_of_delays_security STRING,
    statistics_of_delays_weather STRING,
    statistics_carriers_names STRING,
    statistics_carriers_total STRING,
    statistics_flights_cancelled STRING,
    statistics_flights_delayed STRING,
    statistics_flights_diverted STRING,
    statistics_flights_on_time STRING,
    statistics_flights_total STRING,
    statistics_minutes_delayed_carrier STRING,
    statistics_minutes_delayed_late_aircraft STRING,
    statistics_minutes_delayed_national_aviation_system STRING,
    statistics_minutes_delayed_security STRING,
    statistics_minutes_delayed_total STRING,
    statistics_minutes_delayed_weather STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    "separatorChar"="\,",
    "quoteChar"='\"'
)
STORED AS TEXTFILE
LOCATION '/db_flight/raw/tb_airline'
TBLPROPERTIES("skip.header.line.count"="1");

-- flights.csv
DROP TABLE IF EXISTS tb_flight;
CREATE EXTERNAL TABLE IF NOT EXISTS tb_flight(
    year STRING,
    month STRING,
    day STRING,
    day_of_week STRING,
    airline STRING,
    flight_number STRING,
    tail_number STRING,
    origin_airport STRING,
    destination_airport STRING,
    scheduled_departure STRING,
    departure_time STRING,
    departure_delay STRING,
    taxi_out STRING,
    wheels_off STRING,
    scheduled_time STRING,
    elapsed_time STRING,
    air_time STRING,
    distance STRING,
    wheels_on STRING,
    taxi_in STRING,
    scheduled_arrival STRING,
    arrival_time STRING,
    arrival_delay STRING,
    diverted STRING,
    cancelled STRING,
    cancellation_reason STRING,
    air_system_delay STRING,
    security_delay STRING,
    airline_delay STRING,
    late_aircraft_delay STRING,
    weather_delay STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    "separatorChar"="\,",
    "quoteChar"='\"'
)
STORED AS TEXTFILE
LOCATION '/db_flight/raw/tb_flight'
TBLPROPERTIES("skip.header.line.count"="1");