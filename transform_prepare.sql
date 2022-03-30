USE db_flight;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS tb_voo;
CREATE EXTERNAL TABLE IF NOT EXISTS tb_voo(
    dia STRING,
    dia_semana STRING,
    companhia STRING,
    num_voo STRING,
    num_cauda STRING,
    aeroporto_origem STRING,
    aeroporto_destino STRING,
    partida_agendada STRING,
    tempo_partida STRING,
    atraso_partida STRING,
    taxiamento_fora STRING,
    levantamento_voo STRING,
    tempo_programado STRING,
    tempo_decorrido STRING,
    tempo_ar STRING,
    distancia STRING,
    pouso_voo STRING,
    taxiamento_dentro STRING,
    chegada_programada STRING,
    tempo_chegada STRING,
    atraso_chegada STRING,
    desvio STRING,
    cancelado STRING,
    motivo_cancelamento STRING,
    atraso_sistema_ar STRING,
    atraso_seguranca STRING,
    atraso_companhia STRING,
    atraso_aeronave STRING,
    atraso_clima STRING
)
PARTITIONED BY (ano STRING, mes STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION '/db_flight/transform/tb_voo';

INSERT OVERWRITE TABLE tb_voo 
PARTITION(ano,mes)   
SELECT 
    day,
    day_of_week,
    airline,
    flight_number,
    tail_number,
    origin_airport,
    destination_airport,
    scheduled_departure,
    departure_time,
    departure_delay,
    taxi_out,
    wheels_off,
    scheduled_time,
    elapsed_time,
    air_time,
    distance,
    wheels_on,
    taxi_in,
    scheduled_arrival,
    arrival_time,
    arrival_delay,
    diverted,
    cancelled,
    cancellation_reason,
    air_system_delay,
    security_delay,
    airline_delay,
    late_aircraft_delay,
    weather_delay,
    year,
    month
FROM tb_flight;