USE db_flight;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS tb_aeroporto_companhia;
CREATE EXTERNAL TABLE tb_aeroporto_companhia(
    cod_aeroporto STRING,
    aeroporto STRING,
    cidade STRING,
    estado STRING,
    pais STRING,
    latitude STRING,
    longitude STRING,
    nome_aeroporto STRING,
    data_categoria STRING,
    data_mes STRING,
    nome_data_mes STRING,
    qtd_atraso_operadora STRING,
    qtd_atraso_aeronave STRING,
    qtd_atraso_sistema_nacional_aviacao STRING,
    qtd_atraso_seguranca STRING,
    qtd_atraso_clima STRING,
    desc_nome_operadora STRING,
    qtd_total_operadora STRING,
    qtd_voo_cancelado STRING,
    qtd_voo_atrasado STRING,
    qtd_voo_desviado STRING,
    qtd_voo_horario_correto STRING,
    qtd_total_voo STRING,
    soma_atraso_minutos_operadora STRING,
    soma_atraso_minutos_aeronave STRING,
    soma_atraso_minutos_sistema_nacional_aviacao STRING,
    soma_atraso_minutos_seguranca STRING,
    soma_total_atraso_minutos STRING,
    soma_atraso_minutos_clima STRING
)
PARTITIONED BY (data_ano STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION '/db_flight/transform/tb_aeroporto_companhia';


INSERT OVERWRITE TABLE tb_aeroporto_companhia 
PARTITION(data_ano)   
SELECT 
    tb_airport.iata_code,
    tb_airport.airport,
    tb_airport.city,
    tb_airport.state,
    tb_airport.country,
    tb_airport.latitude,
    tb_airport.longitude,
    tb_airline.airport_name,
    tb_airline.time_label,
    tb_airline.time_month,
    tb_airline.time_month_name,
    tb_airline.statistics_of_delays_carrier,
    tb_airline.statistics_of_delays_late_aircraft,
    tb_airline.statistics_of_delays_national_aviation_system,
    tb_airline.statistics_of_delays_security,
    tb_airline.statistics_of_delays_weather,
    tb_airline.statistics_carriers_names,
    tb_airline.statistics_carriers_total,
    tb_airline.statistics_flights_cancelled,
    tb_airline.statistics_flights_delayed,
    tb_airline.statistics_flights_diverted,
    tb_airline.statistics_flights_on_time,
    tb_airline.statistics_flights_total,
    tb_airline.statistics_minutes_delayed_carrier,
    tb_airline.statistics_minutes_delayed_late_aircraft,
    tb_airline.statistics_minutes_delayed_national_aviation_system,
    tb_airline.statistics_minutes_delayed_security,
    tb_airline.statistics_minutes_delayed_total,
    tb_airline.statistics_minutes_delayed_weather,
    tb_airline.time_year
FROM tb_airport
INNER JOIN tb_airline ON tb_airline.airport_code = tb_airport.iata_code;