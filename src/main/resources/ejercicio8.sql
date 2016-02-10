CREATE TABLE ejercicio8 (
user STRING,
year INT,
mes STRING,
grupo STRING,
tema STRING,
plays INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LOCATION '/user/cloudera/practica1/ejercicio8';

CREATE TABLE ejercicio8Final
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/cloudera/practica1/ejercicio8Final' AS
SELECT user, year, mes, grupo, tema, plays
FROM (
SELECT u2.user, u2.year, u2.mes, u2.grupo, u2.tema, u2.plays, row_number() over (Partition BY u2.user,u2.year,u2.mes order by u2.plays desc) as row
FROM ejercicio8 u2
) rs
WHERE row <= 10;

DROP TABLE ejercicio8; 