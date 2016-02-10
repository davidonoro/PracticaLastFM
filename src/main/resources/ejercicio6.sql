-- este ejercicio está resuelto de dos formas distintas.

-- 1. Resolución por HIVE.
CREATE TABLE weekgendertopsongs
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/practica1/weekgendertopsongs' AS
select year, weekYear, gender,traname,numplays from (
select agregated.*, row_number() over (PARTITION BY agregated.year,agregated.weekYear,agregated.gender order by numplays desc) as row
from (
select uj.year,uj.weekYear, if(uj.gender='m','m', if (uj.gender='f','f','NI')) as gender ,uj.traname, count(*) as numplays
from usertplaystimestamp_userid_join uj
group by uj.year,uj.weekYear,uj.gender,uj.traname
) agregated
) agregatedordered
where agregatedordered.row <= 10;

-- 2. Resolución por el job
CREATE TABLE ejercicio6 (
year STRING,
semana INT,
sexo STRING,
grupo STRING,
tema STRING,
plays INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LOCATION '/user/cloudera/practica1/ejercicio6';

CREATE TABLE ejercicio6Final
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/cloudera/practica1/ejercicio6Final' AS
SELECT year, semana, sexo, grupo, tema, plays
FROM (
SELECT u2.year, u2.semana, u2.sexo, u2.grupo, u2.tema, u2.plays, row_number() over (Partition BY u2.year,u2.semana,u2.sexo order by u2.plays desc) as row
FROM ejercicio6 u2
) rs
WHERE row <= 10;

DROP TABLE ejercicio6;
