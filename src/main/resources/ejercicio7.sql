-- Usamos rangos edad calculados en ejercicio 3 en agePercentiles

-- Join de los datos de fichero tsv y sql
hadoop jar target/Practica1-0.0.1-SNAPSHOT.jar practica1.datasetb.join.DriverJoin /user/cloudera/practica1/tsv/userid /user/cloudera/practica1/sql/userid /user/cloudera/practica1/joindataB

-- Generacion de fichero de salida agrupado por cancion y rango de edad 
hadoop jar target/Practica1-0.0.1-SNAPSHOT.jar practica1.datasetb.ejercicio7.DriverEjercicio7 /user/cloudera/practica1/joindataB /user/cloudera/practica1/agePercentiles/000000_0 /user/cloudera/practica1/ejercicio7

CREATE TABLE ejercicio7 (
cancion STRING,
rango STRING,
plays INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LOCATION '/user/cloudera/practica1/ejercicio7';

CREATE TABLE ejercicio7Final
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/cloudera/practica1/ejercicio7Final' AS
SELECT cancion, rango, plays
FROM (
SELECT e7.cancion, e7.rango, e7.plays, row_number() over (Partition BY e7.cancion, e7.rango order by e7.plays desc) as row
FROM ejercicio7 e7
) rs
WHERE row <= 10;
