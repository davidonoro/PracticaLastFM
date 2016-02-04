DROP TABLE IF EXISTS agePercentiles;
CREATE TABLE agePercentiles
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/practica1/agePercentiles' AS
SELECT percentile(cast(age as BIGINT), 0.25) as q1,
          percentile(cast(age as BIGINT), 0.50) as q2, 
          percentile(cast(age as BIGINT), 0.75) as q3
FROM userhash;


-- Join de los datos de fichero y sql
hadoop jar target/Practica1-0.0.1-SNAPSHOT.jar practica1.dataseta.join.DriverJoin /user/cloudera/practica1/playbyUserGroup /user/cloudera/practica1/userhash /user/cloudera/practica1/joindataA

-- Generacion de fichero de salida agrupado por grupo, rango de edad y sexo 
hadoop jar target/Practica1-0.0.1-SNAPSHOT.jar practica1.dataseta.ejercicio3.DriverEjercicio3 /user/cloudera/practica1/joindataA /user/cloudera/practica1 agePercentiles/000000_0 /user/cloudera/practica1/ejercicio3

CREATE TABLE ejercicio3 (
grupo STRING,
rango STRING,
sexo STRING,
plays INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LOCATION '/user/cloudera/practica1/ejercicio3';

CREATE TABLE ejercicio3Final
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/cloudera/practica1/ejercicio3Final' AS
SELECT sexo, rango, grupo, plays
FROM (
SELECT u2.sexo, u2.rango, u2.grupo, u2.plays, row_number() over (Partition BY u2.sexo,u2.rango order by u2.plays desc) as row
FROM ejercicio3 u2
) rs
WHERE row <= 10;

/**
2.3 Obtener los 10 grupos más escuchados (dataset A) por rango de edad y sexo. Se
necesita también incluir un rango 'no informado' que incluirá la información de los
registros que no tienen edad o sexo como un grupo más.
Para los rangos de edad informados se utilizarán los “cuartiles” del valor “edad”.
Pista: los cuartiles son los cuantiles que dividen la muestra en 4 partes. Los puntos de corte son los percentiles 25, 50 y 75. Los percentiles son los valores X de los datos que dejan un porcentaje P de la muestra indicado de la población con un valor V <= X.


https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-Built-inAggregateFunctions%28UDAF%29
---------------------
DOUBLE
percentile(BIGINT col, p)
Returns the exact pth percentile of a column in the group (does not work with floating point types). p must be between 0 and 1. NOTE: A true percentile can only be computed for integer values. Use PERCENTILE_APPROX if your input is non-integral.
--------------------
Ej:
select percentile(cast(age as BIGINT), 0.25),
          percentile(cast(age as BIGINT), 0.50), 
          percentile(cast(age as BIGINT), 0.75)
from userhash;
+-------+-------+-------+--+
|  _c0  |  _c1  |  _c2  |
+-------+-------+-------+--+
| 20.0  | 23.0  | 28.0  |
+-------+-------+-------+--+
1 row selected (65.653 seconds)
*/
