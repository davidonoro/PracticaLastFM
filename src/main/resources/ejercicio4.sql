--Se necesitaría hacer un ranking con los artistas más importantes de cada país para una de las features de la aplicación (dataset A). Sería el top 10 de los artistas de cada país ordenado por número de reproducciones.

CREATE TABLE countrytopgroup
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/practica1/countrytopgroup' AS
SELECT country, artname, plays
FROM (
SELECT u2.country, u2.artname, u2.plays, row_number() over (Partition BY u2.country order by u2.plays desc) as row
FROM userplaysbygroup_join u2
) rs
WHERE row <= 10;
