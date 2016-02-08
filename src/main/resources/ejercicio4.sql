--Se necesitaría hacer un ranking con los artistas más importantes de cada país para una de las features de la aplicación (dataset A). Sería el top 10 de los artistas de cada país ordenado por número de reproducciones.

CREATE TABLE countrytopgroup
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/practica1/countrytopgroup' AS
select aggregatedtotal.country, aggregatedtotal.artname, aggregatedtotal.total 
from (
SELECT rs.country, rs.artname, rs.total, row_number() over (Partition BY rs.country order by rs.total desc) as row
FROM (
SELECT u2.country, u2.artname, sum(u2.plays) as total
FROM userplaysbygroup_join u2
group by u2.country, u2.artname
) rs
) aggregatedtotal
WHERE aggregatedtotal.row <= 10;
