--top artistas escuchados por día (L-D) y hora (0-23)

CREATE TABLE dayhourtopgroup
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/practica1/dayhourtopgroup' AS
select aggregatedtotal.year, aggregatedtotal.month, aggregatedtotal.dayweek, aggregatedtotal.hour, aggregatedtotal.artname, aggregatedtotal.numplays
from (
SELECT rs.year, rs.month, rs.dayweek, rs.hour, rs.artname, rs.numplays, row_number() over (Partition BY rs.year, rs.month, rs.dayweek, rs.hour order by rs.numplays desc) as row
FROM (
SELECT u2.year, u2.month, u2.dayweek, u2.hour, u2.artname, count(*) as numplays
FROM usertplaystimestamp_userid_join u2
group by u2.year, u2.month, u2.dayweek, u2.hour, u2.artname
) rs
) aggregatedtotal
WHERE aggregatedtotal.row <= 10;
