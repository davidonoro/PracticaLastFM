--query para sacar los 10 grupos más escuchados por usuario
CREATE TABLE usertopgroup
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/practica1/usertopgroup' AS
SELECT usersha1, artname, plays
FROM (
SELECT u2.usersha1, u2.artname, u2.plays, row_number() over (Partition BY u2.usersha1 order by u2.usersha1, u2.plays desc) as row
FROM userplaysbygroup u2
) rs
WHERE row <= 10;
