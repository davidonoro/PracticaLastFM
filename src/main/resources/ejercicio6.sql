-- Pdte de revisar y comparar con el job

CREATE TABLE weekgendertopsongs
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/practica1/weekgendertopsongs' AS
select year, weekYear, gender,traname,numplays from (
select agregated.*, row_number() over (PARTITION BY agregated.year,agregated.weekYear,agregated.gender order by numplays desc) as row
from (
select uj.year,uj.weekYear,uj.gender,uj.traname, count(*) as numplays
from usertplaystimestamp_userid_join uj
group by uj.year,uj.weekYear,uj.gender,uj.traname
) agregated
) agregatedordered
where agregatedordered.row <= 10;



--para hacer un decode del sexo 
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
