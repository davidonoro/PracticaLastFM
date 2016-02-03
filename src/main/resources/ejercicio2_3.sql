
select percentile(cast(age as BIGINT), 0.25),
          percentile(cast(age as BIGINT), 0.50), 
          percentile(cast(age as BIGINT), 0.75),
          percentile(cast(age as BIGINT), 1)
from userhash;

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
*/
| 20.0  | 23.0  | 28.0  |
+-------+-------+-------+--+
1 row selected (65.653 seconds)
