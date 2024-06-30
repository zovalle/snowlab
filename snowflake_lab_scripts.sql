-- 1
/*
Ahora crearemos una tabla llamada TRIPS que se usará para cargar los archivos delimitados 
por comas. Usaremos la pestaña Worksheet en la interfaz de usuario de Snowflake para ejecutar 
el DDL para crear la tabla.
*/

create table trips 
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

-- 2
/*
Estamos trabajando con datos estructurados y delimitados por comas que ya se encuentra disponibles
en un public stage, an external S3 bucket. Antes de que podamos utilizar estos datos, primero debemos 
un stage que especifique la ubicación de nuestro S3 bucket.
*/
create or replace stage citibike_trips url = 's3://snowflake-workshop-lab/citibike-trips';

-- 3
/*
Ahora echemos un vistazo al contenido del stage citibike_trips. 
*/
list @citibike_trips;

-- 4
/*
Antes de que podamos cargar los datos en Snowflake, debemos crear un File Format que coincida con la 
estructura de los datos.
*/
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('');

-- 5
/*
Ahora podemos ejecutar el comando COPY para cargar la data en la tabla TRIPS que fue creada
anteriormente.
*/
copy into trips from @citibike_trips file_format=csv;

-- 6
/*
Con el objetivo de realizar una comparacion de desempeño al cargar datos en la tabla TRIPS
utilizando un virtual warehouse de mas capacidad, vamos a eliminar todos los datos y meta
data de la tabla TRIPS
*/
truncate table trips;

-- 7
/* 
Modificar el tamano del virtual warehouse por defecto que viene con la cuenta de evaluacion
de 30 dias, compute_wh cuyo tamano por defecto es x-small, vamos a llevarlo a large
*/
alter warehouse compute_wh set warehouse_size = 'large';

-- 8
/*
Ejecutar nuevamente el comando COPY para cargar la data en la tabla TRIPS, observar el tiempo
de duracion
*/
copy into trips from @citibike_trips file_format=csv;

alter warehouse compute_wh set warehouse_size = 'x-small';

-- 9
/*
Supongamos que el equipo de Citi Bike quiere garantizar que no haya competencia de recursos 
entre sus cargas de trabajo de carga de datos/ETL y los usuarios finales analíticos que utilizan
herramientas de BI para consultar Snowflake. Como se mencionó anteriormente, Snowflake puede 
hacer esto fácilmente asignando diferentes virtual warehouse del tamaño adecuado a diferentes 
cargas de trabajo. Dado que Citi Bike ya tiene un virtual warehouse para la carga de datos, 
creemos un nuevo virtual warehouse para los usuarios finales que ejecutan análisis.
*/
create or replace warehouse analytics_wh with warehouse_size = 'large' warehouse_type = 'standard' 
  auto_suspend = 600 auto_resume = true;

-- 10
/*
Fijemos los parametros adecuados para trabajar con el nuevo virtual warehouse
*/
use role sysadmin;
use warehouse analytics_wh;
use database citibike;
use schema public;

-- 11
/*
Ver como luce la data en la tabla TRIPS
*/
select * from trips limit 20;

-- 12
/*
Primero, veamos algunas estadísticas horarias básicas sobre el uso de Citi Bike. Ejecute el 
siguiente query. Mostrará para cada hora el número de viajes, la duración promedio del viaje 
y distancia promedio de viaje.
*/
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1;

-- 13
/*
Snowflake tiene un caché de resultados que contiene los resultados de cada consulta ejecutada 
en las últimas 24 horas. Están disponibles en todos los virtual warehouse, por lo que los 
resultados de la consulta devueltos a un usuario están disponibles para cualquier otro usuario 
del sistema que ejecute la misma consulta, siempre que los datos subyacentes no hayan cambiado. 
Estas consultas repetidas no solo regresan extremadamente rápido, sino que tampoco utilizan 
créditos de cómputo. Veamos el caché de resultados en acción ejecutando exactamente la misma 
consulta nuevamente.
*/
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1;

-- 14
/*
Ejecutemos un query para ver cuales meses son los más ocupados
*/
select monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- 15
/*
Snowflake permite clonar tablas, también conocidos como “clones de copia cero” de tablas, 
esquemas y bases de datos en segundos. Cuando se crea el clon, se toma un snapshot de los datos
presentes en el objeto de origen y se pone a disposición del objeto clonado. El objeto clonado 
se puede escribir y es independiente de la fuente del clon. Es decir, los cambios realizados en 
el objeto de origen o en el objeto clonado no forman parte del otro. Un caso de uso popular para 
la clonación de copia cero es clonar un entorno de producción para que lo utilice Desarrollo y 
Pruebas para realizar pruebas y experimentación sin impactar negativamente el entorno de producción
y eliminar la necesidad de configurar y administrar dos entornos de producción separados.

Una gran ventaja es que los datos subyacentes no se copian; solo los metadatos/punteros a la data 
subyacente cambian. De ahí el termino “zero-copy”, los requisitos de almacenamiento no se duplican 
cuando se clonan datos.
*/
create table trips_dev clone trips;

-- 16
/*
Los primeros pasos aquí son similares a los anteriores: Preparación para cargar datos, y Carga de 
datos. Volviendo a la "historia" del laboratorio, el equipo de análisis de Citi Bike quiere ver cómo
el clima afecta la cantidad de viajes.

Para hacer esto haremos:

● Cargar datos meteorológicos en formato JSON almacenados en un depósito público de Amazon S3.
● Crear una vista y consultar los datos semiestructurados utilizando la notación de puntos SQL.
● Ejecute una consulta que una los datos JSON con los datos de la tabla TRIPS.
● Vea cómo el clima afecta el recuento de viajes.

Los datos JSON consisten en información meteorológica proporcionada por https://openweathermap.org/ 
que detalla el historial de condiciones de la ciudad de Nueva York del 2016-07-05 al 2019-06-25. 
Los datos representan 57.9k filas, 61 objetos y un tamaño total comprimido de 2.5 MB.
*/

/*
Crear una base de datos llamada weather
*/
create database weather;

-- 17
/*
Definiendo el contexto de trabajo
*/
use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- 18
/*
Ahora creemos una tabla llamada json_weather_data que utilizará para cargar los datos JSON. 
Snowflake tiene un tipo de datos especial llamada VARIANT que nos permitirá almacenar el objeto 
JSON completo y eventualmente consultarlo directamente.
*/
create table json_weather_data (v variant);

-- 19
/*
Crear un stage externo donde la data no estructurada se encuentra almacenada en un AWS S3 bucket.
*/
create stage nyc_weather url = 's3://snowflake-workshop-lab/weather-nyc';

-- 20
/*
Listar el contenido del stage
*/
list @nyc_weather;

-- 21
/*
Copiar los datos desde el stage externo hacia Snowflake en la tabla json_weather_data
*/
copy into json_weather_data 
from @nyc_weather 
file_format = (type=json);

-- 22
/*
Ver el contenido de la tabla
*/
select * from json_weather_data limit 10;

-- 23
/*
Crear una vista sobre la tabla json_weather_data, observar la notación utilizada para acceder
a los atributos del objeto JSON
*/
create view json_weather_data_view as
select v:time::timestamp as observation_time,
       v:city.id::int as city_id,
       v:city.name::string as city_name,
       v:city.country::string as country,
       v:city.coord.lat::float as city_lat,
       v:city.coord.lon::float as city_lon,
       v:clouds.all::int as clouds,
       (v:main.temp::float)-273.15 as temp_avg,
       (v:main.temp_min::float)-273.15 as temp_min,
       (v:main.temp_max::float)-273.15 as temp_max,
       v:weather[0].main::string as weather,
       v:weather[0].description::string as weather_desc,
       v:weather[0].icon::string as weather_icon,
       v:wind.deg::float as wind_dir,
       v:wind.speed::float as wind_speed
  from json_weather_data
 where city_id = 5128638;

-- 24
/*
Verificar la vista creada seleccionando 20 filas
*/
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01' 
limit 20;

-- 25
/*
Utilizar JOINS para unir la data de la tabla citibike.public.trips con la vista recientemente
creada
*/
select weather as conditions,
       count(*) as num_trips
  from citibike.public.trips 
  left outer join json_weather_data_view
    on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
 where conditions is not null
 group by 1 order by 2 desc;

-- 26
/*
La característica de time travel que ofrece Snowflake permite acceder a datos históricos en 
cualquier punto dentro de un periodo de tiempo preconfigurable. El período de tiempo predeterminado 
es de 24 horas y con la edición Enterprise puede durar hasta 90 días.

Algunas aplicaciones útiles de esto incluyen:

● Restaurar objetos de datos (tablas, esquemas, y base de datos completas) que hayan sido 
  eliminadas accidentalmente o intencionalmente
● Duplicar y hacer copias de seguridad de datos de puntos clave del pasado
● Analizar el uso/manipulación de datos durante períodos de tiempo específicos

Para probar esta capacidad vamos a eliminar la tabla json_weather_data 
*/
drop table json_weather_data;

-- 27
/*
Verificamos que la tabla ya no se encuentra accessible, el objeto no existe
*/
select * from json_weather_data limit 10;

-- 28
/*
Utilizamos el comando UNDROP para restaurar la tabla previamente eliminada
*/
undrop table json_weather_data;

-- 29
/*
Ahora veamos cómo hacer rollback en una tabla a un estado anterior para corregir un error de DML 
involuntario que reemplaza todos los nombres de las estaciones en la tabla TRIPS de la base de datos 
de Citibike con la palabra "oops".
*/
use database citibike;
use schema public;

-- 30
/*
Ejecutar el siguiente update para cambiar el nombre de la estacion en todos los registros de la tabla.
*/
update trips set start_station_name = 'oops';

-- 31
/*
Ahora veamos cuantos registros hay por nombre de estación
*/
select start_station_name as station,
       count(*) as rides
  from trips
 group by 1
 order by 2 desc
 limit 20;

-- 32
/*
Normalmente, tendríamos que luchar y esperar tener un respaldo por ahí para restablecer la data a su
estado anterior exactamente antes del update, sin embargo, con Snowflake simplemente podemos ejecutar 
comandos para encontrar el query ID del último UPDATE realizado.  Lo almacenamos en una variable
que la llamaremos $QUERY_ID para luego utilizarla al restablecer la data.
*/
set query_id = 
(select query_id from 
table(information_schema.query_history_by_session (result_limit => 5)) 
where query_text like 'update%' order by start_time limit 1);

-- 33
/*
Ahora re-creamos la tabla TRIPS con la data exactamente antes del UPDATE.
*/
create or replace table trips as
(select * from trips before (statement => $query_id));
        
-- 34
/*
Ejecutamos el query de verificación para ver cuantas vueltas por nombre de estación tenemos.
*/
select start_station_name as "station",
       count(*) as "rides"
  from trips
 group by 1
 order by 2 desc
 limit 20;
