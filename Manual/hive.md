# HIVE

Descarga de VM:
https://downloads-hortonworks.akamaized.net/sandbox-hdp-2.6.3/HDP_2.6.3_virtualbox_16_11_2017.ova

https://downloads.cloudera.com/demo_vm/virtualbox/cloudera-quickstart-vm-5.12.0-0-virtualbox.zip

Veremos las posibilidades que tenemos con __hive__

|  Propiedades | Decripción  |
|---|---|
|  hive> set hiveconf:hive.cli.print.current.db=true | indica que bbdd de datos hacemos referencia  |
|  hive> set hive.cli.print.header=true; | Para poner los nombres de columnas en la salida de la consulta |

|  Comando | Decripción  |
|---|---|
|  $ hive __-e__ "consulta de hive" | desde fuera podemos hacer consultas |
|  $ hive __-S__ -e " select * from word_counts" > /home/training/temporales_hql  | volcado del resultado a un ficherl  |
| hive> __source__ /path/to/file/hql |  ejecutar desde hive un fichero |
|  $ hive __-f__ /path/to/file/hql  |  ejecutar desde fuera de hive un fichero |
| hive> __!__ comando_linux __;__  |  Podemos ejecutar comandos desde hive linux |
| hive> __!__ dfs -ls / __;__  |  Podemos ejecutar comandos hadoop |
| hive> dfs -help;  |  Ayuda de hadoop en hive |


|  Comando | Decripción  |
|---|---|
| describe / desc  |  Muestra como es la tabla de hive a nivel de atributos |
| desc formatted | Muestra los datos de la tabla con todas las propiedades |

## Datos
### Tipos de datos
|Tipo|---|ejemplo|
|---|---|---|
| TINYINT | 1 byte signed integer. | 20 |
| SMALLINT | 2 byte signed integer. | 20 |
| INT | 4 byte signed integer. | 20 |
| BIGINT | 8 byte signed integer. | 20 |
| BOOLEAN | Boolean true or false. | TRUE |
| FLOAT  | Single precision floating point. | 3.14159 |
| DOUBLE | Double precision floating point. | 3.14159 |
| STRING | Sequence of characters. | 'Now is the time'|  
| TIMESTAMP (v0.8.0+)  | Integer, float, or string. | 1327882394 (Unix epoch seconds), 1327882394.123456789 (Unix epoch seconds plus nanoseconds), and '2012-02-03 12:34:56.123456789' (JDBCcompliant java.sql.Timestamp format) |
| BINARY (v0.8.0+)  | Array of bytes. | See discussion below |
| STRUCT |   | struct('John', 'Doe') |
| MAP |   | map('first', 'John', 'last', 'Doe') |
| ARRAY |   | array('John', 'Doe') |

### Codificación de los ficheros de texto
Estamos acostumbrados a trabajar con CSV (separados por comas) TSV
(separados por tabulaciones).
| Delimitador | Descripción |
| --- | --- |
| \n | Cada línea es un record |
| ^A (“control” A) | Separa todos los campos, código octal \001 |
| ^B | Separa cada elemento en Array o Struct, o los pares clave-valor en un Map, código octal \002 |
| ^C | Separa la clave del valor, correspondiente a un Map, código octal \003 |

Veamos un ejemplo
John Doe^A100000.0^AMary Smith^BTodd Jones^AFederal Taxes^C.2^BState
Taxes^C.05^BInsurance^C.1^A1 Michigan Ave.^BChicago^BIL^B60600  
Como podemos observar cada:  
__^A__ representa un nuevo registro  
__^B__ concatena/crea el array   
__^C__ crea el par nuevo clave - valor, el valor seguido será el valor
```json
{  
  "name": "John Doe",
  "salary": 100000.0,
  "subordinates": ["Mary Smith", "Todd Jones"],
  "deductions": {
    "Federal Taxes": .2,
    "State Taxes": .05,
    "Insurance": .1
 },
 "address": {
   "street": "1 Michigan Ave.",
   "city": "Chicago",
   "state": "IL",
   "zip": 60600
 }
}
```


Para el siguiente esquema  
```hql
CREATE TABLE employees (
 name         STRING,
 salary       FLOAT,
 subordinates ARRAY<STRING>,
 deductions   MAP<STRING, FLOAT>,
 address      STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
```

Veamos como indicar implicitamente  el ejemplo anterior, este es el caso
por defecto
```hql
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
```

Si queremos que el delimitados sea una comas
```hql
CREATE TABLE lo_que_sea (
 primer FLOAT,
 segundo FLOAT,
 tercer FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```
En caso de querer tabuladores usaremos __'\t'__

### Esquema de lectura
Hive podría dañar la estructura del esquema, hive intentará poner los
datos, si no puede pondrá null, hive intenta recuperarse lo mejor que
puede

## Definición de datos
HiveQL es el lenguaje de consultas de Hive, no sigue el estandar ANSI
SQL, pero se pareces a MySQL con diferencias, no tenemos inserciones,
eliminaciones, actualizaciones y además no admite transacciones.
Pero añade extensiones para proporcionar mejor rendimiento con hadoop,
para crear nuevas extensiones personalizadas o programa externos.

### Bases de datos en Hive
El concepto de base de datos es un espacio de nombres o un catálogo, si
no especificamos usaremos la que trae por defecto  
```hql
CREATE DATABASE personas;
```
De esta forma creamos una base de datos o catálogo.  
Hive dará un error si existe la base de datos
```hql
CREATE DATABASE IF NOT EXISTS personas;
```
El comando __IF NOT EXISTS__ es bueno para scrips

|  Comando | Decripción  |
|---|---|
| SHOW  |  Muestra las bases de datos |


hive> SHOW DATABASES;  
listará las bases de datos, si queremos crear una usaremos:

```hql
CREATE DATABASE personas;
```
Si tenemos muchas bbdd podemos usar el comando __LIKE__ como si fuera
una consulta, para hacer un filtrado  
hive> SHOW DATABASES LIKE 'h.\*';  

Por defecto se crean las bases de datos en la siguiente ruta
/user/hive/warehouse  
Podemos cambiar la ruta por defecto a la hora de la creacion de la
tabla:
```hql
CREATE DATABASE financials
  LOCATION '/my/preferred/directory';
  .....
```
Podemos añadir comentarios para la hora de la descripción
```hql
CREATE DATABASE financials
  COMMENT 'Holds all financial tables';
  .....
```
Podemos añadir pares clave valor a la base de datos
```hql
CREATE DATABASE financials
  WITH DBPROPERTIES ('creator' = 'Mark Moneybags', 'date' = '2012-01-02');
  .....
```
Esto será visto en el describe extendido
```hql
 DESCRIBE DATABASE EXTENDED nombre_database;
```

Para poder trabajar con una base de datos usamos, que es lo mismo que
cambiar de directorio;
```hql
USE database;
```

Ahora si hacemos un __show database__ mostrará las tablas asociadas,
como no tenemos un comando para saber a que base de datos estamos , hay
una properties que no la indica
```hql
set hive.cli.print.current.db=true;
```
### Borrar
Para borrar usamos
```hql
DROP DATABASE IF EXISTS financials;
```

```hql
DROP DATABASE IF EXISTS financials CASCADE;
```
Por defecto no se borra las tablas de una base de datos, para así
intentar evitar eliminar todo, si borrarmos el directorio se borrará.
### Modificación
Solo se puede modificar las propiedades clave-valor asociadas a una base
de datos, nunca nombre, directorio.
```hql
ALTER DATABASE financials SET DBPROPERTIES ('edited-by' = 'Joe Dba');
```

### Creación tablas

Hive tiene sentencias muy parecidas pero tiene particuliaridades como
hemos visto anteriormente.   
```hql
CREATE TABLE IF NOT EXISTS mydb.employees (

)
COMMENT 'Description of the table'
TBLPROPERTIES ('creator'='me', 'created_at'='2012-01-02 10:00:00', ...)
LOCATION '/user/hive/warehouse/mydb.db/employees';
```
Si al crear la tabla, existe y tiene distinto squema, Hive ignorará las
discrepancias.  
Hive automáticamente crea dos propuedades a la tabla:  
last_modified_time  
last_modified_by  

Podemos crear una tabla con el mismo esquema que otra tabla, esquema
pero no los datos.  
```hql
CREATE TABLE IF NOT EXISTS mydb.employees2
LIKE mydb.employees
```
Estas tablas son internas que el ciclo de vida lo maneja hive, pero esto
puede ser malo/peligroso porque puede acceder otros programas, para ello
usaremos las tablas externas.
### Tablas externas  
Cuando indicamos que es una tabla externa Hive no gestiona los datos,
por lo que el borrado no se efectuará, solo eliminará los metadatos
asociados a la tabla.  
```hql
CREATE EXTERNAL TABLE IF NOT EXISTS tabla (
```
Podemos saber si es una tabla externa haciendo un __DESCRIBE EXTENDED__

### Particionado , Tablas maestras
El particionado puede servir para ayudar a organizar los datos de una
manera lógica, jerárquicamente.  
Vamos a suponer que queremos hacer la división por país y estado para el
ejemplo que estamos usando de employees  
```hql
CREATE TABLE employees (
 name STRING,
 salary FLOAT,
 subordinates ARRAY<STRING>,
 deductions MAP<STRING, FLOAT>,
 address STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
PARTITIONED BY (country STRING, state STRING);
```
Creando particiones cambiaremos la estructura de almacenamiento de hive
por defecto   hdfs://master_server/user/hive/warehouse/mydb.db/employees
...  
.../employees/country=CA/state=AB  
.../employees/country=CA/state=BC  
...  
.../employees/country=US/state=AL  
.../employees/country=US/state=AK  

Una vez creadas estas particiones se comportan como columnas, los
usuarios no necesitamos saber si es una partición o no, a excepción de
optimizar el rendimiento de las consultas.   

Quiza la razón más importante es por consultas más rápidas, ya que
solamente tiene que escanear el contenido de dicho directorio, por ello
también es importante elegir un buen particionamento.

Cuando añadimos clausulas __WHERE__ que filtran valores de particionado,
lo solemos llamar __filtros de partición__

Si queremos hacer una consulta teniendo particiones que escanee todos
las particiones, puede desencadenar mucho trabajo para el MapReduce, una
medida es poner hive en modo estricto que prohibe la consulta sin where
| Comando | Decripción |  
|---|---|
| set hive.mapred.mode=strict; | Ponemos en modo estricto hive |  

Podemos ver las particiones para una tabla, mostrando todas las
carpetas:
```hql
SHOW PARTITIONS employees;
```
Si tenemos muchas particiones, podemos especificar algún nivel para
reducir las salidas:
```hql
SHOW PARTITIONS employees PARTITION(country='US');
```
### Tablas externas particionadas
Este puede ser el caso más común, usar particioenes en tablas externas

### Borrado de tablas
Para borrar usaremos __DROP__, ojo con si es tabla externa (borra
metadatos) o no (borra directorios).
```hql
DROP TABLE IF EXISTS employees;
```
### Renombrar Tablas
```hql
ALTER TABLE log_messages RENAME TO logmsgs;
```

### Añadir, modificar, borrar particioenes
```hql
ALTER TABLE log_messages DROP IF EXISTS PARTITION(year = 2011, month = 12, day = 2);
```
### Cambiar datos de columnas
Veamos como cambiar el nombre y el tipo de una columna, con comentario y
después de otra columna
```hql
ALTER TABLE log_messages
  CHANGE COLUMN hms hours_minutes_seconds INT
    COMMENT 'The hours, minutes, and seconds part of the timestamp'
    AFTER severity;
```

### Añadir columnas
Si queremos añadir columnas, estas siempre se añadiran al final de la
tabla
```hql
ALTER TABLE log_messages ADD COLUMNS (
  app_name STRING COMMENT 'Application name',
  session_id LONG COMMENT 'The current session id'
);
```
En caso que no sea el orden, siempre podremos mover la columna __ALTER
COLUMN table CHANGE COLUMN__

## Ejemplo
En este ejemplo vamos a crear el proceso de lectura carga, y carga en
otra tablas segun el where datos
```
titanic,leonardo dicaprio:kate winslet,drama,7
avatar,zoe saldana:Sam worthington,science fiction,8
logan,hugh jackman:patrick stewar,science fiction,8
forest gump,tom hanks:robin wright,drama,7
```
Script de la tabla
```hql
CREATE TABLE IF NOT EXISTS peliculas(
        nombre string comment 'nombre de las peliculas',
        actor   array<string> comment 'actores que han participado',
        genero  string  comment 'tipo de pelicula',
        punt    int     comment 'puntuacion del año de la pelicula'
)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        COLLECTION ITEMS TERMINATED BY ':'
        LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
;
```
lanzamos el comando
```
hive (default)> source /home/david.lopez/pruebas/create_peliculas.hql;
```
cargamos los datos
```
LOAD DATA LOCAL INPATH '/home/david.lopez/pruebas/peliculas' INTO TABLE peliculas;
```

creamos las tablas donde guardaremos
```hql
use david_lopez;

CREATE TABLE IF NOT EXISTS drama(
        name string,
        actors array<string>,
        genero string,
        ratio int
)
stored as textfile;

CREATE TABLE IF NOT EXISTS fiction(
         name string,
         actors array<string>,
         genero string,
         ratio int
)
stored as textfile;
```
creamos la consulta que rellenará las tablas según el tipo de genero de
pelicula

```hql
FROM peliculas
  INSERT INTO TABLE drama SELECT * WHERE genero='drama'
  INSERT INTO TABLE fiction SELECT * WHERE genero='science fiction';
```
## Skip
#### Header
Caundo por ejemplo tenemos un fichero en la cabecera o datos en el
fichero
```
system=linux.14.0.1
version=2.0
sub-version=3.4
pars,900
jack,700
fransis,1000
.........
```
Queremos que se salte las 3 primeras lineas tenemos una propiedad, que
lo hace como veremos a continuación:
```hql
CREATE TABLE tabla_ejemplo(
  col1 string,
  col2 int
)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="3");
```
Con esto al hacer el insert inpath por ejemplo se saltará los 3 primeros
datos

#### Footer
```
pars,900
jack,700
fransis,1000
.........
system=linux.14.0.1
version=2.0
sub-version=3.4
```

```hql
CREATE TABLE tabla_ejemplo(
  col1 string,
  col2 int
)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
TBLPROPERTIES("skip.footer.line.count"="3");
```


## Manipulación de Datos

### Cargando datos en tablas internas

### Insertar datos en tablas mediante consultas
Para poder insertar en tablas usaremos una consulta como la
```hql
INSERT OVERWRITE TABLE employees
PARTITION (country = 'US', state = 'OR')
SELECT * FROM staged_employees se
WHERE se.cnty = 'US' AND se.st = 'OR';
```
Podemos usar __OVERWRITE__ (sobreescribe) o __INTO__ (agrega los datos).
  Podemos crear sentencias para generar más tablas con o sin
particiones, sobreescribiendo o no, como vemos en en ejemplo siguiente
que vamos insertando.
```hql
FROM staged_employees se
INSERT OVERWRITE TABLE employees
 PARTITION (country = 'US', state = 'OR')
 SELECT * WHERE se.cnty = 'US' AND se.st = 'OR'
INSERT OVERWRITE TABLE employees
 PARTITION (country = 'US', state = 'CA')
 SELECT * WHERE se.cnty = 'US' AND se.st = 'CA'
INSERT OVERWRITE TABLE employees
 PARTITION (country = 'US', state = 'IL')
 SELECT * WHERE se.cnty = 'US' AND se.st = 'IL';
```
Estas sentencias no son if else, ya que puede escribir en una o en
varias, segun la clausula __WHERE__.
### Inserción dinámica de particiones
Veamos un ejemplo en el que se crearán las particiones que vayamos
necesitando
```hql
INSERT OVERWRITE TABLE employees
PARTITION (country, state)
SELECT ..., se.cnty, se.st
FROM staged_employees se;
```
Podemos ver que la partición va por posición de la clausula __SELECT__,
y no por nombre, además podemos mezclar particiones dínamicas con
estáticas como podemos ver a continuación.
```hql
INSERT OVERWRITE TABLE employees
PARTITION (country = 'US', state)
SELECT ..., se.cnty, se.st
FROM staged_employees se
WHERE se.cnty = 'US';
```
### Crear tablas y cargar datos en una consulta
Podemos crear tablas y cargar el contenido en ella
```hql
CREATE TABLE ca_employees
AS SELECT name, salary, address
FROM employees
WHERE se.state = 'CA';
```
El esquema es tomado de la clausula __SELECT__.  
Un uso común es extraer los datos para un manejo más sencillo.  
Esta función no se puede usar para tabas externas.

### Exportar datos
Como los datos estan ya con el formato, solo sería hacer un copiado
```
hadoop fs -cp source_path target_path
```
Otro forma puede ser:
```hql
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/ca_employees'
SELECT name, salary, address
FROM employees
WHERE se.state = 'CA';
```
### Ejemplo
Veamos un ejemplo que he realizado para poder toparme con las
dificultades y problemas
```
david;lopez,gonzalez;0.1;3;
paco;garcia,perez;0.4;8;
cesar;lopez,gonzalez;2.2;5;
andres;duarte,moreno;1.1;9;
alejandro;duarte,moreno;0.2;6;
jesus;martinez,martinez;0.3;4;
antonio;davia,martinez;0.2;a;
lucio;jesus,arenas;0.4;4;
```
Un problema que he tenido, que al copiar los datos se me cambiaron las
comas y luego no detectaba el array

```hql
USE amigos;
DROP TABLE datos2;
CREATE EXTERNAL TABLE amigos.datos2(
        nombre STRING COMMENT 'Nombre de los amigos',
        apellidos ARRAY<STRING> COMMENT 'Lista de apellidos',
        dato1 FLOAT COMMENT 'Porcentaje',
        ratio INT COMMENT 'Ratio '
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\;'
  COLLECTION ITEMS TERMINATED BY ','
  LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/home/training/datos_nombres2.csv'
  OVERWRITE INTO TABLE datos2;
```
Siendo la salida de un select simple:
```
hive (amigos)> select * from datos2;             
OK                                               
nombre  apellidos       dato1   ratio            
david   ["lopez","gonzalez"]    0.1     3        
paco    ["garcia","perez"]      0.4     8        
cesar   ["lopez","gonzalez"]    2.2     5        
andres  ["duarte","moreno"]     1.1     9        
alejandro       ["duarte","moreno"]     0.2     6
jesus   ["martinez","martinez"] 0.3     4        
antonio ["davia","martinez"]    0.2     NULL     
lucio   ["jesus","arenas"]      0.4     4        
Time taken: 0.07 seconds                         
```
Como podemos ver no esta bien formateada la salida, lo intentaremos
solucionar más adelante.

Veamos como podemos acceder a los elementos del array, como en casi
todos los lenguajes empezamos e 0 los arrays (R empieza en 1)
```
hive (amigos)> select apellidos[0] as apellido1 from datos2;
.........
.........
Total MapReduce CPU Time Spent: 840 msec
OK                                       
apellido1                                
lopez                                    
garcia                                   
lopez                                    
duarte                                   
duarte                                   
martinez                                 
davia                                    
jesus                                    
Time taken: 8.754 seconds                
```

Vamos a añadir una columna a la tabla, que representará las ciudades
```hql
ALTER TABLE datos2 ADD COLUMNS (ciudades STRING);
LOAD DATA LOCAL INPATH '/home/training/datos_nombres_nuevos.csv' INTO TABLE datos2;
```
Estos son los datos que hemos puesto en el fichero.
```
agustin;lopez,zamora;2;2;hellin
```
Realizaremos una consulta simple, para visualizar la salida  
__OJO hay que revisar la salida, ya que parece que no ha puesto NULL__
el problema era de los datos que al final había un " " después del ";"
```
hive (amigos)> select * from datos2;
OK
nombre  apellidos       dato1   ratio   ciudades
david   ["lopez","gonzalez"]    0.1     3
paco    ["garcia","perez"]      0.4     8
cesar   ["lopez","gonzalez"]    2.2     5
andres  ["duarte","moreno"]     1.1     9
alejandro       ["duarte","moreno"]     0.2     6
jesus   ["martinez","martinez"] 0.3     4
antonio ["davia","martinez"]    0.2     NULL
lucio   ["jesus","arenas"]      0.4     4
agustin ["lopez","zamora"]      2.0     2       hellin
```
propuedads que pueden ser utiles; #TODO
TBLPROPERTIES ('skip.header.line.count'='1','serialization.null.format'
= '');

Después de solucionar el problema de  datos, tenemos ya las salidas que
queríamos.
```
hive (amigos)> select * from datos_interna;
OK
nombre  apellidos       dato1   ratio
david   ["lopez","gonzalez"]    0.1     3
paco    ["garcia","perez"]      0.4     8
cesar   ["lopez","gonzalez"]    2.2     5
andres  ["duarte","moreno"]     1.1     9
alejandro       ["duarte","moreno"]     0.2     6
jesus   ["martinez","martinez"] 0.3     4
antonio ["davia","martinez"]    0.2     NULL
lucio   ["jesus","arenas"]      0.4     4
Time taken: 0.083 seconds
hive (amigos)> ALTER TABLE datos_interna ADD COLUMNS (ciudades STRING);
OK
Time taken: 0.1 seconds


hive (amigos)> select * from datos_interna;
OK
nombre  apellidos       dato1   ratio   ciudades
david   ["lopez","gonzalez"]    0.1     3       NULL
paco    ["garcia","perez"]      0.4     8       NULL
cesar   ["lopez","gonzalez"]    2.2     5       NULL
andres  ["duarte","moreno"]     1.1     9       NULL
alejandro       ["duarte","moreno"]     0.2     6       NULL
jesus   ["martinez","martinez"] 0.3     4       NULL
antonio ["davia","martinez"]    0.2     NULL    NULL
lucio   ["jesus","arenas"]      0.4     4       NULL
Time taken: 0.168 seconds

hive (amigos)> select * from datos_interna;
OK
nombre  apellidos       dato1   ratio   ciudades
david   ["lopez","gonzalez"]    0.1     3       NULL
paco    ["garcia","perez"]      0.4     8       NULL
cesar   ["lopez","gonzalez"]    2.2     5       NULL
andres  ["duarte","moreno"]     1.1     9       NULL
alejandro       ["duarte","moreno"]     0.2     6       NULL
jesus   ["martinez","martinez"] 0.3     4       NULL
antonio ["davia","martinez"]    0.2     NULL    NULL
lucio   ["jesus","arenas"]      0.4     4       NULL
agustin ["lopez","zamora"]      2.0     2       hellin
Time taken: 0.198 seconds
```

Ahora como la salida parece que no queda bien, probemos a cambiar el
orden. A continuacón vemos la sintaxis de como sería.
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ChangeColumnName/Type/Position/Comment
```hql
ALTER TABLE table_name [PARTITION partition_spec]
  CHANGE [COLUMN] col_old_name col_new_name column_type
  [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT];
```

Esto lo que hace es mover el nombre de las columnas y el tipo, __OJO no
mueve los datos__ y esto podría descuadrar  como nos pasa en el
siguiente transformación:
```hql
ALTER TABLE datos2
  CHANGE COLUMN apellidos apellidos ARRAY<STRING>
  AFTER ciudades;
```
Datos mal
```
hive (amigos)> select * from datos2;
OK
nombre  dato1   ratio   ciudades        apellidos
david   NULL    NULL    3       [" "]
paco    NULL    NULL    8       [" "]
cesar   NULL    NULL    5       [" "]
andres  NULL    NULL    9       [" "]
alejandro       NULL    NULL    6       [" "]
jesus   NULL    NULL    4       [" "]
antonio NULL    NULL    a       [" "]
lucio   NULL    NULL    4       []
agustin NULL    2       2       ["hellin"]
```
Volvemos a dejar como estaba y ahora vamos a poner el ratio como float
```hql
ALTER TABLE datos
  CHANGE COLUMN ratio ratioFloat FLOAT
  AFTER dato1;
```
Como vemos ya tenemos la columna con float y con el nuevo nombre
```
hive (amigos)> select * from datos;                    
OK                                                      
nombre  apellidos       dato1   ratiofloat      ciudades        
david   ["lopez","gonzalez"]    0.1     3.0     NULL    
paco    ["garcia","perez"]      0.4     8.0     NULL    
cesar   ["lopez","gonzalez"]    2.2     5.0     NULL    
andres  ["duarte","moreno"]     1.1     9.0     NULL    
alejandro       ["duarte","moreno"]     0.2     6.0     NULL    
jesus   ["martinez","martinez"] 0.3     4.0     NULL    
antonio ["davia","martinez"]    0.2     NULL    NULL    
lucio   ["jesus","arenas"]      0.4     4.0     NULL    
agustin ["lopez","zamora"]      2.0     2.0     hellin  
```

Crearemos otra columna, pero ahora calculada usando if
```hql
ALTER TABLE datos ADD COLUMNS (par STRING);

INSERT OVERWRITE TABLE datos
SELECT nombre,apellidos,dato1,ratiofloat,ciudades,
  if(ratiofloat % 2 = 0 , 'par', 'inpar') as par
  FROM datos ;
```
```
hive (amigos)> select * from datos;
OK
nombre  apellidos       dato1   ratiofloat      ciudades        par
david   ["lopez","gonzalez"]    0.1     3.0     NULL    inpar
paco    ["garcia","perez"]      0.4     8.0     NULL    par
cesar   ["lopez","gonzalez"]    2.2     5.0     NULL    inpar
andres  ["duarte","moreno"]     1.1     9.0     NULL    inpar
alejandro       ["duarte","moreno"]     0.2     6.0     NULL    par
jesus   ["martinez","martinez"] 0.3     4.0     NULL    par
antonio ["davia","martinez"]    0.2     NULL    NULL    inpar
lucio   ["jesus","arenas"]      0.4     4.0     NULL    par
agustin ["lopez","zamora"]      2.0     2.0     hellin  par
```
Ahora veamos usando __WHEN__
```hql
ALTER TABLE datos ADD COLUMNS (par_when STRING);

INSERT OVERWRITE TABLE datos SELECT nombre,apellidos,dato1,ratiofloat,ciudades,par,
  CASE WHEN ratiofloat % 2 = 0 THEN 'par2'
  ELSE 'impar2'
  END AS par_when FROM datos ;
```
con la siguiente salida:
```
hive (amigos)> select * from datos;
OK
nombre  apellidos       dato1   ratiofloat      ciudades        par     par_when
david   ["lopez","gonzalez"]    0.1     3.0     NULL    inpar   impar2
paco    ["garcia","perez"]      0.4     8.0     NULL    par     par2
cesar   ["lopez","gonzalez"]    2.2     5.0     NULL    inpar   impar2
andres  ["duarte","moreno"]     1.1     9.0     NULL    inpar   impar2
alejandro["duarte","moreno"]    0.2     6.0     NULL    par     par2
jesus   ["martinez","martinez"] 0.3     4.0     NULL    par     par2
antonio ["davia","martinez"]    0.2     NULL    NULL    inpar   impar2
lucio   ["jesus","arenas"]      0.4     4.0     NULL    par     par2
agustin ["lopez","zamora"]      2.0     2.0     hellin  par     par2
```

## Consultas
### SELECT
SELECT … FROM Clauses  
SELECT es el operador de proyección de SQL, la clausula FROM indica la
tabla, vista o consulta anidada.
```hql
SELECT name, salary FROM employees;
SELECT e.name, e.salary FROM employees e;
```
### SELECT con expresiones regulares
Podemos hacer consultas donde un campo se determine por una expresión
regular
```hql
SELECT symbol, `price.*` FROM stocks;
```

### Transformar datos de columnas
No solo puedes selecionar columnas, sino que puedes
transformar/manipular los valores
### Funciones
Podemos usar funciones matemáticas

### Funciones de Agregación
Una función de agregación devuelve un solo valor del cálculo de muchas
filas. Dos de las más conocidas son __count__ y __avg__
```hql
SELECT count(*), avg(salary) FROM employees;
```
ver https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
http://www.sqlteam.com/article/how-to-use-group-by-with-distinct-aggregates-and-derived-tables
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions

### Funciones generadoras de tablas

### Limit
Es para limitar la salida

### Alias
Con esto podemos dar nombre tanto a las columnas como cuando estemos en
joins hacer referencia a tablas

#### Consultas anidadas

### Sentencias CASE … WHEN … THEN
Son como las sentencias __if__
```hql
SELECT name, salary,
  CASE
    WHEN salary < 50000.0 THEN 'low'
    WHEN salary >= 50000.0 AND salary < 70000.0 THEN 'middle'
    WHEN salary >= 70000.0 AND salary < 100000.0 THEN 'high'
    ELSE 'very high'
  END AS bracket
  FROM employees;
```
Como podemos ver en la consulta, hemos creado una categoría para el
valor del salario

### Evitar el MapReduce
Como hemos podido observar hive, ejecuta algunas consultas en map
reduce, y otras no, las que no las ejecuta en modo local para algunas de
las consultas.
|  Propiedades | Decripción  |
|---|---|
|  hive> set hive.exec.mode.local.auto=true; | intentar ejecutar consultas en modo local  |

Esto lo pondremos en nuestro fichero de configuración de hive
$HOME/.hiverc  

### Cláusula WHERE
Esto actua como filtro de las consultas, usan expresiones de predicado,
podemos usar __AND OR__ para ampliar o reducir la salida, podemos hacer
filtros que una función supere un valor por ejemplo, en las clausulas
where no podemos usar alias, pero podemos usar un select anidado, como
podemos ver a continuación:
```hql
SELECT e.* FROM
  ( SELECT name, salary, deductions["Federal Taxes"] as ded,
    salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
    FROM employees) e
 WHERE round(e.salary_minus_fed_taxes) > 70000;
```
Con esto ya tenemos la función sin duplicar

_____115

## Funciones
Estas son funciones de agregación definidas por el usuario que operan
por filas o por grupos y dan como resultado una fila o una fila para
cada grupo como, por ejemplo, las funciones MAX y COUNT integradas. Un
UDAF debe ser una subclase de org. apache.hadoop.hive.ql.exec.UDAF que
contiene una o más clases estáticas anidadas que implementan
org.apache.hadoop.hive.ql.exec.UDAFEvaluator. Asegúrese de que la clase
interna que implementa UDAFEvaluator se defina como pública. De lo
contrario, Hive no podrá usar la reflexión y determinar la
implementación de UDAFEvaluator.

También deberíamos implementar las cinco funciones requeridas, init,
iterate, terminaPartial, merge y terminate. Tanto UDF como UDAF también
pueden implementarse extendiendo desde las clases GenericUDF y
GenericUDAFEvaluator para evitar el uso de la reflexión de Java para un
mejor rendimiento. Y, estas funciones genéricas en realidad se extienden
internamente por las implementaciones UDF integradas de Hive. Las
funciones genéricas admiten tipos de datos complejos, como MAP, ARRAY y
STRUCT, como argumentos, pero las clases UDF y UDAF no.

### UDF
Las funciones definidas por el usuario __UDF__ es una poderosa
herramienta de que disponemos para ampliar la funcionalidad de HiveQl.  
Las UDF se ejecutan en los mismos procesos que las tareas de las
consultas de Hive, para saber las funciones así como lo que hace
```hql
 SHOW FUNCTIONS;
 DESCRIBE FUNCTION concat;
 DESCRIBE FUNCTION EXTENDED concat;
 ```
Podemos saber los jar que hemos agregado de la siguient manera:
```
hive (amigos)> list jars;
```
Para crear nuestras funciones debemos añadir las dependencias de hadoop-client y hive, para ello debemos saber que versión usa para que sea la misma para añadirla al pom de maven

Los pasos para agregar una UDF son:
1) Crear un programa en Java
2) Guardar / crear jar
3) Añadir el jar en Hive  
hive> add jar /ruta/al/jar;
4) Create función del jar añadido  
create temporary function nameFunc as 'paquete.nombre';
5) Usar la función en las consultas Hive.

```
$ hadoop version
Hadoop 2.0.0-cdh4.2.1
```
Podemos hacer un
```
mvn archetype:generate -DgroupId=es.ejemplo -DartifactId=ExampleUDF
-DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
 ```
para que se genere el proyecto maven y añadir al podemos
```xml
......
<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-client</artifactId>
			<version>2.0.0-cdh4.2.1</version>
			<scope>provided</scope>
		</dependency>

		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-exec</artifactId>
			<version>1.2.1</version>
			<scope>provided</scope>
		</dependency>
</dependency>
.....

<build>
		<plugins>
			<!-- build for Java 1.8. This is required by HDInsight 3.5 -->
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-compiler-plugin</artifactId>
				<version>3.3</version>
				<configuration>
					<source>1.6</source>
					<target>1.6</target>
				</configuration>
			</plugin>
			<!-- build an uber jar -->
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-shade-plugin</artifactId>
				<version>2.3</version>
				<configuration>
					<!-- Keep us from getting a can't overwrite file error -->
					<transformers>
						<transformer
							implementation="org.apache.maven.plugins.shade.resource.ApacheLicenseResourceTransformer">
						</transformer>
						<transformer
							implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer">
						</transformer>
					</transformers>
					<!-- Keep us from getting a bad signature error -->
					<filters>
						<filter>
							<artifact>*:*</artifact>
							<excludes>
                <exclude>META-INF/*.SF</exclude>
								<exclude>META-INF/*.DSA</exclude>
								<exclude>META-INF/* .RSA</exclude>
							</excludes>
						</filter>
					</filters>
				</configuration>
				<executions>
					<execution>
						<phase>package</phase>
						<goals>
							<goal>shade</goal>
						</goals>
					</execution>
				</executions>
			</plugin>
		</plugins>
	</build>
```
Estamos usando la versión 1.6 de java, por temas de hive y hadoop.  
```
mvn clean package
cd target
scp -p 2222 ExampleUDF-1.0-SNAPSHOT.jar training@ip:/home/training/
```
cuando lo tengamos en el equipo, lo añadimos a las funciones de hive

```java
// Description of the UDF
@Description(
    name="ExampleUDF",
    value="returns a upper case version of the input string.",
    extended="select ExampleUDF(deviceplatform) from hivesampletable
limit 10;" )
public class ExampleUDF extends UDF {
    // Parametro de entrada
    public String evaluate(String input) {
        // If the value is null, return a null
        if(input == null)
            return null;
        // upper the input string and return it
        return input.toUpperCase();
    }
}
```


```hql
hive (amigos)> add jar /home/training/ExampleUDF-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION mayusculas as 'es.ejemplo.ExampleUDF';

hive (amigos)> select mayuscula(nombre) from datos;
```
```
hive (amigos)> select mayusculas(nombre) from datos;
Automatically selecting local only mode for query
Total MapReduce jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
WARNING: org.apache.hadoop.metrics.jvm.EventCounter is deprecated.
Please use org.apache.hadoop.log.metrics.EventCounter in all the
log4j.properties files. Execution log at:
/tmp/training/training_20171227154242_a08feb67-2402-4b96-8872-1a3a61e2ea04.log Job running in-process (local Hadoop)
2017-12-27 15:42:56,868 null map = 100%,  reduce = 0%
Ended Job = job_local1931977071_0001
Execution completed successfully
Mapred Local Task Succeeded . Convert the Join into MapJoin
OK
_c0
DAVID
PACO
CESAR
ANDRES
ALEJANDRO
JESUS
ANTONIO
LUCIO
AGUSTIN
Time taken: 4.434 seconds
```
Siempre tenemos que implementar alguna de las funciones con la siguiente
signatura:
```
public int evaluate();
public int evaluate(int a);
public double evaluate(int a, double b);
public String evaluate(String a, int b, Text c);
public Text evaluate(String a);
public String evaluate(List<Integer> a);(En hive una lista es
representada como Array)
```
Evaluate nunca debe de estar vacío, pero puedes devolver null si lo
necesitas.   Los tipos de retorno solamente pueden ser datos primitivos
de Java con su correspondiente clase __writable__.   


https://dzone.com/articles/hive-user-defined-functions
### UDFs Scala
https://community.hortonworks.com/articles/42695/how-to-create-a-hive-udf-in-scala.html
```
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

libraryDependencies ++= Seq("org.apache.hive" % "hive-exec" % "1.2.1")

```
Si tenemos el problema que no encuentra una libreria podemos hacer esto
```
 libraryDependencies ++= Seq("org.apache.hive" % "hive-exec" % "1.2.1").map(_.exclude("org.pentaho", "pentaho-aggdesigner-algorithm"))
```

```scala
import org.apache.hadoop.hive.ql.exec.UDF

class ScalaUDF extends UDF{
  def evaluate(str: String): Int = {
    str.length()
  }
}
```




### GenericUDF
Estas funciones que podemos añadir son más complejas pero ofrece un
mejor rendimiento.   Si heredamos de ellas, necesitamos sobreescribir 3
métodos
```java
public class ExampleGenericUDF extends GenericUDF{

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
                throws UDFArgumentException {  
    // TODO Auto-generated method stub
		return null;
	}
}
```
__initialize(ObjectInspector[] arg0)__   
Este método solo se llama una vez por JVM al comienzo para iniciar el
UDF. Se usa para comprobar y validar el número y tipo de parámetros que
toma una UDF y el tipo de argumento que devuelve. También devuelve un
ObjectInspector  correspondiente al tipo de devolución de la UDF.

__evaluate(DeferredObject[] arg0)__  
Este método se llama una vez para cada fila de datos que se procesa.
Aquí se escribe la lógica real para la transformación / procesamiento de
cada fila. Devolverá un objeto que contiene el resultado de la lógica de
procesamiento.

__getDisplayString(String[] arg0)__  
Un método simple para devolver la cadena de visualización para el UDF
cuando se usa Explain.

Además podemos hacer uso de anotaciones,
__@UDFType (deterministic = true)__  
Un UDF determinista es uno que siempre da el mismo resultado cuando pasa
los mismos parámetros. Mientras que uno no determinista no devuelve el
mismo resultado como puede ser unix_timestamp()   

Y las descripciones  
__@Description(
    name="ExampleUDF",
    value="resultado de DESCRIBE FUNCTION nombre_funcion;",
    extended="resultado de DESCRIBE FUNCTION EXTENDED nombre_funcion;"
)__   

Ejempo
```java
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ExampleGenericUDF extends GenericUDF {
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
	private ObjectInspector[] argumentOIs;

	@Override
	public Object evaluate(DeferredObject[] arguments)
                throws HiveException{  

  Object retVal =
returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
		if (retVal == null) { 			
      retVal =
returnOIResolver.convertIfNecessary(arguments[1].get(), argumentOIs[1]);
		} 		return retVal;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		StringBuilder sb = new StringBuilder();
		sb.append("if ");
		sb.append(arg0[0]);
		sb.append(" is null ");
		sb.append("returns");
		sb.append(arg0[1]);
		return sb.toString();
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
                        throws UDFArgumentException { 		
    argumentOIs = arguments;
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException("Esperamos dos argumentos.");
		}
		returnOIResolver = new
GenericUDFUtils.ReturnObjectInspectorResolver(true); 		
 if (!(returnOIResolver.update(arguments[0]) &&
      returnOIResolver.update(arguments[1]))) {
        throw new UDFArgumentTypeException
        (2,"The 1st and 2nd args of function should have the same type"
        + ",but they are different: \""
        + arguments[0].getTypeName() + "\" and \""
        + arguments[1].getTypeName() + "\""); 		
  } 		

  return returnOIResolver.get();
	}

}
```
### Tipos GenericUDF Hive
#### ObjectInspector
Hay ObjectInspectors para todos los tipos y se clasifican entre
PrimitiveObjectInspector, ListObjectInspector, MapObjectInspector y
StructObjectInspector.

#### Initialize
Cuando se usa una UDF en una consulta, Hive carga la UDF en la memoria.
Se invoca initialize() por primera vez cuando se invoca el UDF. El
propósito de la llamada a este método es verificar el tipo de argumentos
que se pasarán a la UDF. Para cada valor que se pasará a la UDF, se
llamará al método de evaluación (). Entonces, si hay 10 filas para las
cuales se llamará la UDF, se llamará a () 10 veces. Sin embargo, Hive
llama primero al método initialize () de la UDF genérica antes de
realizar una llamada a evaluate(). Los objetivos de initialize() son

* validar los argumentos de entrada y quejarse si la entrada no es según
las expectativas
* guarde los inspectores de objetos de los argumentos de entrada para
usarlos posteriormente durante la evaluación ()
* proporcionar un inspector de objetos a Hive para el tipo de devolución

Puede hacer varias formas de validar la entrada, como verificar la
matriz de argumentos para tamaño, categoría en el tipo de entrada
(recuerde PrimitiveObjectInspector, MapObjectInspector, etc.),
verificando el tamaño de los objetos subyacentes (en el caso de un Map o
Struct, etc.). La validación puede ir en cualquier medida que elija,
incluso atravesar la jerarquía de objetos completa y validar cada
objeto. Cuando falla la validación, podemos lanzar una
UDFArgumentException o uno de sus subtipos para indicar un error.

El Inspector de objetos para el tipo de devolución debe construirse
dentro del método initialize () y devuelto. Podemos usar los métodos de
fábrica de la clase ObjectInspectorFactory. Por ejemplo, si la UDF va a
devolver un tipo de MAP, podemos usar el método
getStandardMapObjectInspector (), que acepta información sobre cómo se
construirá el Mapa (por ejemplo, el tipo de clave del Mapa y el tipo de
Valor del Mapa).

Los inspectores de Objetos guardados son instrumentales cuando tratamos
de obtener el valor de entrada en el método de evaluación ().




#### Evaluate
Supongamos que la tabla temporal tiene 10 filas. El método de evaluación
() se llamará 10 veces para cada valor de columna en 10 filas. Todos los
valores pasados ​​a evaluar () sin embargo son bytes serializados. Hive
retrasa la creación de instancias de objetos hasta que se realiza una
solicitud para el objeto, de ahí el nombre DeferredObject. Según el tipo
de valor que se pasó a la UDF, el objeto diferido podría representar
objetos inicializados de forma lenta. En el ejemplo anterior, podría ser
una instancia de la clase LazyDouble. Cuando se solicita el valor, como
LazyDouble.getWritableObject () , los bytes se deserializan en un objeto
y se devuelven.

Sin embargo, si se llama al mismo GenericUDF con un valor proporcionado
en la línea de comandos (en lugar de como resultado de IO), podría ser
un objeto DoubleWritable en primer lugar y no necesita una
deserialización. En función del tipo de objeto que obtenemos en la
Entrada, necesitamos usar sus datos en consecuencia y procesarlo.

Finalmente, de acuerdo con el tipo de entrada que recibimos, queremos
devolver el mismo tipo de Salida, ya que solo doblamos la entrada y
volvimos. El método convertIfNecessary() nos ayuda en esto y convierte
el tipo de salida en el mismo tipo de Entrada basado en el Inspector de
Objetos que le pasamos.
https://vpathak.wordpress.com/2015/07/18/writing-a-generic-udf-in-hive/
http://www.congiu.com/structured-data-in-hive-a-generic-udf-to-sort-arrays-of-structs/

A continuación mostraremos la corresponcia de Java con Hive para las
comparaciones
* Todos los tipos primitivos  
org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE
* List, para los arrays de Hive  
org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST
* Map, para los maps de Hive  
org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.MAP
* Struct, para los structs de Hive  
org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT

#### Primitivos
```java
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

PrimitiveCategory.BYNARY
PrimitiveCategory.INT
PrimitiveCategory.FLOAT
......
PrimitiveCategory.TIMESTAMP
......
```
https://hive.apache.org/javadocs/r2.2.0/api/org/apache/hadoop/hive/ql/udf/generic/GenericUDF.html

### Funciones de agregación definidas por el usuario UDAF
https://cwiki.apache.org/confluence/display/Hive/GenericUDAFCaseStudy  
Cómo escribir UDAF?

1) Cree una clase Java que herede de
org.apache.hadoop.hive.ql.exec.hive.UDAF;  
2) Crear clase interna que implementa UDAFEvaluator  
3) Implementar cinco métodos ()
* init() - El método init () inicializa al evaluador y restablece su
estado interno. Estamos utilizando una nueva Columna () en el código a
continuación para indicar que aún no se han agregado los valores.
* iterate(): este método se llama cada vez que hay un nuevo valor para
agregar. El evaulador debería actualizar su estado interno con el
resultado de realizar la agrregación (estamos haciendo suma, ver a
continuación). Devolvemos true para indicar que la entrada fue válida.
* terminatePartial(): se llama a este método cuando Hive quiere un
resultado para la agregación parcial. El método debe devolver un objeto
que encapsula el estado de la agregación.
* merge(): se llama a este método cuando Hive decide combinar una
agregación parcial con otra.
* terminate(): se llama a este método cuando se necesita el resultado
final de la agregación.  

4) Compilar y empaquetar JAR  
5) Crear función temporal en CLI de colmena  
6) Ejecutar consulta de agregación - Verificar salida

# #TODO revisar 172 libro hive
Estas funciones son más complejas
Tenemos dos (que vea) heredar de __UDAF__ o de
__AbstractGenericUDAFResolver__   Vermos los pasos de __UDAF__ que esta
deprecated Crear una clase que herede de
org.apache.hadoop.hive.ql.exec.hive.UDAF; Crear una clase interna que
implemente UDAFEvaluator; Implemetar los métodos:  
__init()__ – The init() method initializes the evaluator and resets its
internal state. We are using new Column() in the code below to indicate
that no values have been aggregated yet.   __iterate()__ – this method
is called every time there is a new value to be aggregated. The
evaulator should update its internal state with the result of performing
the aggregation (we are doing sum – see below). We return true to
indicate that the input was valid.   __terminatePartial()__ – this
method is called when Hive wants a result for the partial aggregation.
The method must return an object that encapsulates the state of the
aggregation.   __merge()__ – this method is called when Hive decides to
combine one partial aggregation with another.   __terminate()__ – this
method is called when the final result of the aggregation is needed

### Funciones de agregacion/analíticas
Las funciones analíticas generalmente se usan con OVER, PARTITION BY,
ORDER BY y la especificación de ventana.Las funciones de agregaciones
estándar:   COUNT(), SUM(), MIN(), MAX(), AVG().  
```hql
select count(*) as total from datos;
select count(*) as total from datos group by ciudades;
```
### Funciones de ventana
Las funciones de ventana le permiten a uno ver valores de la misma
columna previos o siguientes

Veamos un Ejemplo

```
1;2;
1;3;
1;12;
1;1;
1;3;
1;5;
1;1;
1;3;
1;4;
2;4;
2;2;
2;4;
2;0;
2;1;
2;1;
2;3;
2;4;
2;3;
3;4;
3;0;
3;0;
3;4;
3;;  
3;5;
3;5;
```
```hql
USE david;
CREATE EXTERNAL TABLE IF NOT EXISTS david.ventana(
         grupo STRING COMMENT 'agrupacion',
         dato INT COMMENT 'valor obtenido'
)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\;'
        LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/home/cloudera/datos_ventana.csv' OVERWRITE INTO
TABLE ventana;
```
Con la siguiente consulta para mostrar los datos previos, calcularemos
la diferencia con el posterior menos dato, además mostremos el siguiente
dato.
```
hive (david)> SELECT dato,lag(dato) OVER (PARTITION BY grupo) - dato as
diff, lead(dato) OVER (PARTITION BY grupo) as next FROM ventana; .....
Total MapReduce CPU Time Spent: 3 seconds 300 msec
OK                                                
dato    diff  next                              
1       NULL    3                                 
3       -2      5                                 
5       -2      2                                 
2       3       1                                 
1       1       3                                 
3       -2      12                                
12      -9      4                                 
4       8       3                                 
3       1       NULL                              
0       NULL    3                                 
3       -3      4                                 
4       -1      3                                 
3       1       1                                 
1       2       1                                 
1       0       4                                 
4       -3      2                                 
2       2       4                                 
4       -2      NULL                              
4       NULL    0                                 
0       4       0                                 
0       0       4                                 
4       -4      NULL                              
NULL    NULL    5                                 
5       NULL    5                                 
5       0       NULL                              
Time taken: 30.649 seconds, Fetched: 25 row(s)    
```
Como podemos ver para las funciones de ventana __LAG__ y __LEAD__
necesita un __OVER__, en este le indicaremos por el que vamos a agrupar
que puede estar ordenado también.   Siendo esta la sintaxis
```hql
LEAD(column, desplazamiento, default) OVER( window_spec)
LAG(column, desplazamiento, default) OVER( window_spec)
```
Donde indicamos la columna, el desplazamiento, y el valor por defecto
#### First_Value Last_Value
```
hive (david)> SELECT grupo,dato,first_value(dato) OVER (PARTITION BY
grupo) as next FROM ventana;
OK
grupo   dato    next
1       1       1
1       3       1
1       5       1
1       2       1
1       1       1
1       3       1
1       12      1
1       4       1
1       3       1
2       0       0
2       3       0
2       4       0
2       3       0
2       1       0
2       1       0
2       4       0
2       2       0
2       4       0
3       4       4
3       0       4
3       0       4
3       4       4
3       NULL    4
3       5       4
3       5       4
```

```hql
select lag(dato1, 1) over (order by dato1) as previous, dato1,
lead(dato1, 1) over (order by dato1) as next from datos; select
lag(dato1, 1) over (order by dato1) as previous, dato1, lead(dato1, 1)
over (order by dato1) as next from foo;
```
SELECT dato1, LAG(dato1, 3, 0) OVER (ORDER BY dato1) FROM datos;
#### ROW_NUMBER
 Asigna un número de secuencia único que comienza en 1 a cada fila de
acuerdo con la especificación de partición y orden; es una función de
ventana, por lo que debe usarse junto con una cláusula OVER, en este
ejemplo ordenaremos por dato.
```
hive (david)> SELECT grupo, dato, ROW_NUMBER ( ) OVER (ORDER BY dato)
FROM ventana;
OK                   
grupo   dato    _wcol
3       NULL    1    
3       0       2    
3       0       3    
2       0       4    
2       1       5    
1       1       6    
1       1       7    
2       1       8    
1       2       9    
2       2       10   
2       3       11   
2       3       12   
1       3       13   
1       3       14   
1       3       15   
2       4       16   
1       4       17   
3       4       18   
2       4       19   
3       4       20   
2       4       21   
1       5       22   
3       5       23   
3       5       24   
1       12      25   
```
http://www.hadoopinsight.com/blog/hive/hive-analytic-functions/

#### CUME_DIST
Calcula el número de filas cuyo valor es menor o igual al valor del
número total de filas divididas por la fila actual, el último será 1.  
En este caso tenemos 25 registros, 1/25= 0.04, por ello se muestra las
frecuencias acumuladas.
```
hive (david)> SELECT grupo, dato, CUME_DIST( ) OVER (ORDER BY dato) FROM
ventana;
......................  
OK                    
grupo   dato    _wcol0
3       NULL    0.04  
3       0       0.16  
3       0       0.16  
2       0       0.16  
2       1       0.32  
1       1       0.32  
1       1       0.32  
2       1       0.32  
1       2       0.4   
2       2       0.4   
2       3       0.6   
2       3       0.6   
1       3       0.6   
1       3       0.6   
1       3       0.6   
2       4       0.84  
1       4       0.84  
3       4       0.84  
2       4       0.84  
3       4       0.84  
2       4       0.84  
1       5       0.96  
3       5       0.96  
3       5       0.96  
1       12      1.0   
```
#### PERCENT_RANK() o rango percentil
Donde la formula es (rk-1)/(rn-1), con rk = rango del valor, rn = total
de itmes.   Es similar a CUME_DIST, pero usa valores de rango en lugar
de conteos de filas. Por lo tanto, devuelve el rango porcentual de un
valor relativo a un grupo de valores.
```
hive (david)> SELECT grupo, dato, PERCENT_RANK( ) OVER (ORDER BY dato)
FROM ventana;  
OK                                       
grupo   dato    _wcol0                   
3       NULL    0.0                      
3       0       0.041666666666666664     
3       0       0.041666666666666664     
2       0       0.041666666666666664     
2       1       0.16666666666666666      
1       1       0.16666666666666666      
1       1       0.16666666666666666      
2       1       0.16666666666666666      
1       2       0.3333333333333333       
2       2       0.3333333333333333       
2       3       0.4166666666666667       
2       3       0.4166666666666667       
1       3       0.4166666666666667       
1       3       0.4166666666666667       
1       3       0.4166666666666667       
2       4       0.625                    
1       4       0.625                    
3       4       0.625                    
2       4       0.625                    
3       4       0.625                    
2       4       0.625                    
1       5       0.875                    
3       5       0.875                    
3       5       0.875                    
1       12      1.0                      
```
#### NTILE
Divide un conjunto de datos ordenado en una cantidad de cubos y asigna
un número de cubeta adecuado a cada fila. Se puede usar para dividir
filas en conjuntos iguales y asignar un número a cada fila.
```
hive (david)> SELECT grupo, dato, NTILE(2) OVER (ORDER BY dato) FROM
ventana;  
OK                    
grupo   dato    _wcol0
3       NULL    1     
3       0       1     
3       0       1     
2       0       1     
2       1       1     
1       1       1     
1       1       1     
2       1       1     
1       2       1     
2       2       1     
2       3       1     
2       3       1     
1       3       1     
1       3       2     
1       3       2     
2       4       2     
1       4       2     
3       4       2     
2       4       2     
3       4       2     
2       4       2     
1       5       2     
3       5       2     
3       5       2     
1       12      2     
```

#### Sampling / Muestreo
Cuando nuestro volumén de datos es muy grande puede que necesitemos una
muestra de los datos para poder manejarlas, con el fin de analizarlas y
obtener patrones y tendencias, en hive tenemos 3 tipos de muestreos
__Muestreo Aleaotorio Simple (MAS)__, __Muestreo por cubetas__ y
__Muestreo en bloque__.   

Muestreo aleatorio: utiliza la función RAND() y la palabra clave LIMIT
para obtener el muestreo de datos. Las palabras clave DISTRIBUTE y SORT
se usan aquí para asegurarse de que los datos también se distribuyan
aleatoriamente entre los mapeadores y los reductores de manera
eficiente. La instrucción ORDER BY RAND () también puede lograr el mismo
propósito, pero el rendimiento no es bueno.
```hql
SELECT * FROM <Table_Name> DISTRIBUTE BY RAND() SORT BY RAND()
LIMIT <N rows to sample>;
```
```
hive (david)> SELECT * FROM ventana DISTRIBUTE BY RAND() SORT BY RAND()
LIMIT 5;   
OK
ventana.grupo   ventana.dato
1       12
3       0
1       3
1       1
2       3
Time taken: 67.383 seconds, Fetched: 5 row(s)
```
Muestre por cubetas/bucket: es un muestreo especial optimizado para
mesas de cubetas. El valor de colname especifica la columna donde
muestrear los datos. La función RAND () también se puede usar cuando el
muestreo está en todas las filas. Si la columna de muestra también es la
columna CLUSTERED BY, la instrucción TABLESAMPLE será más eficiente
```hql
SELECT * FROM <Table_Name> TABLESAMPLE(BUCKET <specified bucket number
to sample> OUT OF <total number of buckets> ON [colname|RAND()])
table_alias;  

SELECT name FROM employee_id_buckets TABLESAMPLE(BUCKET 1
OUT OF 2 ON rand()) a;
```

Muestreo en bloque : permite que Hive recoja aleatoriamente N filas de
datos, porcentaje (n porcentaje) de tamaño de datos o tamaño de bytes N
de datos. La granularidad de muestreo es el tamaño de bloque HDFS.

```hql
SELECT * FROM <Table_Name> TABLESAMPLE(N PERCENT|ByteLengthLiteral |
NROWS) s;
SELECT name FROM employee_id_buckets TABLESAMPLE(4 ROWS) a;
SELECT name FROM employee_id_buckets TABLESAMPLE(10 PERCENT) a;
```

## Funciones
Veremos unas funciones que nos pueden ser de ayuda.

### Fechas
Tratamiento de fechas
```
hive (david_lopez)> select unix_timestamp('2018-01-01 00:00:00');
OK
_c0
1514782800
```

```
hive (david_lopez)> select from_unixtime(1234123);
OK
_c0
1970-01-15 01:48:43
```


```
hive (david_lopez)> select TO_DATE('2000-01-01 00:00:00');
OK
_c0
2000-01-01
```

```
hive (david_lopez)> select YEAR('2000-01-01 00:00:00');
OK
_c0
2000
```

```
hive (david_lopez)> select MONTH('2000-01-01 00:00:00');
OK
_c0
1
```

```
hive (david_lopez)> select DAY('2000-01-01 00:00:00');
OK
_c0
1
```

```
hive (david_lopez)> select DAYOFMONTH('2000-01-01 00:00:00');
OK
_c0
1
```

```
hive (david_lopez)> select DAYOFMONTH('2000-02-01 00:00:00');
OK
_c0
1
```

```
hive (david_lopez)> select HOUR('2000-02-01 23:12:30');
OK
_c0
23
```

```
hive (david_lopez)> select MINUTE('2000-02-01 23:12:30');
OK
_c0
12
```

```
hive (david_lopez)> select SECOND('2000-02-01 23:12:30');
OK
_c0
30
```

```
hive (david_lopez)> select WEEKOFYEAR('2000-02-01 23:12:30');
OK
_c0
5
```

```
hive (david_lopez)> select DATEDIFF('2000-02-01 23:12:30','2000-01-01
12:23:34'); OK
_c0
31
```

```
hive (david_lopez)> select DATE_ADD('2000-02-01 23:12:30',5);
OK
_c0
2000-02-06
```

```
hive (david_lopez)> select DATE_SUB('2000-02-01 23:12:30',5);
OK
_c0
2000-01-27
```
### Float
Aproximar por arriba
```
hive (david_lopez)> select ceil(8.8);
OK
_c0
9
```
```
hive (david_lopez)> select ceil(8.2);
OK
_c0
9
```
Truncar
```
hive (david_lopez)> select floor(8.8);
OK
_c0
8
```
```
hive (david_lopez)> select floor(8.2);
OK
_c0
8
```

Redondeo
```
hive (david_lopez)> select round(8.8);
OK
_c0
9.0
```
```
hive (david_lopez)> select round(8.2);
OK
_c0
8.0
```
### String


## FECHA
Si tenemos un formato de fecha y queremos cambiar el formatod de salida
para las fechas, podemos hacer un cambio así.
```
# formato a cambiar
2017-10-19 11:32:50

select from_unixtime(unix_timestamp(fecha, 'yyyy-MM-dd HH:mm:ss'),
      'dd/MMM/yyyy:HH:mm:ss') as fechaFormatoNuevo from datos limit 1;

# formato con salida nueva
15/Nov/2016:19:08:13
```

```
hive (david_lopez)> select from_unixtime(unix_timestamp
('19/Oct/2017:03:48:41', 'dd/MMM/yyy:HH:mm:ss'),'yyyy-MM-dd HH:mm:ss');
OK _c0
2017-10-19 03:48:41
```


dada una fecha calcula el tiempo
```
hive (david_lopez)> SELECT unix_timestamp ('2009-03-20', 'yyyy-MM-dd');
OK                                                                      
_c0                                                                     
1237521600                                                              
Time taken: 0.099 seconds, Fetched: 1 row(s)                            
hive (david_lopez)> SELECT unix_timestamp ('2009-Mar-20',
'yyyy-MMM-dd');
OK                                                      
_c0
1237521600                                            

```

Dado un tiempo calcula el datatime
```
hive (david_lopez)> select
from_unixtime(1237521600,'dd/MMM/yyyy:HH:mm:ss');
OK
_c0
20/Mar/2009:00:00:00
```

ejemplo que viene como string y crearemos
```
....
66.249.93.4     19/Oct/2017:05:11:16    GET
66.249.93.4     19/Oct/2017:05:11:16    GET


hive (david_lopez)> select from_unixtime(unix_timestamp (fecha,
'dd/MMM/yyy:HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') from datos; ...
OK
_c0
2017-10-16 07:47:56
```




veamos un caso, en el que tenemos los datos en String y lo vamos a pasar
a una tabla con timestamp

```

CREATE EXTERNAL TABLE temporal
(
  FECHA STRING COMMENT 'String de la fecha en la que se produjo el
)
COMMENT 'Tabla ...'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        LOCATION '/user/david/temps';

CREATE EXTERNAL TABLE datos
(
  FECHA TIMESTAMP COMMENT 'Fecha ...',
)
COMMENT 'Tabla ...'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert into table datos select from_unixtime(unix_timestamp (fecha,
'dd/MMM/yyy:HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') from temporal;
```



## UDF maven

Para generar el proyecto podemos hacerlo de varias formas, por consola o
por el ide, a continuación veremos una configuración básica
```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
 <modelVersion>4.0.0</modelVersion>

 <groupId>es.pragsis.analytics</groupId>
 <artifactId>ParentUDF</artifactId>
 <version>0.0.1-SNAPSHOT</version>
 <packaging>jar</packaging>

 <name>myudfs</name>

 <properties>
   <hive.version>1.1.0-cdh5.5.0-SNAPSHOT</hive.version>
   <hadoop.version>2.6.0-cdh5.5.0-SNAPSHOT</hadoop.version>
 </properties>

 <build>
   <pluginManagement>
     <plugins>
       <plugin>
         <groupId>org.apache.maven.plugins</groupId>
         <artifactId>maven-compiler-plugin</artifactId>
         <version>2.3.2</version>
         <configuration>
           <source>1.6</source>
           <target>1.6</target>
         </configuration>
       </plugin>
     </plugins>
   </pluginManagement>
 </build>

 <dependencies>
   <dependency>
     <groupId>org.apache.hive</groupId>
     <artifactId>hive-exec</artifactId>
     <version>${hive.version}</version>
   </dependency>
   <dependency>
     <groupId>org.apache.hadoop</groupId>
     <artifactId>hadoop-common</artifactId>
     <version>${hadoop.version}</version>
   </dependency>
 </dependencies>

</project>
```
Podriamos crear un padre por ejemplo para heredar
