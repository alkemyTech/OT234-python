-- UNVM_2020-09-01_2021-02-01_OT234-12.sql
/*COMO: Analista de datos
QUIERO: Escribir el c�digo de dos consultas SQL, una para cada universidad.
PARA: Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para las siguientes facultades: 

Universidad De Flores

Universidad Nacional De Villa Mar�a

Criterios de aceptaci�n: 
Deben presentar la consulta en un archivo .sql. La consulta debe disponibilizar �nicamente la informaci�n necesaria para que en un futuro sea procesada y genere los siguientes datos para las fechas indicadas. 
Datos esperados:

university -
career -
inscription_date -
first_name -
last_name -
gender -
age -
postal_code -
location -
email -

Aclaraci�n: Las tablas tienen dos universidades cada una. No hace falta interpretar datos que no parezcan l�gicos como fechas de nacimiento y de inscripci�n fuera del rango de inter�s. Lo importante es traer toda la informaci�n de la base de datos en las fechas especificadas y cada tarea se debe ejecutar 5 veces antes de fallar.*/
/*
select fecha_nacimiento,
	to_date(fecha_nacimiento, 'dd/Mon/yy'),
	age(to_date(fecha_nacimiento, 'dd-Mon-yy'))
from salvador_villa_maria svm limit 3

select distinct (to_date(fecha_nacimiento, 'dd-Mon-yy')) from salvador_villa_maria svm 
order by 1 desc*/


select universidad as university,
	carrera as career,
	fecha_de_inscripcion as inscription_date,
	SPLIT_PART(nombre ,' ',1) as first_name,
	split_part(nombre, ' ', 2) as last_name,
	sexo as gender,	
	AGE(TO_DATE(fecha_nacimiento , 'dd-Mon-yy')) AS age,
	direccion as postal_code,--cruzar con csv		
	direccion as location,
	email as email
from salvador_villa_maria svm 
where universidad ='UNIVERSIDAD_NACIONAL_DE_VILLA_MAR�A'   
limit 10

