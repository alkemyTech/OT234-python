
/*
Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para las siguientes facultades:
- Facultad Latinoamericana De Ciencias Sociales
*/

SELECT universities AS university,
       careers AS career,
       TO_DATE(inscription_dates , 'dd/mm/yyyy') AS inscription_date,
	   SPLIT_PART(names, '-', 1) AS first_name,							
	   SPLIT_PART(names, '-', 2) AS last_name,
       sexo AS gender,
       AGE(TO_DATE(birth_dates , 'dd/mm/yyyy')) AS age,
	   REVERSE(SPLIT_PART(REVERSE(direccion), '-', 1))  AS postal_code,
	   locations AS location,
       emails AS email
FROM lat_sociales_cine
WHERE 	universities  = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES' AND
		TO_DATE(inscription_dates , 'dd/mm/yyyy') BETWEEN '2020-09-01' AND '2021-02-01'; 