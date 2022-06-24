/*
Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para las siguientes facultades:
- Universidad Abierta Interamericana
*/
SELECT  univiersities AS university,
		trabajo AS career,
		TO_DATE(inscription_dates , 'yy/Mon/dd') AS inscription_date, 		-- Casting value to date type
		SPLIT_PART(names, '-', 1) AS first_name,							-- Getting first name from value
		SPLIT_PART(names, '-', 2) AS last_name, 							-- Getting last name name from value
		sexo AS gender, 
		AGE(TO_DATE(fechas_nacimiento , 'yy/Mon/dd')) AS age, 
		REVERSE(SPLIT_PART(REVERSE(direcciones), '-', 1))  AS postal_code,	-- After splitting string, it gets the postal code (last element)
		direcciones AS location, 
		email AS email
FROM 	rio_cuarto_interamericana rci
WHERE 	univiersities  = '-universidad-abierta-interamericana' AND
		TO_DATE(inscription_dates , 'yy/Mon/dd') BETWEEN '2020-09-01' AND '2021-02-01';