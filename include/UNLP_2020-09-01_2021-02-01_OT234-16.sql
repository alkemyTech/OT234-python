/*
Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para las siguientes facultades:
- Universidad Nacional De La Pampa
*/
SELECT  universidad AS university,
		carrerra AS career,
		TO_DATE(fechaiscripccion, 'dd/mm/yyyy') AS inscription_date,		-- Casting values to date type
		SPLIT_PART(nombrre, ' ', 1) AS first_name,							-- Getting first name from value
		SPLIT_PART(nombrre, ' ', 2) AS last_name, 							-- Getting last name name from value
		sexo AS gender, 
		AGE(TO_DATE(nacimiento, 'dd/mm/yyyy')) AS age, 
		codgoposstal AS postal_code, 
		direccion AS location, 
		eemail AS email
FROM 	moron_nacional_pampa 
WHERE 	universidad = 'Universidad nacional de la pampa' AND
		TO_DATE(fechaiscripccion, 'dd/mm/yyyy') BETWEEN '2020-09-01' AND '2021-02-01';