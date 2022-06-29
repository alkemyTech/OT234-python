/*
Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para las siguientes facultades:
- Universidad J. F. Kennedy
*/

SELECT  universidades AS university,
		carreras AS career,
		TO_DATE(fechas_de_inscripcion , 'yy/Mon/dd') AS inscription_date,
        SPLIT_PART(nombres, '-', 2) AS last_name,
		sexo AS gender, 
		AGE(TO_DATE(fechas_nacimiento , 'yy/Mon/dd')) AS age, 
		REVERSE(SPLIT_PART(REVERSE(direcciones), '-', 1))  AS postal_code,
		emails AS email
FROM 	uba_kenedy
WHERE 	universidades  = 'universidad-j.-f.-kennedy' AND
		TO_DATE(fechas_de_inscripcion , 'yy/Mon/dd') BETWEEN '2020-09-01' AND '2021-02-01'; 