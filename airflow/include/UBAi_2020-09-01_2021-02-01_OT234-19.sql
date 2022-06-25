SELECT	
        universidades AS university,
		carreras AS career,
		fechas_de_inscripcion AS inscription_date,
		SPLIT_PART(nombres, '-', 1) AS first_name,
		SPLIT_PART(nombres, '-', 2) AS last_name,
		sexo AS gender,
		DATE_PART('year', AGE(current_date ,TO_DATE(fechas_nacimiento, 'YY-Mon-DD'))) AS age,
		codigos_postales AS postal_code,
		direcciones AS location,
		emails AS email
FROM 	
        uba_kenedy uk
WHERE	
        universidades = 'universidad-de-buenos-aires'AND 
		TO_DATE(fechas_de_inscripcion , 'YY-Mon-DD') BETWEEN '09-01-2020' AND '02-01-2021';