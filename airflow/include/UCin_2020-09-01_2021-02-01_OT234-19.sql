SELECT	
		universities AS university,
		careers AS career,
		inscription_dates AS inscription_date,
		SPLIT_PART(names, '-', 1) AS first_name,
		SPLIT_PART(names, '-', 2) AS last_name,
		sexo AS gender,
		DATE_PART('year', AGE(current_date ,TO_DATE(birth_dates, 'DD-MM-YYYY'))) AS age,
		NULL AS postal_code,
		locations AS location,
		emails AS email
FROM 	
		lat_sociales_cine lsc
WHERE	
		universities  = 'UNIVERSIDAD-DEL-CINE' AND
		TO_DATE(inscription_dates, 'DD-MM-YY') BETWEEN '09-01-2020' AND '02-01-2021';