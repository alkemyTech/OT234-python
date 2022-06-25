/* Query Universidad de Palermo */
/* Required fields to extract through postprocessing:  university, career, inscription_date,
 first_name, last_name, gender, age, postal_code, location, email*/

SELECT 
	universidad,
    careers,
	TO_DATE(fecha_de_inscripcion,'DD/MON/YY') as fecha_de_inscripcion,
    names, 
    sexo,   
    birth_dates, 
    direcciones, 
	codigo_postal
    correos_electronicos
    
FROM
   palermo_tres_de_febrero
   
WHERE
	universidad = '_universidad_de_palermo' AND
	TO_DATE(fecha_de_inscripcion,'DD/MON/YY') BETWEEN '2020/09/01' AND '2021/02/01'
	