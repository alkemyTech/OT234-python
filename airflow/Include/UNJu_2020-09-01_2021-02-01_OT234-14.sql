/* Query Universidad Nacional de Jujuy */

/* Required fields to extract through postprocessing:  university, career, inscription_date,
 first_name, last_name, gender, age, postal_code, location, email*/

SELECT
    university,
    career,
    TO_DATE(inscription_date,'YYYY/MM/DD') as inscription_date,
    nombre, 
    sexo,   
    birth_date, 
    direccion, 
    email
FROM
   jujuy_utn
WHERE 
	university = 'universidad nacional de jujuy' AND
	 TO_DATE(inscription_date,'YYYY/MM/DD') BETWEEN '2020/09/01' AND '2021/02/01'