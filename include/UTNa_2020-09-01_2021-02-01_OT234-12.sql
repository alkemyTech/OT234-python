SELECT 
	university,
	career,
	inscription_date,
    SPLIT_PART(nombre ,' ',1) as first_name,
    SPLIT_PART(nombre ,' ',2) as last_name,
    sexo as gender,
    AGE(TO_DATE(birth_date,'yyyy/mm/dd')) as age,
    REVERSE(SPLIT_PART(REVERSE(direccion),'-',1))  as postal_code,
    location,
    email
FROM 
	jujuy_utn ju 
WHERE 
	university  = 'universidad tecnológica nacional' and 
	inscription_date  between '2020/09/01' and '2021/02/01'
