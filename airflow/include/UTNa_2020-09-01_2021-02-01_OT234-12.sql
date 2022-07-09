SELECT 
	university,
	career,
	inscription_date,
    nombre as names,
    sexo as gender,
    birth_date as birth_date,
    direccion as postal_code,
    location,
    email
FROM 
	jujuy_utn ju 
WHERE 
	university  = 'universidad tecnológica nacional' and 
	inscription_date  between '2020/09/01' and '2021/02/01'
