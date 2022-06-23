select 
	universidad as university,
	careers as career,
	to_date(fecha_de_inscripcion,'dd/Mon/yy') as inscription_date,
    split_part(names,'_',1) as first_name,
    split_part(names,'_',2) as last_name,
    sexo as gender,
    age(to_date(birth_dates,'dd/Mon/yy')) as age,
    codigo_postal as postal_code,
    direcciones as location,
    correos_electronicos as email
from 
	palermo_tres_de_febrero ptdf 
where 
	universidad  = 'universidad_nacional_de_tres_de_febrero' and
    to_date(fecha_de_inscripcion,'dd/Mon/yy') between '2020/09/01' and '2021/02/01'
