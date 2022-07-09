select 
	universidad as university,
	careers as career,
	fecha_de_inscripcion as inscription_date,
    names as names,
    sexo as gender,
    birth_dates as age,
    codigo_postal as postal_code,
    direcciones as location,
    correos_electronicos as email
from 
	palermo_tres_de_febrero ptdf 
where 
	universidad  = 'universidad_nacional_de_tres_de_febrero' and
    to_date(fecha_de_inscripcion,'dd/Mon/yy') between '2020/09/01' and '2021/02/01'