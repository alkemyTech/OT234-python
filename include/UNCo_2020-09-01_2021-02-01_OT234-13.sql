"Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para la facultad:
Univ. Nacional Del Comahue"

SELECT universidad,
	   carrera,
	   fecha_de_inscripcion,
	   SPLIT_PART(name, ' ',1) AS primer_nombre,
	   SPLIT_PART(name, ' ',2) AS segundo_nombre,
	   sexo,
	   AGE(to_date(fecha_nacimiento, 'yyyy-Mo-dd')) AS edad,
	   codigo_postal,
	   direccion,
	   correo_electronico	   	   
FROM 
      flores_comahue
WHERE
	universidad='UNIV. NACIONAL DEL COMAHUE' AND
      fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01';
