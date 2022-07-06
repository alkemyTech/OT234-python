"Obtener los datos de las personas anotadas en entre las fechas 01/9/2020 al 01/02/2021 para las facultad
Universidad Del Salvador"

SELECT universidad,
      carrera,
      fecha_de_inscripcion,
      SPLIT_PART(nombre, '_',1) AS primer_nombre,
      SPLIT_PART(nombre, '_',1) AS segundo_nombre,
      sexo,
      fecha_nacimiento,
      AGE(to_date(fecha_nacimiento, 'dd/Mon/yy')) AS edad,
      SPLIT_PART(direccion, '_',1) as codigo_postal,
      localidad,
      email
FROM 
      Salvador_villa_maria
WHERE 
      universidad = 'UNIVERSIDAD_DEL_SALVADOR' and
      fecha_de_inscripcion BETWEEN '01-Apr-20' and '01-Feb-21';
