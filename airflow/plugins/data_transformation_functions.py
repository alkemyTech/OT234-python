import pandas as pd

def to_lower_strip(serie: pd.Series) -> pd.Series:
    """
    This function removes spaces (' '), hyphen ('-') symbols and converts strings to lowercase.
    e.g.:
    Args:
        'Esta-es-una cadena'
    Returns:
        'estaesunacadena'
    """
    return serie.str.lower().str.replace(' ','').str.replace('-','')

def transform_OT234_72(raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    This function implements all transformations required by OT234-80.
    Datos Finales:
        university: str minúsculas, sin espacios extras, ni guiones
        career: str minúsculas, sin espacios extras, ni guiones
        inscription_date: str %Y-%m-%d format
        first_name: str minúscula y sin espacios, ni guiones
        last_name: str minúscula y sin espacios, ni guiones
        gender: str choice(male, female)
        age: int
        postal_code: str
        location: str minúscula sin espacios extras, ni guiones
        email: str minúsculas, sin espacios extras, ni guiones
    """
    columns_to_lower_strip = ['university', 'career', 'first_name', 'last_name', 'location', 'email','location']
    data = raw_data.copy()
    cp = pd.read_csv('airflow/files/codigos_postales.csv').rename(columns={'codigo_postal':'postal_code'})
    merged = pd.merge(data[['postal_code', 'location']], cp, how='left', on = 'postal_code')
    mask = merged.localidad.notnull()
    data.loc[mask,'location'] = merged.localidad[mask].copy()
    data[columns_to_lower_strip] = data[columns_to_lower_strip].apply(to_lower_strip)
    data['gender'] = data.gender.map({'M':'male','F':'female'})
    data['age'] = (data.age.str.replace(' days','').astype('int')/365).astype('int')
    data['postal_code'] = data['postal_code'].astype('str')
    dataset = data.copy()
    return dataset

if __name__ == '__main__':
    print('This module has fuctions to transform data according to issue OT234-72')