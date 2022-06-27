import pandas as pd

def to_lower_strip(serie: pd.Series) -> pd.Series:
    """_summary_

    Args:
        serie (pd.Series): _description_

    Returns:
        pd.Series: _description_
    """
    return serie.str.lower().str.replace(' ','').str.replace('-','')

def transform_OT234_72(raw_data: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        raw_data (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    columns = ['university', 'career', 'first_name', 'last_name', 'location', 'email']
    data = raw_data.copy()
    data[columns] = data[columns].apply(to_lower_strip)
    data['gender'] = data.gender.map({'M':'male','F':'female'})
    data['age'] = (data.age.str.replace(' days','').astype('int')/365).astype('int')
    dataset = data
    return dataset

if __name__ == '__main__':
    print('This module has fuctions to transform data according to issue OT234-72')