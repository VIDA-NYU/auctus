import pandas

from . import types


BOOLEAN_MAP = {
    '0': False,
    '1': True,
    'false': False,
    'true': True,
    'n': False,
    'no': False,
    'y': True,
    'yes': True,
}


def cast_to_structural_types(dataframe, columns):
    """Use profile information to cast a DataFrame to the right dtypes.
    """
    if isinstance(columns, dict) and 'columns' in columns:
        columns = columns['columns']
    if not isinstance(columns, list):
        raise TypeError("Expected list for 'columns', got %s" % type(columns))

    for name, column in zip(dataframe.columns, columns):
        str_type = column['structural_type']
        if str_type in (types.MISSING_DATA, types.TEXT):
            dataframe[name] = dataframe[name].astype(str)
        elif str_type == types.INTEGER:
            dataframe[name] = pandas.to_numeric(
                dataframe[name],
                errors='coerce',
                downcast='integer',
            )
        elif str_type == types.FLOAT:
            dataframe[name] = pandas.to_numeric(
                dataframe[name],
                errors='coerce',
                downcast='float',
            )
        elif str_type in (types.GEO_POINT, types.GEO_POLYGON):
            # TODO: Spatial types
            dataframe[name] = dataframe[name].astype(str)
        else:
            raise ValueError(
                "Unknown structural type %r for column %r" % (str_type, name)
            )

    for name, column in zip(dataframe.columns, columns):
        sem_types = set(column['semantic_types'])
        if types.BOOLEAN in sem_types:
            dataframe[name] = dataframe[name].apply(
                lambda e: BOOLEAN_MAP.get(e.lower())
            )
        elif types.DATE_TIME in sem_types:
            dataframe[name] = pandas.to_datetime(
                dataframe[name],
                errors='coerce',
            )

    return dataframe
