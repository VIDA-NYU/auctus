import numpy as np
import os
import pandas as pd
import shutil
import tempfile
import uuid

from datamart_core.common import Type
from datamart_materialize.d3m import D3mWriter


temporal_resolutions = [
    'second',
    'minute',
    'hour',
    'date'
]


def convert_to_pd(file_path, columns_metadata):
    """
    Convert a dataset to pandas.DataFrame based on the provided metadata.
    """

    df = pd.read_csv(
        file_path,
        error_bad_lines=False
    )

    for column in columns_metadata:
        name = column['name']
        if Type.DATE_TIME in column['semantic_types']:
            df[name] = pd.to_datetime(df[name], errors='coerce')
        elif column['structural_type'] in (Type.INTEGER, Type.FLOAT):
            df[name] = pd.to_numeric(df[name], errors='coerce')

    return df


def get_temporal_resolution(data):
    for res in temporal_resolutions[:-1]:
        if len(set([eval('x.%s' % res) for x in data[data.notnull()]])) > 1:
            return temporal_resolutions.index(res)
    return temporal_resolutions.index('date')


def fix_temporal_resolution(left_data, right_data,
                            left_temporal_column, right_temporal_column):
    """
    Put datasets into the same temporal resolution.
    This function modifies datasets in place.
    """

    res_left = get_temporal_resolution(left_data[left_temporal_column])
    res_right = get_temporal_resolution(right_data[right_temporal_column])
    if res_left > res_right:
        for res in temporal_resolutions[:res_left]:
            right_data[right_temporal_column] = [
                x.replace(**{res: 0}) for x in right_data[right_temporal_column]
            ]
    elif res_left < res_right:
        for res in temporal_resolutions[:res_right]:
            left_data[left_temporal_column] = [
                x.replace(**{res: 0}) for x in left_data[left_temporal_column]
            ]


def join(original_data, augment_data, left_columns, right_columns,
         columns=None, how='left', qualities=False,
         return_only_datamart_data=False):
    """
    Performs a join between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using left_columns and right_columns.

    Returns the new pandas.DataFrame object.
    """

    for i in range(len(left_columns)):
        if len(left_columns[i]) > 1 or len(right_columns[i]) > 1:
            raise Exception('DataMart currently does not support '
                            'combination between columns for augmentation.')

    # remove undesirable columns from augment_data
    # but first, make sure to keep the join keys
    if columns:
        for right_column in right_columns:
            columns.append(right_column[0])
        columns = set([augment_data.columns[c] for c in columns])
        drop_columns = list(set(augment_data.columns).difference(columns))
        augment_data = augment_data.drop(drop_columns, axis=1)

    rename = dict()
    for i in range(len(left_columns)):
        rename[augment_data.columns[right_columns[i][0]]] = \
            original_data.columns[left_columns[i][0]]
    augment_data = augment_data.rename(columns=rename)

    # matching temporal resolutions
    original_data_dt = original_data.select_dtypes(include=[np.datetime64]).columns
    augment_data_dt = augment_data.select_dtypes(include=[np.datetime64]).columns
    for i in range(len(left_columns)):
        column_name = original_data.columns[left_columns[i][0]]
        if column_name in original_data_dt and column_name in augment_data_dt:
            fix_temporal_resolution(original_data, augment_data, column_name, column_name)

    # TODO: work on aggregations

    # join
    join_ = pd.merge(
        original_data,
        augment_data,
        how=how,
        on=list(rename.values()),
        suffixes=('', '_r')
    )

    # qualities
    qualities_list = list()

    if return_only_datamart_data:
        # dropping columns from original data
        join_ = join_.drop(
            list(set(original_data.columns).difference(set(rename.values()))),
            axis=1
        )

        # dropping rows with all null values
        join_.dropna(axis=0, how='all', inplace=True)

        # finally, rename back columns
        rename_back = dict()
        for k, v in rename.items():
            rename_back[v] = k
        for column in join_.columns:
            if column.endswith('_r'):
                rename_back[column] = column[:-2]
        join_ = join_.rename(columns=rename_back)

    else:
        if qualities:
            new_columns = list(set(join_.columns).difference(
                set([c for c in original_data.columns])
            ))
            qualities_list.append(dict(
                qualName='augmentation_info',
                qualValue=dict(
                    new_columns=new_columns,
                    removed_columns=[],
                    nb_rows_before=original_data.shape[0],
                    nb_rows_after=join_.shape[0],
                    augmentation_type='join'
                ),
                qualValueType='dict'
            ))

    return join_, qualities_list


def union(original_data, augment_data, left_columns, right_columns,
          qualities=False):
    """
    Performs a union between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using columns.

    Returns the new pandas.DataFrame object.
    """

    for i in range(len(left_columns)):
        if len(left_columns[i]) > 1 or len(right_columns[i]) > 1:
            raise Exception('DataMart currently does not support '
                            'combination between columns for augmentation.')

    # saving all columns from original data
    original_data_cols = original_data.columns

    # dropping columns not in union
    original_columns = [original_data.columns[c[0]] for c in left_columns]
    original_columns.append('d3mIndex')
    augment_data_columns = [augment_data.columns[c[0]] for c in right_columns]
    original_data = original_data.drop(
        [c for c in original_data.columns if c not in original_columns],
        axis=1
    )
    augment_data = augment_data.drop(
        [c for c in augment_data.columns if c not in augment_data_columns],
        axis=1
    )

    rename = dict()
    for i in range(len(left_columns)):
        rename[augment_data.columns[right_columns[i][0]]] = \
            original_data.columns[left_columns[i][0]]
    augment_data = augment_data.rename(columns=rename)

    # matching temporal resolutions
    original_data_dt = original_data.select_dtypes(include=[np.datetime64]).columns
    augment_data_dt = augment_data.select_dtypes(include=[np.datetime64]).columns
    for i in range(len(left_columns)):
        column_name = original_data.columns[left_columns[i][0]]
        if column_name in original_data_dt and column_name in augment_data_dt:
            fix_temporal_resolution(original_data, augment_data, column_name, column_name)

    # union
    union_ = pd.concat([original_data, augment_data])

    # qualities
    qualities_list = list()
    if qualities:
        removed_columns = list(
            set([c for c in original_data_cols]).difference(
                union_.columns
            )
        )
        qualities_list.append(dict(
            qualName='augmentation_info',
            qualValue=dict(
                new_columns=[],
                removed_columns=removed_columns,
                nb_rows_before=original_data.shape[0],
                nb_rows_after=union_.shape[0],
                augmentation_type='union'
            ),
            qualValueType='dict'
        ))

    return union_, qualities_list


def generate_d3m_dataset(data, destination=None, qualities=None):
    """
    Generates a D3M dataset from data (pandas.DataFrame).

    Returns the path to the D3M-style directory.
    """

    def add_column(column_, type_, metadata_):
        metadata_['columns'].append({'name': column_})
        if 'datetime' in type_:
            metadata_['columns'][-1]['semantic_types'] = [Type.DATE_TIME]
            metadata_['columns'][-1]['structural_type'] = Type.TEXT
        elif 'int' in type_:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.INTEGER
        elif 'float' in type_:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.FLOAT
        elif 'bool' in type_:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.BOOLEAN
        else:
            metadata_['columns'][-1]['semantic_types'] = []
            metadata_['columns'][-1]['structural_type'] = Type.TEXT

    dir_name = uuid.uuid4().hex
    if destination:
        data_path = os.path.join(destination, dir_name)
        if os.path.exists(data_path):
            shutil.rmtree(data_path)
    else:
        temp_dir = tempfile.mkdtemp()
        data_path = os.path.join(temp_dir, dir_name)

    metadata = dict(columns=[])
    for column in data.columns:
        add_column(
            column,
            str(data[column].dtype),
            metadata
        )
    metadata['size'] = data.memory_usage(index=True, deep=True).sum()

    if qualities:
        metadata['qualities'] = qualities

    writer = D3mWriter(dir_name, data_path, metadata)
    with writer.open_file('w') as fp:
        data.to_csv(fp, index=False)

    return data_path


def augment(data, newdata, metadata, task, columns=None, destination=None,
            return_only_datamart_data=False):
    """
    Augments original data based on the task.

    :param data: the data to be augmented.
    :param newdata: the data to augment with.
    :param metadata: the metadata of the data to be augmented.
    :param task: the augmentation task.
    :param columns: a list of column indices from newdata that will be added to data
    :param destination: location to save the files.
    :param return_only_datamart_data: only returns the portion of newdata that matches
      well with data.
    """

    if 'id' not in task:
        raise RuntimeError('Dataset id for the augmentation task not provided.')

    if task['augmentation']['type'] == 'join':
        join_, qualities = join(
            convert_to_pd(data, metadata['columns']),
            convert_to_pd(newdata, task['metadata']['columns']),
            task['augmentation']['left_columns'],
            task['augmentation']['right_columns'],
            columns=columns,
            qualities=True,
            return_only_datamart_data=return_only_datamart_data
        )
        return generate_d3m_dataset(join_, destination, qualities)
    elif task['augmentation']['type'] == 'union':
        union_, qualities = union(
            convert_to_pd(data, metadata['columns']),
            convert_to_pd(newdata, task['metadata']['columns']),
            task['augmentation']['left_columns'],
            task['augmentation']['right_columns'],
            qualities=True
        )
        return generate_d3m_dataset(union_, destination, qualities)
    else:
        raise RuntimeError('Augmentation task not provided.')
