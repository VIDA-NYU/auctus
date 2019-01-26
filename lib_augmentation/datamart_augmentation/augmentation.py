import elasticsearch
import numpy as np
import os
import pandas as pd
import shutil
import tempfile
import uuid

from .utils import conv_datetime, conv_float, conv_int
from datamart_core.common import Type
from datamart_core.materialize import get_dataset
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

    converters = dict()

    for column in columns_metadata:
        name = column['name']
        if Type.DATE_TIME in column['semantic_types']:
            converters[name] = conv_datetime
        elif Type.INTEGER in column['structural_type']:
            converters[name] = conv_int
        elif Type.FLOAT in column['structural_type']:
            converters[name] = conv_float

    return pd.read_csv(
        file_path,
        converters=converters,
        error_bad_lines=False
    )


def materialize_dataset(es, dataset_id):
    """
    Materializes a dataset as a pandas.DataFrame
    """

    # get metadata data from Elasticsearch
    try:
        metadata = es.get('datamart', '_doc', id=dataset_id)['_source']
    except elasticsearch.NotFoundError:
        raise RuntimeError('Dataset id not found in Elasticsearch.')

    getter = get_dataset(metadata, dataset_id, format='csv')
    try:
        dataset_path = getter.__enter__()
        df = convert_to_pd(dataset_path, metadata['columns'])
    except Exception:
        raise RuntimeError('Materializer reports failure.')
    finally:
        getter.__exit__(None, None, None)

    return df


def get_temporal_resolution(data):
    for res in temporal_resolutions[:-1]:
        if len(set([eval('x.%s' % res) for x in data if x != np.nan])) > 1:
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


def join(original_data, augment_data, columns, how='inner',
         qualities=False):
    """
    Performs an inner join between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using columns.

    Returns the new pandas.DataFrame object.
    """

    rename = dict()
    for c in columns:
        rename[c[1]] = c[0]
    augment_data = augment_data.rename(columns=rename)

    # matching temporal resolutions
    original_data_dt = original_data.select_dtypes(include=[np.datetime64]).columns
    augment_data_dt = augment_data.select_dtypes(include=[np.datetime64]).columns
    for c in columns:
        if c[0] in original_data_dt and c[0] in augment_data_dt:
            fix_temporal_resolution(original_data, augment_data, c[0], c[0])

    # join
    join_ = pd.merge(
        original_data,
        augment_data,
        how=how,
        on=[c[0] for c in columns],
        suffixes=('_l', '_r')
    )

    # remove all columns with 'd3mIndex'
    join_ = join_.drop([c for c in join_.columns if 'd3mIndex' in c], axis=1)

    # drop rows with missing values
    join_.dropna(axis=0, how='any', inplace=True)

    # qualities
    qualities_list = list()
    if qualities:
        new_columns = list(set(join_.columns).difference(
            set([c for c in original_data.columns if 'd3mIndex' not in c])
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

    # create a single d3mIndex
    join_['d3mIndex'] = pd.Series(
        data=[i for i in range(join_.shape[0])],
        index=join_.index
    )

    return join_, qualities_list


def union(original_data, augment_data, columns,
          qualities=False):
    """
    Performs a union between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using columns.

    Returns the new pandas.DataFrame object.
    """

    # saving all columns from original data
    original_data_cols = original_data.columns

    # dropping columns not in union
    original_columns = [c[0] for c in columns]
    augment_data_columns = [c[1] for c in columns]
    original_data = original_data.drop(
        [c for c in original_data.columns if c not in original_columns],
        axis=1
    )
    augment_data = augment_data.drop(
        [c for c in augment_data.columns if c not in augment_data_columns],
        axis=1
    )

    rename = dict()
    for c in columns:
        rename[c[1]] = c[0]
    augment_data = augment_data.rename(columns=rename)

    # matching temporal resolutions
    original_data_dt = original_data.select_dtypes(include=[np.datetime64]).columns
    augment_data_dt = augment_data.select_dtypes(include=[np.datetime64]).columns
    for c in columns:
        if c[0] in original_data_dt and c[0] in augment_data_dt:
            fix_temporal_resolution(original_data, augment_data, c[0], c[0])

    # union
    union_ = pd.concat([original_data, augment_data])

    # drop rows with missing values
    union_.dropna(axis=0, how='any', inplace=True)

    # qualities
    qualities_list = list()
    if qualities:
        removed_columns = list(
            set([c for c in original_data_cols if 'd3mIndex' not in c]).difference(
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

    # create a single d3mIndex
    union_['d3mIndex'] = pd.Series(
        data=[i for i in range(union_.shape[0])],
        index=union_.index
    )

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


def augment_data(left_data, right_data, left_columns, right_columns,
                 left_metadata, right_metadata, how='join', destination=None):
    """
    Augments two given datasets.

    :param left_data: the leftside dataset for augmentation
    :param right_data: the rightside dataset for augmentation
    :param left_columns: a list of lists of headers(str)
      of the leftside dataset
    :param right_columns: a list of lists of headers(str)
      of the rightside dataset
    :param left_metadata: the metadata of the leftside dataset
    :param right_metadata: the metadata of the rightside dataset
    :param how: type of augmentation ('join' or 'union')
    :param destination: location to save the files.
    """

    def get_column(column, metadata):
        name = None
        type_ = None
        if isinstance(column, list):
            item = column[0]
        else:
            item = column
        if isinstance(item, int):
            try:
                name = metadata['columns'][item]['name']
                if Type.DATE_TIME in metadata['columns'][item]['semantic_types']:
                    type_ = Type.DATE_TIME
                else:
                    type_ = metadata['columns'][item]['structural_type']
            except Exception:
                raise RuntimeError('Column not identified: %d' % item)
        elif isinstance(item, str):
            for c in metadata['columns']:
                if item.strip() == c['name']:
                    name = c['name']
                    if Type.DATE_TIME in c['semantic_types']:
                        type_ = Type.DATE_TIME
                    else:
                        type_ = c['structural_type']
                    break
            if not name:
                raise RuntimeError('Column not identified: %s' % item)
        else:
            raise RuntimeError('Column not identified: %r' % item)
        return name, type_

    if len(left_columns) != len(right_columns):
        raise RuntimeError('left_columns and right_columns must have the same length.')

    pairs = []
    for i in range(len(left_columns)):
        name_left, type_left = get_column(left_columns[i], left_metadata)
        name_right, type_right = get_column(right_columns[i], right_metadata)
        if type_left != type_right:
            raise RuntimeError('Columns %s and %s have different types: %s and %s' %
                               (name_left, name_right, type_left, type_right))
        pairs.append([name_left, name_right])

    if not pairs:
        raise RuntimeError('No columns for augmentation.')

    if 'join' in how:
        join_, qualities = join(
            convert_to_pd(left_data, left_metadata['columns']),
            convert_to_pd(right_data, right_metadata['columns']),
            pairs
        )
        return generate_d3m_dataset(join_, destination)
    elif 'union' in how:
        union_, qualities = union(
            convert_to_pd(left_data, left_metadata['columns']),
            convert_to_pd(right_data, right_metadata['columns']),
            pairs
        )
        return generate_d3m_dataset(union_, destination)
    else:
        raise RuntimeError('Augmentation task not recognized: %s.' % how)


def augment(es, data, metadata, task, destination=None):
    """
    Augments original data based on the task.

    :param es: Elasticsearch client.
    :param data: the data to be augmented.
    :param metadata: the metadata of the data to be augmented.
    :param task: the augmentation task.
    :param destination: location to save the files.
    """

    if 'id' not in task:
        raise RuntimeError('Dataset id for the augmentation task not provided.')

    if 'join_columns' in task and len(task['join_columns']) > 0:
        join_, qualities = join(
            convert_to_pd(data, metadata['columns']),
            materialize_dataset(es, task['id']),
            task['join_columns'],
            qualities=True
        )
        return generate_d3m_dataset(join_, destination, qualities)
    elif 'union_columns' in task and len(task['union_columns']) > 0:
        union_, qualities = union(
            convert_to_pd(data, metadata['columns']),
            materialize_dataset(es, task['id']),
            task['union_columns'],
            qualities=True
        )
        return generate_d3m_dataset(union_, destination, qualities)
    else:
        raise RuntimeError('Augmentation task not provided.')
