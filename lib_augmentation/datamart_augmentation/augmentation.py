import logging
import os
import pandas as pd
import shutil
import tempfile
import time
import uuid

from datamart_core.common import Type
from datamart_materialize.d3m import D3mWriter


logger = logging.getLogger(__name__)


temporal_resolutions = [
    'second',
    'minute',
    'hour',
    'date'
]


temporal_resolution_format = {
    'second': '%Y-%m-%d %H:%M:%S',
    'minute': '%Y-%m-%d %H:%M',
    'hour': '%Y-%m-%d %H',
    'date': '%Y-%m-%d'
}


def convert_data_types(data, columns, columns_metadata):
    """
    Converts columns in a dataset (pandas.DataFrame) to their corresponding
    data types, based on the provided metadata.
    """

    data.set_index(
        [columns_metadata[column]['name'] for column in columns],
        drop=False,
        inplace=True
    )

    for i in range(len(columns)):
        index = columns[i]
        column = columns_metadata[index]
        name = column['name']
        if Type.DATE_TIME in column['semantic_types']:
            start = time.perf_counter()
            if isinstance(data.index, pd.MultiIndex):
                data.index = data.index.set_levels(
                    [data.index.levels[j] if j != i
                     else pd.to_datetime(data.index.levels[j], errors='coerce')
                     for j in range(len(data.index.levels))]
                )
            else:
                data.index = pd.to_datetime(data.index, errors='coerce')
            logger.info("Column %s converted to datetime in %.4fs" %
                        (name, (time.perf_counter() - start)))
        elif column['structural_type'] == Type.INTEGER:
            start = time.perf_counter()
            if isinstance(data.index, pd.MultiIndex):
                data.index = data.index.set_levels(
                    [data.index.levels[j] if j != i
                     else pd.to_numeric(data.index.levels[j], errors='coerce', downcast='integer')
                     for j in range(len(data.index.levels))]
                )
            else:
                data.index = pd.to_numeric(data.index, errors='coerce', downcast='integer')
            logger.info("Column %s converted to numeric (int) in %.4fs" %
                        (name, (time.perf_counter() - start)))
        elif column['structural_type'] == Type.FLOAT:
            start = time.perf_counter()
            if isinstance(data.index, pd.MultiIndex):
                data.index = data.index.set_levels(
                    [data.index.levels[j] if j != i
                     else pd.to_numeric(data.index.levels[j], errors='coerce', downcast='float')
                     for j in range(len(data.index.levels))]
                )
            else:
                data.index = pd.to_numeric(data.index, errors='coerce', downcast='float')
            logger.info("Column %s converted to numeric (float) in %.4fs" %
                        (name, (time.perf_counter() - start)))

    return data


def match_temporal_resolutions(input_data, companion_data):
    """Matches the resolutions between the datasets.
    """

    if isinstance(input_data.index, pd.MultiIndex):
        # TODO: support MultiIndex
        pass
    elif (isinstance(input_data.index, pd.DatetimeIndex)
          and isinstance(companion_data.index, pd.DatetimeIndex)):
        input_data.index, companion_data.index = \
            match_column_temporal_resolutions(input_data.index, companion_data.index)

    return input_data, companion_data


def match_column_temporal_resolutions(index_1, index_2):
    """Matches the resolutions between the dataset indices.
    """

    start = time.perf_counter()
    resolution_1 = check_temporal_resolution(index_1)
    resolution_2 = check_temporal_resolution(index_2)
    logger.info("Temporal resolutions checked for %s and %s in %.4fs" %
                (index_1.name, index_2.name, (time.perf_counter() - start)))
    if (temporal_resolutions.index(resolution_1) >
            temporal_resolutions.index(resolution_2)):
        start = time.perf_counter()
        index_name = index_2.name
        index_2 = \
            index_2.strftime(temporal_resolution_format[resolution_1])
        logger.info("Temporal resolution fixed for %s in %.4fs" %
                    (index_name, (time.perf_counter() - start)))
    else:
        start = time.perf_counter()
        index_name = index_1.name
        index_1 = \
            index_1.strftime(temporal_resolution_format[resolution_2])
        logger.info("Temporal resolution fixed for %s in %.4fs" %
                    (index_name, (time.perf_counter() - start)))

    return index_1, index_2


def check_temporal_resolution(data):
    """Returns the resolution of the temporal attribute.
    """

    if not data.is_all_dates:
        return None
    for res in temporal_resolutions[:-1]:
        if len(set([eval('x.%s' % res) for x in data[data.notnull()]])) > 1:
            return res
    return 'date'


def join(original_data, augment_data, left_columns, right_columns,
         columns=None, how='left', qualities=False,
         return_only_datamart_data=False):
    """
    Performs a join between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using left_columns and right_columns.

    Returns the new pandas.DataFrame object.
    """

    # remove undesirable columns from augment_data
    # but first, make sure to keep the join keys
    if columns:
        for right_column in right_columns:
            columns.append(right_column[0])
        columns = set([augment_data.columns[c] for c in columns])
        drop_columns = list(set(augment_data.columns).difference(columns))
        augment_data = augment_data.drop(drop_columns, axis=1)

    # matching temporal resolutions
    original_data, augment_data = \
        match_temporal_resolutions(original_data, augment_data)

    # join
    start = time.perf_counter()
    join_ = original_data.join(
        augment_data,
        how=how,
        rsuffix='_r'
    )
    logger.info("Join completed in %.4fs" % (time.perf_counter() - start))

    # TODO: work on aggregations
    if join_[join_.duplicated(original_data.columns)].shape[0] > 0:
        raise ValueError("After a successful join, "
                         "the shape of the data changed "
                         "(i.e., the number of records increased), "
                         "and therefore aggregations are required. DataMart "
                         "currently does not have support for aggregations.")

    # qualities
    qualities_list = list()

    if return_only_datamart_data:
        # dropping columns from original data
        drop_columns = list()
        intersection = set(original_data.columns).intersection(set(augment_data.columns))
        if len(intersection) > 0:
            drop_columns = list(intersection)
        drop_columns += list(set(original_data.columns).difference(intersection))
        join_ = join_.drop(drop_columns, axis=1)
        if len(intersection) > 0:
            rename = dict()
            for column in intersection:
                rename[column + '_r'] = column
            join_ = join_.rename(columns=rename)

        # dropping rows with all null values
        join_.dropna(axis=0, how='all', inplace=True)

    else:
        # removing duplicated join columns
        drop_join_columns = list()
        for i in range(len(right_columns)):
            name = augment_data.columns[right_columns[i][0]]
            if (augment_data.columns[right_columns[i][0]] ==
                    original_data.columns[left_columns[i][0]]):
                name += '_r'
            drop_join_columns.append(name)
        join_ = join_.drop(drop_join_columns, axis=1)
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

    # union
    start = time.perf_counter()
    union_ = pd.concat([original_data, augment_data])
    logger.info("Union completed in %.4fs" % (time.perf_counter() - start))

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
        raise ValueError("Dataset id for the augmentation task not provided")

    # TODO: add support for combining multiple columns before an augmentation
    #   e.g.: [['street number', 'street', 'city']] and [['address']]
    #   currently, DataMart does not support such cases
    #   this means that spatial joins (with GPS) are not supported for now

    # only converting data types for columns involved in augmentation
    aug_columns_input_data = []
    aug_columns_companion_data = []
    for i in range(len(task['augmentation']['left_columns'])):
        if (len(task['augmentation']['left_columns'][i]) > 1 or
                len(task['augmentation']['right_columns'][i]) > 1):
            raise ValueError("DataMart currently does not support "
                             "combination of columns for augmentation.")
        aug_columns_input_data.append(task['augmentation']['left_columns'][i][0])
        aug_columns_companion_data.append(task['augmentation']['right_columns'][i][0])

    try:
        if task['augmentation']['type'] == 'join':
            logger.info("Performing join...")
            join_, qualities = join(
                convert_data_types(
                    pd.read_csv(data, error_bad_lines=False),
                    aug_columns_input_data,
                    metadata['columns']
                ),
                convert_data_types(
                    pd.read_csv(newdata, error_bad_lines=False),
                    aug_columns_companion_data,
                    task['metadata']['columns']
                ),
                task['augmentation']['left_columns'],
                task['augmentation']['right_columns'],
                columns=columns,
                qualities=True,
                return_only_datamart_data=return_only_datamart_data
            )
            return generate_d3m_dataset(join_, destination, qualities)
        elif task['augmentation']['type'] == 'union':
            logger.info("Performing union...")
            union_, qualities = union(
                pd.read_csv(data, error_bad_lines=False),
                pd.read_csv(newdata, error_bad_lines=False),
                task['augmentation']['left_columns'],
                task['augmentation']['right_columns'],
                qualities=True
            )
            return generate_d3m_dataset(union_, destination, qualities)
        else:
            raise ValueError("Augmentation task not provided")
    except ValueError:
        raise
