import copy
import io
import logging
import numpy as np
import os
import pandas as pd
import tempfile
import time
import uuid

from datamart_materialize.d3m import D3mWriter
from datamart_materialize import types


logger = logging.getLogger(__name__)


class AugmentationError(ValueError):
    """Error during augmentation.
    """


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
        if types.DATE_TIME in column['semantic_types']:
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
        elif column['structural_type'] == types.INTEGER:
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
        elif column['structural_type'] == types.FLOAT:
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


def perform_aggregations(data, groupby_columns,
                         original_data_join_columns,
                         augment_data_join_columns):
    """Performs group by on dataset after join, to keep the shape of the
    new, augmented dataset the same as the original, input data.
    """

    if data[data.duplicated(groupby_columns)].shape[0] > 0:
        start = time.perf_counter()
        agg_columns = list(
            set(data.columns).difference(
                set(groupby_columns))
        )
        agg_functions = dict()
        for column in agg_columns:
            if column not in augment_data_join_columns:
                # column is not a join column
                if ('int' in str(data.dtypes[column]) or
                        'float' in str(data.dtypes[column])):
                    agg_functions[column] = [
                        np.mean, np.sum, np.max, np.min
                    ]
            else:
                # column is a join column
                if 'datetime' in str(data.dtypes[column]):
                    # TODO: handle datetime
                    pass
                else:
                    # getting the first non-null element
                    # since it is a join column, we expect all the values
                    # to be exactly the same
                    agg_functions[column] = [lambda x: x.iloc[0]]
                    # agg_functions[column] = \
                    #     lambda x: x.loc[x.first_valid_index()].iloc[0]
        if not agg_functions:
            raise AugmentationError("No numerical columns to perform aggregation.")
        data.index.name = None  # avoiding warnings
        data = data.groupby(by=groupby_columns).agg(agg_functions)
        data = data.reset_index(drop=False)
        data.columns = [' '.join(col[::-1]).strip()
                        # keep same name for join column
                        if col[0] not in (original_data_join_columns +
                                          augment_data_join_columns)
                        else col[0].strip()
                        for col in data.columns.values]
        logger.info("Aggregations completed in %.4fs" % (time.perf_counter() - start))
    return data


def join(original_data, augment_data, left_columns, right_columns,
         columns=None, how='left', qualities=False,
         return_only_datamart_data=False):
    """
    Performs a join between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using left_columns and right_columns.

    Returns the new pandas.DataFrame object.
    """

    # join columns
    original_join_columns = list()
    augment_join_columns = list()
    for i in range(len(right_columns)):
        name = augment_data.columns[right_columns[i][0]]
        if (augment_data.columns[right_columns[i][0]] ==
                original_data.columns[left_columns[i][0]]):
            name += '_r'
        augment_join_columns.append(name)
        original_join_columns.append(original_data.columns[left_columns[i][0]])

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
        # aggregations
        join_ = perform_aggregations(
            join_,
            list(original_data.columns),
            original_join_columns,
            augment_join_columns
        )

        # removing duplicated join columns
        join_ = join_.drop(
            list(set(augment_join_columns).intersection(set(join_.columns))),
            axis=1
        )

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
    for left, right in zip(left_columns, right_columns):
        rename[augment_data.columns[right[0]]] = original_data.columns[left[0]]
    augment_data = augment_data.rename(columns=rename)

    # union
    start = time.perf_counter()
    union_ = pd.concat([original_data, augment_data], sort=False)
    logger.info("Union completed in %.4fs" % (time.perf_counter() - start))

    # special treatment for 'd3mIndex' column
    if 'd3mIndex' in union_.columns:
        filler = [i for i in range(int(union_['d3mIndex'].max() + 1), union_.shape[0])]
        union_.loc[union_['d3mIndex'].isnull(), 'd3mIndex'] = filler
        union_['d3mIndex'] = pd.to_numeric(union_['d3mIndex'], downcast='integer')

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


def generate_d3m_dataset(data, input_metadata, companion_metadata,
                         destination=None, qualities=None):
    """
    Generates a D3M dataset from data (pandas.DataFrame).

    Returns the path to the D3M-style directory.
    """

    if destination is None:
        destination = os.path.join(
            tempfile.mkdtemp(prefix='datamart_aug_'),
            'dataset',
        )

    # collecting information about all the original columns
    # from input (supplied) and companion datasets
    original_columns_metadata = dict()
    for column in input_metadata:
        original_columns_metadata[column['name']] = column
    for column in companion_metadata:
        names = [
            column['name'],
            column['name'] + '_r'
        ]
        # agg names
        all_names = ['sum ' + name for name in names]
        all_names += ['mean ' + name for name in names]
        all_names += ['amax ' + name for name in names]
        all_names += ['amin ' + name for name in names]
        all_names += names
        for name in all_names:
            column_metadata = copy.deepcopy(column)
            column_metadata['name'] = name
            if ('sum' in name or 'mean' in name
                    or 'amax' in name or 'amin' in name):
                column_metadata['structural_type'] = types.FLOAT
            original_columns_metadata[name] = column_metadata

    # column metadata for the new, augmented dataset
    columns_metadata = list()
    for column_name in data.columns:
        columns_metadata.append(
            original_columns_metadata[column_name]
        )

    metadata = dict(columns=columns_metadata)
    metadata['size'] = data.memory_usage(index=True, deep=True).sum()

    if qualities:
        metadata['qualities'] = qualities

    writer = D3mWriter(uuid.uuid4().hex, destination, metadata)
    with writer.open_file('w') as fp:
        data.to_csv(fp, index=False)

    return destination


def augment(data, newdata, metadata, task, columns=None, destination=None,
            return_only_datamart_data=False):
    """
    Augments original data based on the task.

    :param data: the data to be augmented, as bytes.
    :param newdata: the path to the CSV file to augment with.
    :param metadata: the metadata of the data to be augmented.
    :param task: the augmentation task.
    :param columns: a list of column indices from newdata that will be added to data
    :param destination: location to save the files.
    :param return_only_datamart_data: only returns the portion of newdata that matches
      well with data.
    """

    if 'id' not in task:
        raise AugmentationError("Dataset id for the augmentation task not provided")

    # TODO: add support for combining multiple columns before an augmentation
    #   e.g.: [['street number', 'street', 'city']] and [['address']]
    #   currently, Datamart does not support such cases
    #   this means that spatial joins (with GPS) are not supported for now

    # only converting data types for columns involved in augmentation
    aug_columns_input_data = []
    aug_columns_companion_data = []
    for left_columns, right_columns in zip(
                task['augmentation']['left_columns'],
                task['augmentation']['right_columns'],
            ):
        if len(left_columns) > 1 or len(right_columns) > 1:
            raise AugmentationError("Datamart currently does not support "
                                    "combination of columns for augmentation.")
        aug_columns_input_data.append(left_columns[0])
        aug_columns_companion_data.append(right_columns[0])

    if task['augmentation']['type'] == 'join':
        logger.info("Performing join...")
        result, qualities = join(
            convert_data_types(
                pd.read_csv(io.BytesIO(data), error_bad_lines=False),
                aug_columns_input_data,
                metadata['columns'],
            ),
            convert_data_types(
                pd.read_csv(newdata, error_bad_lines=False),
                aug_columns_companion_data,
                task['metadata']['columns'],
            ),
            task['augmentation']['left_columns'],
            task['augmentation']['right_columns'],
            columns=columns,
            qualities=True,
            return_only_datamart_data=return_only_datamart_data,
        )
    elif task['augmentation']['type'] == 'union':
        logger.info("Performing union...")
        result, qualities = union(
            pd.read_csv(io.BytesIO(data), error_bad_lines=False),
            pd.read_csv(newdata, error_bad_lines=False),
            task['augmentation']['left_columns'],
            task['augmentation']['right_columns'],
            qualities=True,
        )
    else:
        raise AugmentationError("Augmentation task not provided")

    return generate_d3m_dataset(
        result,
        metadata['columns'],
        task['metadata']['columns'],
        destination,
        qualities,
    )
