import collections
import copy
import io
import itertools
import json
import logging
import numpy as np
import os
import pandas as pd
import tempfile
import time
import uuid

from datamart_materialize.d3m import d3m_metadata
from datamart_materialize import types


logger = logging.getLogger(__name__)


class AugmentationError(ValueError):
    """Error during augmentation.
    """


class _UniqueIndexKey(object):
    def __repr__(self):
        return "UNIQUE_INDEX_KEY"


UNIQUE_INDEX_KEY = _UniqueIndexKey()


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


def convert_data_types(data, columns, columns_metadata, drop=False):
    """
    Converts columns in a dataset (pandas.DataFrame) to their corresponding
    data types, based on the provided metadata.
    """

    data.set_index(
        [columns_metadata[column]['name'] for column in columns],
        drop=drop,
        inplace=True
    )

    for i, index in enumerate(columns):
        column = columns_metadata[index]
        if types.DATE_TIME in column['semantic_types']:
            if isinstance(data.index, pd.MultiIndex):
                data.index = data.index.set_levels(
                    [data.index.levels[j] if j != i
                     else pd.to_datetime(data.index.levels[j], errors='coerce')
                     for j in range(len(data.index.levels))]
                )
            else:
                data.index = pd.to_datetime(data.index, errors='coerce')
        elif column['structural_type'] == types.INTEGER:
            if isinstance(data.index, pd.MultiIndex):
                data.index = data.index.set_levels(
                    [data.index.levels[j] if j != i
                     else pd.to_numeric(data.index.levels[j], errors='coerce', downcast='integer')
                     for j in range(len(data.index.levels))]
                )
            else:
                data.index = pd.to_numeric(data.index, errors='coerce', downcast='integer')
        elif column['structural_type'] == types.FLOAT:
            if isinstance(data.index, pd.MultiIndex):
                data.index = data.index.set_levels(
                    [data.index.levels[j] if j != i
                     else pd.to_numeric(data.index.levels[j], errors='coerce', downcast='float')
                     for j in range(len(data.index.levels))]
                )
            else:
                data.index = pd.to_numeric(data.index, errors='coerce', downcast='float')

    return data


def match_temporal_resolutions(input_data, companion_data):
    """Matches the resolutions between the datasets.

    This takes in example indexes, and returns a function to update future
    indexes. This is because we are streaming, and want to decide once how to
    process multiple batches.
    """

    if isinstance(input_data.index, pd.MultiIndex):
        # TODO: support MultiIndex
        pass
    elif (isinstance(input_data.index, pd.DatetimeIndex)
          and isinstance(companion_data.index, pd.DatetimeIndex)):
        return match_column_temporal_resolutions(input_data.index, companion_data.index)

    return lambda input_idx, comp_idx: (input_idx, comp_idx)  # no-op


def match_column_temporal_resolutions(index_1, index_2):
    """Matches the resolutions between the dataset indices.
    """

    resolution_1 = check_temporal_resolution(index_1)
    resolution_2 = check_temporal_resolution(index_2)
    if (temporal_resolutions.index(resolution_1) >
            temporal_resolutions.index(resolution_2)):
        # Change resolution of second index to the first's
        fmt = temporal_resolution_format[resolution_1]
        return lambda idx1, idx2: (idx1, idx2.strftime(fmt))
    else:
        # Change resolution of first index to the second's
        fmt = temporal_resolution_format[resolution_2]
        _idx1 = index_1.strftime(fmt)  # Cache it for speed
        return lambda idx1, idx2: (_idx1, idx2)


def check_temporal_resolution(data):
    """Returns the resolution of the temporal attribute.
    """

    if not data.is_all_dates:
        return None
    for res in temporal_resolutions[:-1]:
        if len(set([eval('x.%s' % res) for x in data[data.notnull()]])) > 1:
            return res
    return 'date'


def perform_aggregations(data, original_columns):
    """Performs group by on dataset after join, to keep the shape of the
    new, augmented dataset the same as the original, input data.
    """

    col_indices = {
        col: idx for idx, col in enumerate(data.columns)
    }

    def first(series):
        return series.iloc[0]

    start = time.perf_counter()
    original_columns_set = set(original_columns)
    agg_functions = dict()
    for column in data.columns:
        if column == UNIQUE_INDEX_KEY or column in original_columns_set:
            # Just pick the first value
            # (they are all the same, from a single row in the original data)
            agg_functions[column] = [first]
        else:
            if ('int' in str(data.dtypes[column]) or
                    'float' in str(data.dtypes[column])):
                agg_functions[column] = collections.OrderedDict([
                    ('mean', np.mean), ('sum', np.sum),
                    ('max', np.max), ('min', np.min),
                ])
            else:
                # Just pick the first value
                agg_functions[column] = [first]

    # Perform group-by
    data = data.groupby(by=[UNIQUE_INDEX_KEY]).agg(agg_functions)

    # Drop group-by column
    data.reset_index(drop=True, inplace=True)

    # Reorder columns
    # sorted() is a stable sort, so we'll keep the order of agg_functions above
    data = data[sorted(
        data.columns,
        key=lambda col: col_indices.get(col[0], 999999999)
    )]

    # Rename columns
    data.columns = [
        col[0] if col[1] == 'first'
        else ' '.join(col[::-1]).strip()
        for col in data.columns
    ]

    logger.info("Aggregations completed in %.4fs", time.perf_counter() - start)
    return data


CHUNK_SIZE_ROWS = 10_000


def join(original_data, augment_data_path, original_metadata, augment_metadata,
         destination_csv,
         left_columns, right_columns,
         how='left', columns=None, return_only_datamart_data=False):
    """
    Performs a join between original_data (pandas.DataFrame)
    and augment_data (pandas.DataFrame) using left_columns and right_columns.

    Returns the new pandas.DataFrame object.
    """

    augment_data_columns = [col['name'] for col in augment_metadata['columns']]

    # only converting data types for columns involved in augmentation
    original_join_columns_idx = []
    augment_join_columns_idx = []
    for left, right in zip(left_columns, right_columns):
        if len(left) > 1 or len(right) > 1:
            raise AugmentationError("Datamart currently does not support "
                                    "combination of columns for augmentation.")
        original_join_columns_idx.append(left[0])
        augment_join_columns_idx.append(right[0])

    original_data = convert_data_types(
        original_data,
        original_join_columns_idx,
        original_metadata['columns'],
        drop=False,  # Keep the values of join columns from this side
    )

    # Add a column of unique indices which will be used to aggregate
    original_data[UNIQUE_INDEX_KEY] = pd.RangeIndex(len(original_data))

    logger.info("Performing join...")

    # join columns
    augment_join_columns = list()
    for left, right in zip(left_columns, right_columns):
        left_name = original_data.columns[left[0]]
        right_name = augment_data_columns[right[0]]
        if right_name == left_name:
            right_name += '_r'
        augment_join_columns.append(right_name)

    # Stream the data in
    augment_data_chunks = pd.read_csv(
        augment_data_path,
        error_bad_lines=False,
        chunksize=CHUNK_SIZE_ROWS,
    )
    first_augment_data = next(augment_data_chunks)

    # Columns to drop
    drop_columns = None
    if columns:
        drop_columns = list(
            # Drop all the columns in augment_data
            set(augment_data_columns[c] for c in columns)
            # except
            - (
                # the requested columns
                set(columns)
                # and the join columns
                | {col[0] for col in right_columns}
            )
        )

    # Guess temporal resolutions
    update_idx = match_temporal_resolutions(original_data, first_augment_data)

    # Streaming join
    start = time.perf_counter()
    join_ = []
    # Iterate over chunks of augment data
    for i, augment_data in enumerate(
            itertools.chain([first_augment_data], augment_data_chunks)
    ):
        # Convert data types
        augment_data = convert_data_types(
            augment_data,
            augment_join_columns_idx,
            augment_metadata['columns'],
            drop=True,  # Drop the join columns on that side (avoid duplicates)
        )

        # Match temporal resolutions
        original_data_res, augment_data = update_idx(
            original_data,
            augment_data,
        )

        # Filter columns
        if drop_columns:
            augment_data = augment_data.drop(drop_columns, axis=1)

        # Join
        joined_chunk = original_data_res.join(
            augment_data,
            how=how,
            rsuffix='_r'
        )

        # Drop the join columns we set as index
        joined_chunk.reset_index(drop=True, inplace=True)

        join_.append(joined_chunk)

    join_ = pd.concat(join_)
    logger.info("Join completed in %.4fs", time.perf_counter() - start)

    # qualities
    qualities_list = list()

    if return_only_datamart_data:
        # drop unique index
        join_.drop([UNIQUE_INDEX_KEY], axis=1, inplace=True)

        # drop columns from original data
        drop_columns = list()
        intersection = set(original_data.columns).intersection(set(first_augment_data.columns))
        if len(intersection) > 0:
            drop_columns = list(intersection)
        drop_columns += list(set(original_data.columns).difference(intersection))
        join_.drop(drop_columns, axis=1, inplace=True)
        if len(intersection) > 0:
            rename = dict()
            for column in intersection:
                rename[column + '_r'] = column
            join_.rename(columns=rename, inplace=True)

        # drop rows with all null values
        join_.dropna(axis=0, how='all', inplace=True)

    else:
        # aggregations
        join_ = perform_aggregations(
            join_,
            list(original_data.columns),
        )

        # drop unique index
        join_.drop([UNIQUE_INDEX_KEY], axis=1, inplace=True)

        original_columns_set = set(original_data.columns)
        new_columns = [
            col for col in join_.columns if col not in original_columns_set
        ]
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

    join_.to_csv(destination_csv, index=False)

    # Build a dict of information about all columns
    columns_metadata = dict()
    for column in original_metadata['columns']:
        columns_metadata[column['name']] = column
    for column in augment_metadata['columns']:
        names = [
            column['name'],
            column['name'] + '_r'
        ]
        # agg names
        all_names = ['sum ' + name for name in names]
        all_names += ['mean ' + name for name in names]
        all_names += ['max ' + name for name in names]
        all_names += ['min ' + name for name in names]
        all_names += ['first ' + name for name in names]
        all_names += names
        for name in all_names:
            column_metadata = copy.deepcopy(column)
            column_metadata['name'] = name
            if ('sum' in name or 'mean' in name
                    or 'max' in name or 'min' in name):
                column_metadata['structural_type'] = types.FLOAT
            columns_metadata[name] = column_metadata

    # Then construct column metadata by looking them up in the dict
    columns_metadata = [columns_metadata[name] for name in join_.columns]

    return {
        'columns': columns_metadata,
        'size': os.path.getsize(destination_csv),
        'qualities': qualities_list,
    }


def union(original_data, augment_data_path, original_metadata, augment_metadata,
          destination_csv,
          left_columns, right_columns,
          return_only_datamart_data=False):
    """
    Performs a union between original_data (pandas.DataFrame)
    and augment_data_path (path to CSV file) using columns.

    Returns the new pandas.DataFrame object.
    """

    augment_data_columns = [col['name'] for col in augment_metadata['columns']]

    logger.info(
        "Performing union, original_data: %r, augment_data: %r, "
        "left_columns: %r, right_columns: %r",
        original_data.columns, augment_data_columns,
        left_columns, right_columns,
    )

    # Column renaming
    rename = dict()
    for left, right in zip(left_columns, right_columns):
        rename[augment_data_columns[right[0]]] = original_data.columns[left[0]]

    # Missing columns will be created as NaN
    missing_columns = list(
        set(original_data.columns) - set(augment_data_columns)
    )

    # Sequential d3mIndex if needed, picking up from the last value
    # FIXME: Generated d3mIndex might collide with other splits?
    d3mIndex = None
    if 'd3mIndex' in original_data.columns:
        d3mIndex = int(original_data['d3mIndex'].max() + 1)

    logger.info("renaming: %r, missing_columns: %r", rename, missing_columns)

    # Streaming union
    start = time.perf_counter()
    with open(destination_csv, 'w', newline='') as fout:
        # Write original data
        fout.write(','.join(original_data.columns) + '\n')
        total_rows = 0
        if not return_only_datamart_data:
            original_data.to_csv(fout, index=False, header=False)
            total_rows += len(original_data)

        # Iterate on chunks of augment data
        augment_data_chunks = pd.read_csv(
            augment_data_path,
            error_bad_lines=False,
            chunksize=CHUNK_SIZE_ROWS,
        )
        for augment_data in augment_data_chunks:
            # Rename columns to match
            augment_data = augment_data.rename(columns=rename)

            # Add d3mIndex if needed
            if d3mIndex is not None:
                augment_data['d3mIndex'] = np.arange(
                    d3mIndex,
                    d3mIndex + len(augment_data),
                )
                d3mIndex += len(augment_data)

            # Add empty column for the missing ones
            for name in missing_columns:
                augment_data[name] = np.nan

            # Reorder columns
            augment_data = augment_data[original_data.columns]

            # Add to CSV output
            augment_data.to_csv(fout, index=False, header=False)
            total_rows += len(augment_data)
    logger.info("Union completed in %.4fs", time.perf_counter() - start)

    return {
        'columns': original_metadata['columns'],
        'size': os.path.getsize(destination_csv),
        'qualities': [dict(
            qualName='augmentation_info',
            qualValue=dict(
                new_columns=[],
                removed_columns=[],
                nb_rows_before=original_data.shape[0],
                nb_rows_after=total_rows,
                augmentation_type='union'
            ),
            qualValueType='dict'
        )],
    }


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

    # Prepare output D3M structure
    if destination is None:
        destination = tempfile.mkdtemp(prefix='datamart_aug_')
    os.mkdir(destination)
    os.mkdir(os.path.join(destination, 'tables'))
    destination_csv = os.path.join(destination, 'tables', 'learningData.csv')
    destination_metadata = os.path.join(destination, 'datasetDoc.json')

    # Perform augmentation
    start = time.perf_counter()
    if task['augmentation']['type'] == 'join':
        output_metadata = join(
            pd.read_csv(io.BytesIO(data), error_bad_lines=False),
            newdata,
            metadata,
            task['metadata'],
            destination_csv,
            task['augmentation']['left_columns'],
            task['augmentation']['right_columns'],
            columns=columns,
            return_only_datamart_data=return_only_datamart_data,
        )
    elif task['augmentation']['type'] == 'union':
        output_metadata = union(
            pd.read_csv(io.BytesIO(data), error_bad_lines=False),
            newdata,
            metadata,
            task['metadata'],
            destination_csv,
            task['augmentation']['left_columns'],
            task['augmentation']['right_columns'],
            return_only_datamart_data=return_only_datamart_data,
        )
    else:
        raise AugmentationError("Augmentation task not provided")
    logger.info("Total augmentation: %.4fs", time.perf_counter() - start)

    # Write out the D3M metadata
    d3m_meta = d3m_metadata(uuid.uuid4().hex, output_metadata)
    with open(destination_metadata, 'w') as fp:
        json.dump(d3m_meta, fp, sort_keys=True, indent=2)

    return destination
