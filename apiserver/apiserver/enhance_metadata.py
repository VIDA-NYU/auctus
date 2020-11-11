from datamart_materialize.d3m import d3m_metadata


def enhance_metadata(result):
    """Add more metadata (e.g. D3M) from the original metadata.

    :param result: A dict with 'id' and 'metadata' keys
    :type result: dict
    :return: A dict with the 'metadata' key and additional keys such as
        'd3m-metadata'
    """
    # Generate metadata in D3M format
    result = dict(
        result,
        d3m_dataset_description=d3m_metadata(result['id'], result['metadata']),
    )

    # Add temporal coverage information to columns for compatibility
    if result['metadata'].get('temporal_coverage'):
        columns = list(result['metadata']['columns'])
        for temporal in result['metadata']['temporal_coverage']:
            # Only works for temporal coverage extracted from a single column
            if len(temporal['column_indexes']) == 1:
                idx = temporal['column_indexes'][0]
                columns[idx] = dict(
                    columns[idx],
                    coverage=temporal['ranges'],
                )
                if 'temporal_resolution' in temporal:
                    columns[idx]['temporal_resolution'] = \
                        temporal['temporal_resolution']

        result['metadata'] = dict(result['metadata'], columns=columns)

    return result
