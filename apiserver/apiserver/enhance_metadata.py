from datamart_materialize.d3m import d3m_metadata


def enhance_metadata(result):
    """Add more metadata (e.g. D3M) from the original metadata.

    :param result: A dict with 'id' and 'metadata' keys
    :type result: dict
    :return: A dict with the 'metadata' key and additional keys such as
        'd3m-metadata'
    """
    result = dict(
        result,
        d3m_dataset_description=d3m_metadata(result['id'], result['metadata']),
    )
    return result
