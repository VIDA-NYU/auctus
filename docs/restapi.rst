Using the REST API
==================

You can access Datamart through a REST API. In addition to the documentation below, you can find a `Swagger UI <swagger/index.html>`__ which can be used to try the API.

There is also a :doc:`Python client library <python/datamart-rest>` for this API.

..  _rest-search:

``POST /search``
----------------

Queries the DataMart system for datasets.

The ``Content-Type`` should be set to `multipart/form-data <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition>`__ to allow sending both the query description and the data file.

The following keys are accepted in the request body (you need to specify at least one of them):

* ``data``: path to a D3M dataset **OR** path to a ``csv`` file **OR** ``csv`` file contents
* ``query``: JSON object representing the query, according to :ref:`the query API specification <schema-query>`

This endpoint returns a JSON object, according to :ref:`the query results specification <schema-result>`.

..  _rest-download:

``POST /download``
------------------

Downloads a dataset from DataMart.

The ``Content-Type`` should be set to `multipart/form-data <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition>`__.

The following keys are accepted in the request body:

* ``data``: path to a D3M dataset **OR** path to a ``csv`` file **OR** ``csv`` file contents
* ``task``: a JSON object that represents a query result, according to :ref:`the query results specification <schema-result>`
* ``format``: indicates the format of the returned file

If ``data`` is supplied, DataMart will return a dataset that augments well with ``data``, i.e., it will only return the portions of the dataset referenced by ``id`` that matches well with ``data``.

The options for ``format`` are:

* ``"csv"``: returns the dataset as a ``csv`` file (``application/octet-stream``); this is the default option
* ``"d3m"``: returns a ``zip`` file (``application/zip``) containing the dataset as a ``csv`` file and its corresponding ``datasetDoc.json`` file

The structure for the ``zip`` file follows the D3M format:

..  code::

    dataset.zip
    +-- datasetDoc.json
    +-- tables
        +-- learningData.csv

..  _rest-download-get:

``GET /download/id``
--------------------

Downloads a dataset from DataMart, where id is the dataset identifier. It also accepts one query parameter, ``format``, as specified above.

..  _rest-augment:

``POST /augment``
-----------------

Augments a dataset.

The ``Content-Type`` should be set to `multipart/form-data <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition>`__.

The accepted key/value pairs in the request body are the following:

* ``data``: path to a D3M dataset **OR** path to a ``csv`` file **OR** ``csv`` file contents
* ``task``: a JSON object that represents a query result, according to :ref:`the query results specification <schema-result>`
* ``columns``: a list of column indices from the DataMart dataset that will be added to ``data`` (optional)
* ``destination``: the location in disk where the new data will be saved (optional). Note that DataMart must have access to this path.

The function returns one of the following:

* a zip file (``application/zip``) containing the dataset as a ``csv`` file and its corresponding ``datasetDoc.json`` file, if the parameter ``destination`` is not defined
* the path (``text/plain``) to the directory containing the dataset as a ``csv`` file and its corresponding ``datasetDoc.json`` file, if the parameter ``destination`` is defined.

The structure for the ``zip`` file follows the D3M format explained before for the :ref:`download endpoint <rest-download>`.

``POST /upload``
----------------

Adds a dataset to the index. The file can be provided either via a URL or direct upload.

When providing a URL, make sure it is a direct link to a file in a supported format (CSV, Excel, SPSS, ...) and not to an HTML page with a "download" button or GitHub page where the content is embedded (use the "raw" button).

The request will return the ID of the new dataset immediately, but profiling will happen in the background so the file will only appear in searches after a couple minutes::

    {"id": "datamart.upload.abcdef1234567890"}
