Materialization library
=======================

This library can materialize datasets from Auctus. You can use it to materialize search results directly on your side without relying on the server. It is also used internally by Auctus to materialize datasets (the ``/download`` endpoint downloads the dataset using this library then sends it to you).

Installing datamart-materialize
-------------------------------

You can get it directly from the Python Package Index using PIP::

    pip install datamart-materialize

API
---

This library is organized around pluggable materializers, writers, and converters, which can be registered through Python's entrypoint mechanism.

If a dataset is provided to :func:`~datamart_materialize.download` that is not recognized or not installed, the library can use the server to do a "proxy" materialization, eg the server will perform the materialization from the original source and send it for us to write. Materializing a dataset from a simple ID rather than materialization information also requires contacting a server.

..  autofunction:: datamart_materialize.download

Materializers
`````````````

A materializer is an object that can take materialization information for a dataset (a JSON dictionary such as the one provided by Auctus under the ``materialize`` key) and can materialize it as a CSV file, for example by simply downloading it, by converting a different file to CSV, or possibly by doing multiple API calls to obtain all the rows.

Some datasets provided by Auctus contain a key ``materialize.direct_url``, in which case no materializer is needed, we download the CSV directly.

..  autoclass:: datamart_materialize.noaa.NoaaMaterializer

    Only a single materializer is included with ``datamart-materialize`` for ``noaa`` data. Downloading from the NOAA API requires numerous API calls that are slow and rate-limited; the JSON results can then be converted to a CSV. Use of the NOAA API requires a token that can be obtained from `NOAA's Climate Data Online: Web Services Documentation <https://www.ncdc.noaa.gov/cdo-web/webservices/v2>`__ and should be set as the environment variable ``NOAA_TOKEN`` for the materializer to work.

Writers
```````

..  autoclass:: datamart_materialize.CsvWriter

..  autoclass:: datamart_materialize.PandasWriter

..  autoclass:: datamart_materialize.d3m.D3mWriter

Converters
``````````

..  autoclass:: datamart_materialize.excel.ExcelConverter
