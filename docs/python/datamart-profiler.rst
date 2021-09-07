Profiling library
=================

This library can be used to profile datasets standalone. You can use it to profile datasets on your side and send that to Auctus for search, instead of uploading the whole dataset. It is also used internally by Auctus to process search-by-example queries (when sending a file to the ``/search`` endpoint) and to add datasets to the index (to be queried against later).

Installing datamart-profiler
----------------------------

You can get it directly from the Python Package Index using PIP::

    pip install datamart-profiler

API
---

The :py:func:`datamart_profiler.process_dataset` function is the entrypoint for the library. It returns a dict following Auctus's JSON result schema.

..  autofunction:: datamart_profiler.core.process_dataset

..  autofunction:: datamart_profiler.temporal.parse_date

..  autofunction:: datamart_profiler.core.count_rows_to_skip

Command-line usage
------------------

You can also use datamart-profiler from the command-line like so::

    $ python -m datamart_profiler <file.csv>

It will output the extracted metadata as JSON.
