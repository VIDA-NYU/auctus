Profiling library
=================

This library can be used to profile datasets standalone. You can use it to profile datasets on your side and send that to Auctus for search, instead of uploading the whole dataset. It is also used internally by Auctus to process search-by-example queries (when sending a file to the ``/search`` endpoint) and to add datasets to the index (to be queried against later).

Installing auctus-data-profiler
-------------------------------

You can get it directly from the Python Package Index using PIP::

    pip install auctus-data-profiler

API
---

The :py:func:`auctus_data_profiler.process_dataset` function is the entrypoint for the library. It returns a dict following Auctus's JSON result schema.

..  autofunction:: auctus_data_profiler.process_dataset
