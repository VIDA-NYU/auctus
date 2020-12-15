Datamart profiling library
==========================

This library can profile datasets for use with Auctus, NYU's dataset search engine. You can use it to profile datasets on your side and send that to the server for search, instead of uploading the whole dataset. It is also used internally by the service to process search-by-example queries (when sending a file to the ``/search`` endpoint) and to add datasets to the index (to be queried against later).

See also:

* `The datamart-rest library for search/augmentation <https://pypi.org/project/datamart-rest/>`__
* `The datamart-materialize library, used to materialize dataset from search results <https://pypi.org/project/datamart-materialize/>`__
* `The datamart-augmentation library, used to performs data augmentation with a dataset from Auctus <https://pypi.org/project/datamart-augmentation/>`__
* `Auctus, NYU's dataset search engine <https://auctus.vida-nyu.org/>`__
* `Our project on GitLab <https://gitlab.com/ViDA-NYU/auctus/auctus>`__
