Datamart materialization library
================================

This library can materialize datasets from Auctus, NYU's dataset search engine. You can use it to materialize search results directly on your side without relying on the server. It is also used internally by the service to materialize datasets (the ``/download`` endpoint downloads the dataset using this library then sends it to you).

See also:

* `The datamart-rest library for search/augmentation <https://pypi.org/project/datamart-rest/>`__
* `The datamart-profiler library, used to profile datasets for search <https://pypi.org/project/datamart-profiler/>`__
* `The datamart-augmentation library, used to performs data augmentation with a dataset from Auctus <https://pypi.org/project/datamart-augmentation/>`__
* `Auctus, NYU's dataset search engine <https://auctus.vida-nyu.org/>`__
* `Our project on GitLab <https://gitlab.com/ViDA-NYU/auctus/auctus>`__
