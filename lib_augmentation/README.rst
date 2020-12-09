Auctus augmentation library
===========================

This library performs data augmentation between datasets from Auctus, the dataset search engine from NYU. You can use it to augment a dataset with a search result directly on your side without relying on the server. It is also used internally by the service to perform augmentations (the ``/augment`` endpoint downloads the dataset using this library, performs augmentation, then sends the result to you).

See also:

* `The datamart-rest library for search/augmentation <https://pypi.org/project/datamart-rest/>`__
* `The auctus-data-profiler library, used to profile datasets for search <https://pypi.org/project/auctus-data-profiler/>`__
* `The auctus-materialize library, used to materialize dataset from search results <https://pypi.org/project/auctus-materialize/>`__
* `Auctus, the dataset search engine from NYU <https://auctus.vida-nyu.org/>`__
* `Our project on GitLab <https://gitlab.com/ViDA-NYU/auctus/auctus>`__
