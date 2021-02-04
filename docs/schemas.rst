JSON Schemas
============

..  _schema-query:

Query
-----

JSON objects expected by `the search endpoint <../rest/#operation/search>`__.

..  literalinclude:: schemas/query_input_schema.json
    :language: json
    :linenos:

..  _schema-result:

Result schema
-------------

Description of a dataset, such as a search result. `The search endpoint <../rest/#operation/search>`__ returns an array of those. They are also what you give the :func:`datamart_materialize.download`.

..  literalinclude:: schemas/query_result_schema.json
    :language: json
    :linenos:
