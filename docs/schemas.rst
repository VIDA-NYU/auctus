JSON Schemas
============

..  _schema-query:

Query
-----

JSON objects expected by the :ref:`rest-search` endpoint.

..  literalinclude:: schemas/query_input_schema.json
    :language: json
    :linenos:

..  _schema-result:

Result schema
-------------

Description of a dataset, such as a search result. The :ref:`rest-search` endpoint returns an array of those. They are also what you give the :func:`auctus_materialize.download`.

..  literalinclude:: schemas/query_result_schema.json
    :language: json
    :linenos:
