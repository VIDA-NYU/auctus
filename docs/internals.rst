Internals
=========

Architecture
------------

Datamart is a cloud-native application divided in multiple components (containers) which can be scaled independently.

..  figure:: architecture.png
    :align: center

    Overall architecture of Datamart

At the core of Datamart is an `Elasticsearch cluster <https://www.elastic.co/>`__ which stores sketches of the datasets, obtained by downloading and profiling them.

The system is design to be extensible. Plugins called ":ref:`discoverers`" can be added to support additional data sources.

The different components communicate via the AMQP message-queueing protocol through a `RabbitMQ server <https://www.rabbitmq.com/>`__, allowing the discoverers to queue datasets to be downloaded and profiled when profilers are available, while updating waiting clients about relevant new discoveries.

..  figure:: amqp.png
    :align: center
    :width: 25em

    AMQP queues and exchanges used by Datamart

..  _discoverers:

Discoverers
-----------

A discoverer is responsible for finding data. It runs as its own container, and can either:

* announce all datasets to AMQP when they appear in the source, to be profiled **in advance** of user queries, or
* react to user queries, use it to perform a search in the source, and announce the datasets found, **on-demand**.

Either way, a base Python class is provided as part of :mod:`datamart_core` that can easily be extended instead of re-implementing the AMQP setup.

..  autoclass:: datamart_core.discovery.Discoverer

    ..  py:method:: main_loop()

        Optional entrypoint of the discoverer.

        Not necessary if the discoverer is only reacting to queries.

    ..  py:method:: handle_query(query, publisher)

        Optional hook to react to user queries and find datasets on-demand.

        Not necessary if the discoverer is only discovering datasets ahead-of-time.

        ..  py:function:: publisher(materialize, metadata, dataset_id=None)

            Callable object used to publish datasets found for that query. They will be profiled if necessary and recorded in the index, as well as considered for the results of the user's query.

            :param dict materialize: Materialization information for that dataset
            :param dict metadata: Metadata for that dataset, that might be augmented with profiled information
            :param str dataset_id: Dataset id. If unspecified, a UUID4 will be generated for it.

    ..  automethod:: datamart_core.discovery.Discoverer.record_dataset

    ..  automethod:: datamart_core.discovery.Discoverer.write_to_shared_storage

    ..  automethod:: datamart_core.discovery.Discoverer.delete_dataset

..  autoclass:: datamart_core.discovery.AsyncDiscoverer
