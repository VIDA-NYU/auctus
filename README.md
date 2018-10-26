DataMart
========

This project is designed to be a web crawler and search engine for dataset, specifically meant for data augmentation tasks in machine learning. Its goal is to be able to find datasets in different repositories and index them for later retrieval.

It is divided in multiple services:

* **Discovery plugins**: those are responsible for discovering datasets and downloading them. Each plugin can talk to a specific repository. **Materialization metadata** can be recorded with a dataset, to allow future retrieval of that dataset.
  * `datamart_core` contains the base interfaces for discovery plugins, `Discoverer` and `AsyncDiscoverer`.
* **Profiler**: this service goes over a dataset to compute additional metadata that can be used during searches (for example, dimensions, semantic types, value distributions)
  * `datamart_profiler` contains the implementation, which is currently just a stub
* **Index**: an Elasticsearch cluster stores the metadata about all known datasets, as provided by the discovery plugins and the profiler.
* **Query**: this service responds to queries from clients by looking up datasets in the index, and can trigger on-demand query by discovery plugins that support it. It also requests the materialization of the datasets selected by the client for download.
  * `datamart_query` contains the implementation, that provies a JSON API using the Tornado web framework
* The **coordinator**: this service is in charge of the dataset cache, where discovery plugins download datasets, and that is read by the profiler and query services. It also provides the monitoring facilities, showing a live feed of indexed datasets.
  * `datamart_coordinator` contains the implementation, based on the Tornado web framework
* All those components exchange messages through `RabbitMQ`, allowing us to have complex messaging patterns with queueing and retrying semantics, and complex patterns such as the on-demand querying

![DataMart Architecture](architecture.png)
