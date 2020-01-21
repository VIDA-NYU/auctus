0.6 (TBD 2020-01-??)
====================

Incompatible changes:
* Updated Elasticsearch from 6 to 7. You should recreate your index from
scratch, or by importing the JSON files.
* Fix semantic type for enumeration (`http:` not `https:`)
* `docker-compose.yml` now uses local mounts for volumes (create them with `scripts/setup.sh`)
* Aggregation now always happens, even if there are no duplicate rows (so you'll always get the min/max/mean columns)
* Aggregated columns renamed (`amin` -> `min`, `amax` -> `max`)
* Renamed `lazo_server` container to `lazo`
* Updated to MIT-LL D3M schema version 4.0.0

Enhancements:
* Compute additional metadata `missing_values_ratio`, `unclean_values_ratio`, `num_distinct_values`
* Don't drop any column from supplied data during union
* Checked-in Grafana settings for anonymous access, SMTP, and the dashboards we use (in `contrib/grafana-dashboards`)
* Improvements to caching code
* Use a fixed seed for sampling in profiling (makes it deterministic)
* Store a sample of each dataset, show it in search result API (and dataset page)
* Don't drop non-numeric columns in aggregation
* Clear old entries from caches when a limit is reached (`MAX_CACHE_BYTES`)
* Datasets too big to be joined will no longer be profiled into the index
* Add 'temporal' and 'spatial' badges on datasets
* Fix aggregation breaking with missing data in join column
* Added Kubernetes config (`contrib/k8s/`)
* Don't ignore Lazo errors on profiling (you will now see errors if using Lazo and it's not responding). Have it re-try on Elasticsearch errors

0.5 (2019-08-28)
================

Re-submission at DARPA's request, to add support for categorical data. Used for evaluation.

Incompatible changes:
* Added a `lazo_server` component
* `/metadata/` endpoint now puts metadata under `metadata` key

Enhancements:
* Docker containers should now shut down properly
* Fix a problem with the type identification thresholds
* Add caching
* Index text data using Lazo
* Return metadata in D3M Dataset format under `d3m_dataset_description` key

0.4 (2019-07-19)
================

Final version for the Summer workshop, submitted for evaluation.
