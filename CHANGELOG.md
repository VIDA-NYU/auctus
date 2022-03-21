0.10 (2022-03-21)
=================

Incompatible changes:
* Discovery scripts are now batch processes instead of services, e.g. they exit on completion instead of waiting for the next day (Kubernetes configs were updated to CronJob)
* Rewrote Kubernetes config using jsonnet, for easier customization

Enhancements:
* Added OpenTelemetry integration
* Add snapshotter service, dumping Elasticsearch data as a tar.gz of JSON files, and putting it in object store to be serve by API server at `/snapshot/`
* Use a single Docker image for all services (not including databases, Lazo, frontend)
* Add option not to profile the pandas DataFrame's index
* Removed volume and job for getting the list of synonyms for Elasticsearch: just do it at startup of every container
* Removed volume and job for getting the datamart-geo data: put them in the Docker image instead. This makes for a big image, but allows deduplication between multiple Auctus instances and is simpler to setup
* Add ability to recognize and convert/profile Parquet files

Bugfixes:
* Added SSRF prevention code. Unless you are running other services alongside Auctus, the only thing that could be hit is Elasticsearch, whose JSON output would not be leaked, but let's plug this anyway
* Set Grafana data source proxy whitelist, to prevent SSRF through Grafana
* Fix docker_import_snapshot.sh syntax error
* Fix import_all.py not quiting on repeated error, making it look like it's making progress when it is not
* Don't have haproxy listen on a privileged port, fixes issues on Docker for Mac

0.9 (2021-04-28)
================

Incompatible changes:
* Socrata and Zenodo configuration is now from JSON files mounted in the countainers
* Changed format of temporal coverage information: now in separate key `temporal_coverage` instead of column, similar to `spatial_coverage`
* Added a World Bank discoverer, for their indicators
* Renamed repository to 'auctus'
* Updated Poetry from 1.0 to 1.1 (lock files are incompatible)
* Moved state for discoverers to a separate ES index 'discovery'
* Changed ES synonym configuration to `synonym_graph` filter
* Moved cleaning file cache to a dedicated container (to run as DaemonSet on Kubernetes)
* Use an object storage service to store uploaded datasets (S3-compatible or GCS). You can use Minio to run locally

Enhancements:
* Added a maintenance interface to the coordinator container, allowing the operator to view recently uploaded datasets and remove datasets, view errored datasets per exception type, and reprocess datasets
* Added pagination to search API (non-augmentation only for now)
* Assume admin areas have the same level, record the level to metadata
* Strip HTML from dataset descriptions
* Automatically pivot tables with years or dates for columns
* Added pagination to the frontend (in first 150 API results for now)
* Searching for related files will now ask for either join or union (and which columns)
* Added a 'source_url' field to metadata, linked from interface
* Return facets in API (under key 'facets') and total number of results (under key `total`)
* Show count of results for each facet in interface
* Lowered profiled sample size dramatically (from max 50MB to max 5MB)
* Use openpyxl to read Excel 2003+ files, as xlrd is no longer supporting them
* Make random sampling exact, making sure that the right number of rows is selected even with very small selection ratios
* Added command-line support to lib_profiler
* Implemented new spatial sketch based on geohash, display as heatmap in the interface
* Recognize when columns consist of filenames or URLs
* Use a prefix for Elasticsearch indexes names, allowing to run multiple instances against one Elasticsearch cluster
* Added a discovery plugin for CKAN, which allows getting all data from a CKAN repository (or those which match a keyword query)
* Added logging to JSON format

Bugfixes:
* Expose Content-Disposition header via CORS, allowing browser to read downloaded file names
* Various fixes to API spec, now checked during integration tests
* Improve sniffing CSV/TSV file formats
* Fix profiler not recognizing real numbers when they include negative exponents e.g. `1.23e-04`

0.8 (2020-10-20)
================

Incompatible changes:
* Moved API endpoints under /api/v1. This will make local deployments match auctus.vida-nyu.org.
* Updated spatial coverage format, files need to get re-profiled (or re-create ES index and use migrate-spatial-coverage.py script)
* Removed "half-augmentation" flow (download the subset of a dataset that matches some input data), existed for feature-parity with ISI system, but unused
* Bind ports to localhost in docker-compose file
* Updated RabbitMQ from 3.7 to 3.8, changes metrics location
* datamart-geo needs a data volume, populate it with `python -m datamart_geo --update lib_geo/data/`
* Synonym filter for Elasticsearch will only work if you re-configure the index
* Changed default port numbers if using docker-compose, update your reverse-proxy if you're exposing RabbitMQ/HAproxy/Prometheus/Grafana

Enhancements:
* Added a search box on the map for the spatial filter
* Added search by "dataset type", e.g. numerical/categorical/spatial/temporal, computed from the sum of column types
* Let user select the temporal resolution of the join
* Compute keywords from the attribute names (dealing with punctuation, camel case, ...)
* Allow selecting which column of the input data to use in the search for possible joins
* Added ability to download the dataset sample as CSV
* Integrated with Sentry to track errors during queries and profiling, made the system more reliable by handling more edge cases
* Added support for delimited files with a different delimited than comma or tab (for example, semicolon)
* Show a spinner while profiling related data, prevent submitting the search before it's done
* Automatically handle Stata files
* Choose between joins and unions when searching with a related file
* New version of datamart-geo with more data
* Configured a synonym filter in Elasticsearch
* Store more information about indexing failure in the 'pending' index for easy retrieval

Bugfixes:
* Fix join result metadata, it previously contained some information relating to the dataset before join
* Retry Lazo calls a second time to try and limit the number of `_InactiveRpcError`

0.7 (2020-08-04)
================

Incompatible changes:
* Add a Zenodo discovery plugin (can be configured with a keyword)
* Renamed docker-compose services to use dashes `-` instead of underscores `_`
* Containers no longer run as root, change permissions on volumes to uid 998
* Changed Elasticsearch index settings, use `scripts/es_reindex.sh` to update
* Changed Redis cache from Pickle to JSON, make sure to clear Redis on update

Enhancements:
* Remove latitude/longitude semantic types from unmatched columns
* Recognize "Well-Known Text" (WKT) and "Socrata combined" point formats
* Send batch queries to Nominatim, retry smaller batches on 500
* While a profiler is processing, have it download the next dataset in parallel
* Automatically handle TSV files
* Automatically pivot files with columns for years
* Add a new alternate index "pending" for non-profiled datasets (uploaded and waiting, or failed)
* Add discovery plugin for indicators from University of Arizona
* Recognize columns called "year" as temporal
* Add plot for text columns (with most common words)
* Aggregate search hits in frontend (same dataset with different augmentations)
* Remove clusters of outliers when computing ranges
* Show plots on frontend again
* Recognize datetime strings in YYYYMMDD format (even though they are valid integers)
* Fix searching on column names
* Detect named administrative areas via the datamart-geo database
* Accept format parameters for augmentations (don't only return D3M format)
* Automatically handle SPSS files
* Convert search input the same as discovered datasets (so searching is possible from a TSV, Excel, or SPSS file)
* Serve files with the correct extension
* Filter results by temporal granularity
* Cache data sent to `/profile`, to allow augmentations from it without reupload
* Update URL in frontend to reflect current query
* Add a "TA3 API" allowing systems to use our search frontend as part of their workflow, and collect results directly from Datamart afterwards
* Add "custom fields" which can be added to the upload form via configuration
* Allow the user to preview the profiled information on the upload form, and manually override column metadata
* Have lib_augmentation accept file objects
* Implement spatial joins using a KDTree to align the right dataset to the nearest point of the left dataset
* Implement joins on multiple columns (e.g. spatio-temporal)
* Re-license Datamart under Apache-2.0
* Allow searching by named spatial area in API
* Decode dates in Excel files, from the floating-point number of days format

0.6 (2020-04-29)
================

Incompatible changes:
* Updated Elasticsearch from 6 to 7. You should recreate your index from
scratch, or by importing the JSON files.
* Fix semantic type for enumeration (`http:` not `https:`)
* `docker-compose.yml` now uses local mounts for volumes (create them with `scripts/setup.sh`)
* Aggregation now always happens, even if there are no duplicate rows (so you'll always get the min/max/mean columns)
* Aggregated columns renamed (`amin` -> `min`, `amax` -> `max`)
* Renamed `lazo_server` container to `lazo`
* Updated to MIT-LL D3M schema version 4.0.0
* Added 'source' key to schema, you will have to manually add this to the indexed documents (or just re-profile)
* Added a Redis server, used to store profiling information
* Profiling now needs a Nominatim server, to resolve addresses
* New frontend, served as static JavaScript app that uses the API directly, in new container `frontend`
* Renamed `query` container to `apiserver`
* Updated Lazo to transform strings to lowercase (index needs to be recreated)

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
* Correctly deal with empty datasets
* Move additional endpoints to REST-JSON and the API container (upload, statistics)
* Improved logging (quiet Elasticsearch)
* Get lat/long from addresses to include in spatial coverage
* Add option to generate d3mIndex column, to use dataset as ML input
* Allow ability to refer to already-profiled input data using a token instead of re-uploading
* Many improvements to spatial profiling, added new aggregation resolutions (weekly, monthly, yearly)
* Profiler detects and indexes temporal resolution of datetime columns
* Can search for datasets related to another dataset in the index (by sending only its ID)
* Add selection of aggregation functions to the API (`agg_functions` list in `augmentation` dict) and frontend

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
