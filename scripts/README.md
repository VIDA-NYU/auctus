Scripts
=======

This folder contains scripts that make it easy to perform certain tasks. Those scripts are meant to be run inside the Auctus environment; some of them are prefixed with `docker_`, those will run the corresponding script inside an Auctus container (provided the images have been built using the default docker-compose names).

* setup.sh: Run this once to setup your local checkout. This sets up the permissions on the volumes for docker-compose.
* docker_import_snapshot.sh: This downloads a dump of Elasticsearch from https://auctus.vida-nyu.org/snapshot/ and imports it using import_all.py
* docker_import_all.sh / import_all.py: This can be used to load a dump of Elasticsearch as JSON files. Useful to restore a backup
* import.py: Import a single dataset from a JSON file
* reprocess_all.py: This loads a dump of Elasticsearch as JSON files, but reprocesses the datasets
* freshen_old_index.py: This reprocesses datasets that were profiled by old versions
* docker_export_all.sh / export_all.py: This can be used to do a backup of the index. It creates a dump of Elasticsearch as JSON files
* docker-save_uploads.sh: This can be used to save the datasets that have been manually uploaded into Auctus (the data itself, not the indexed JSON documents)
* delete_dataset.py: Removes a single dataset from the index
* list_big_datasets.py: Lists the big datasets that have been indexed (by looking for the 'size' property above 50 MB)
* list_sources.py: This lists the number of datasets in the index per source (this is now shown on the index page of the coordinator as well)
* docker_purge_source.sh / purge_source.py: This removes all datasets from a given source
* clear_caches.py / docker_clear_caches.sh: This safely clears the caches
* upload_dataset.sh: This profiles and adds a dataset to the index
* report-uploads.sh: Alerts when datasets are uploaded to the system
* dataset_to_sup_index.py: This creates the supplementary column indices after 5507ab47
