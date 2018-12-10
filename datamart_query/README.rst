DataMart Query Service
======================

This service handles queries from clients. It is responsible for parsing them and searching ElasticSearch as well as on-demand discoverers and return results to clients.

It can also materialize datasets at a client's request, though it is possible for clients to do the materialization themselves.

Query API
---------

The query has two parts: locally available data that we need to augment (either join or union), and information on the data we want to find (specified loosely using keywords/shape/...). Each of those is optional, to support the various use-cases of DataMart: you can provide only local data (it's the case of TA2-only evaluation, there is no user to provide input of the kind of data that is desirable), or only information on the data we want (to support out-of-system queries from a user, e.g. just keywords, just shape, etc).

The query API is available both as a Python API and an HTTP API. The advantages of the Python API are ease of use and locality of processing (both inspection of the input data and materialization of the results can avoid a network copy). The advantages of the HTTP API are availability to non-Python software (in particular TA3s) and direct querying by users (from a web interface).

For now, the merging routine is part of the DataMart interface. There has been discussion on whether this should be separate (to allow users to access the best DataMart with the best joining routines) or even TA1 primitives, however this can be done later once such code is available.

First is the search endpoint::

    datamart.search(
        # Local data we have, specified as D3M dataset object
        data=dict(
            dataset=dataset,
            # path=".../data/datasetDoc.json",  # can also use path
            columns=None,  # use all columns
            # columns=[("0", "Player"), ("0", "Fielding_ave")],  # subset
        ),
        # TODO: Should we be able to provide D3M problem JSON?
        filter=dict(
            keywords=["baseball", "players"],
            license=["CC-0"],  # filter on license type
            columns=[
                # Those columns we are looking for
                {
                    "structural_type": "http://schema.org/Integer",
                    "keywords": ["age"],
                },
                {
                    "structural_type": "http://schema.org/Text",
                },
            ],
        ),
    )

    # Example user query (no data is provided)
    datamart.search(
        filter=dict(
            keywords=["new york"],
            columns=[
                {
                    "structural_type": "http://schema.org/Float",
                    "keywords": ["wind speed"],
                },
                {
                    "structural_type": "http://schema.org/Float",
                    "semantic_types": ["http://schema.org/Latitude"],
                    "float_range": [40.65, 40.83],
                },
                },
                {
                    "structural_type": "http://schema.org/Float",
                    "semantic_types": ["http://schema.org/Longitude"],
                    "float_range": [-73.9, -74.0],
                },
            ],
        ),
    )

Over HTTP, the API is similar (POST a JSON object)::

    POST /search
    Content-Type: application/json
    Accept: application/json

    {
        "data": {
            ... unspecified datamart-specific data description ...
            # otherwise you can send the data using multipart upload
        },
        "filter": {
            "keywords": ["baseball", "players"],
            "license": ["CC-0"],
            "columns": [
                {
                    "colType": "http://schema.org/Integer",
                    "keywords": ["age"],
                },
                {
                    "colType": "http://schema.org/Text",
                }
            ]
        }
    }

To upload data, a standard multipart upload is used::

    POST /search
    Content-Type: multipart/form-data; boundary=sep

    --sep
    Content-Disposition: form-data; name="datasetDoc.json"; filename="datasetDoc.json"
    Content-Type: application/octet-stream

    ...

    --sep
    Content-Disposition: form-data; name="query"
    Content-Type: application/json

    {
        "filter": {
            "keywords": ["weather"],
            "license": ["CC-0"]
        }
    }

The results are provided back as a list of objects in JSON::

    [
        {
            "augmentType": "join" / "union",
            "id": "datamart.noaa_discoverer.GHCND.AEM00041194.201705",
            "score": 0.758,
            "metadata": {
                "about": {
                    # Matches datasetDoc's `about`
                    ...
                },
                "columns": [
                    # Matches datasetDoc's `dataResources[].columns`
                    {
                        "colIndex": 0,
                        "colName": "lat",
                        "role": "attribute",
                        "colType": "http://schema.org/Float",
                        "semantic_types": [
                            "http://schema.org/Latitude"
                        ],
                        "unionWith": "latitude" / "refersTo": "latitude"
                    },
                    {
                        "colIndex": 1,
                        "colName": "temperature",
                        "role": "attribute",
                        "colType": "http://schema.org/Float",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Temperature"
                        ]
                    },
                ]
            },
            "materialize": {
                # This is private information for the DataMart to materialize
                # the dataset (so it can be done client-side)
                "identifier": "datamart.noaa_discoverer",
                "noaa_dataset_id": "GHCND",
                "noaa_station_id": "AEM00041194",
                "noaa_start": "2017-05-01",
                "noaa_end": "2017-05-31"
            }
        },
        {
            "id": "datamart.socrata_discoverer.9hyh-zkx9",
            "score": 0.679,
            "materialize": {
                "identifier": "datamart.socrata_discoverer",
                "socrata_domain": "data.cityofnewyork.us",
                "socrata_id": "9hyh-zkx9",
                "socrata_updated": "2018-12-02T11:22:34Z",
                "direct_url": "https://data.cityofnewyork.us/api/views/9hyh-zkx9/rows.csv?accessType=DOWNLOAD"
            },
            "metadata": {
                ...
            }
        }
    ]

Download API
------------

You can provide the dataset ID or the full object returned by search to the download function to materialize a dataset::

    datamart.download(results[0])  # Full dict, can use 'materialize' info to materialize client-side

    datamart.download("9hyh-zkx9", destination="augmentation_data/selected_dataset")  # Dataset ID

The client can use the ``materialize`` dictionary to download the dataset directly. This will allow the client to avoid latency, queueing in the server, use their own API key, ...

Otherwise, the query service also supports materializing the dataset and returning it to the client through HTTP download. Use the ``/download/<dataset_id>`` endpoint.

Join Evaluation API
-------------------

An additional endpoint allows to evaluate joins::

    datamart.evaluate_join(
        data=...,
        result=result[0],
    )
    # -> {"score": 0.894}

    datamart.evaluate_join(
        data=...,
        result="9hyh-zkx9",
    )
    # -> {"score": 0.275}

Join API
--------

This is tentatively part of the DataMart API as well::

    datamart.join(
        data=...,
        result="9hyh-zkx9",
    )
    # -> D3M Dataset object

Questions
=========

* How to provide data? D3M Dataset, D3M DataFrame, ...?
* Should we also send the D3M Problem definition? Has very useful bits
* Should we specify this "materialize" bit?
* D3M doesn't use schema.org types and roles, just "integer", "attribute", ...
* Is multipart upload too insane? (HTTP API can be specified later)
