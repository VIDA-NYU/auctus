DataMart Query Service
======================

This service handles queries from clients. It is responsible for parsing them and searching ElasticSearch as well as on-demand discoverers and return results to clients.

It can also materialize datasets at a client's request, though it is possible for clients to do the materialization themselves.

Query API
---------

The API is based on JSON. A JSON object describing the query is POSTed to the ``/query`` endpoint. ``Accept`` header `SHOULD` be set to  ``application/json``.

The query object has the following format::

    {
        "i_have": {
            "keywords": ["taxi", "demand"],
            "columns": [
                {
                    "structural_type": "http://schema.org/Float",
                    "semantic_types": [
                        "http://schema.org/Latitude"
                    ],
                    "name": "lat"
                },
                {
                    "structural_type": "http://schema.org/Float",
                    "semantic_types": [
                        "http://schema.org/Longitude"
                    ],
                    "name": "long"
                },
                {
                    "structural_type": "http://schema.org/Float",
                    "name": "taxi demand"
                }
            ],
        },
        "i_want": {
            "keywords": ["weather", "nyc"],
            "columns": [
                {
                    "structural_type": "http://schema.org/Float",
                    "keywords": ["temperature"]
                },
                {
                    "structural_type": "http://schema.org/Float",
                    "keywords": ["wind speed"]
                }
            ]
        }
    }

And the response::

    {
        "results": [
            {
                "augmentType": "join" / "union",
                "id": "datamart.noaa_discoverer.GHCND.AEM00041194.201705",
                "score": 0.758,
                "materialize": {
                    "identifier": "datamart.noaa_discoverer",
                    "noaa_dataset_id": "GHCND",
                    "noaa_station_id": "AEM00041194",
                    "noaa_start": "2017-05-01",
                    "noaa_end": "2017-05-31"
                },
                "metadata": {
                    ...
                    "columns": [
                        {
                            "structural_type": "http://schema.org/Float",
                            "semantic_types": [
                                "http://schema.org/Latitude"
                            ],
                            "name": "lat",
                            "unionWith": "latitude" / "refersTo": "latitude"
                        },
                        {
                            "structural_type": "http://schema.org/Float",
                            "semantic_types": [
                                "https://metadata.datadrivendiscovery.org/types/Temperature"
                            ],
                            "name": "temperature"
                        },
                    ]
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
    }

Download API
------------

The client can use the ``materialize`` dictionary to download the dataset directly. This will allow the client to avoid latency, queueing in the server, use their own API key, ...

Otherwise, the query service also supports materializing the dataset and returning it to the client through HTTP download. Use the ``/download/<dataset_id>`` endpoint.

D3M Set-Up
----------

We envision the search to be done outside the pipeline, through a TA1 primitive or not::

    search_datamart(keywords=..., column1=..., column2=...)

The user can select dataset IDs in that list from the TA3 interface, or if running in TA2-only-mode, TA2 can make multiple pipelines with each of the top N results.

Then the download/materialization and join would appear in the pipeline as primitives::

      input dataset
           |
    DenormalizeDataset
           |
           |    MaterializeDatamart(dataset_id=...)
           |    /
         DataJoin
           |
          ...
           |
    ConstructPredictions
