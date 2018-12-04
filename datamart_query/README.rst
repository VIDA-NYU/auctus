DataMart Query Component
========================

This container handles queries from clients. It is responsible for parsing them and searching ElasticSearch as well as on-demand discoverers and return results to clients.

It can also materialize datasets at a client's request, though it is possible for clients to do the materialization themselves.

Query API
---------

The API is based on JSON. A JSON object describing the query is POSTed to the ``/query`` endpoint. ``Accept`` header `SHOULD` be set to  ``application/json``.

The query object has the following format::

    {
        "keywords": ["weather", "nyc"],
        "search_for": ["union", "join"],
        "columns_all": [
            {
                "structural_type": "http://schema.org/Text",
                "semantic_types": [
                    "http://schema.org/DateTime"
                ]
            },
            {
                "structural_type": "http://schema.org/Float",
                "semantic_types": [
                    "https://metadata.datadrivendiscovery.org/types/Temperature"
                ],
                "keywords": ["ground", "average", "month"]
            }
        ]
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
