API client
==========

A client library for `the REST API <../rest>`__ is available for convenience. It supports searching, downloading, and augmenting datasets.

It can perform some operations both on the client-side (for speed, the server has limited capacity; also saves time by not uploading the data) and on the server-side in "proxy mode" (working around the need to install and configure some dependencies on the client, and taking advantage of cached results on the server).

Installing datamart-rest
------------------------

You can get it directly from the Python Package Index using PIP::

    pip install datamart-rest

API
---

The REST client is currently maintained as part of the D3M project, with `documentation available here <https://datadrivendiscovery.gitlab.io/datamart-api/>`__.
