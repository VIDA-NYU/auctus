#!/usr/bin/env python3
import csv
import json
import logging
import os
import requests
from urllib.parse import urlencode


logger = logging.getLogger()


def sparql_query(query):
    """Get results from the Wikidata SparQL endpoint.
    """
    url = 'https://query.wikidata.org/sparql?' + urlencode({
        'query': query,
    })
    logger.info("Querying: %s", url)
    response = requests.get(
        url,
        headers={
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'Auctus',
        },
    )
    response.raise_for_status()
    obj = response.json()
    results = obj['results']['bindings']
    logger.info("SparQL: %d results", len(results))
    return results


def main():
    logging.basicConfig(level=logging.INFO)
    os.chdir(os.path.dirname(__file__) or '.')

    # Get all countries with their geometry
    # P31 = instance of
    # Q6256 = country
    countries = sparql_query(
        'SELECT ?item ?shape\n'
        'WHERE\n'
        '{\n'
        '  ?item wdt:P31 wd:Q6256.\n'  # item "instance of" "country"
        '  ?item wdt:P3896 ?shape.\n'  # item "geoshape" shape
        '  MINUS{ ?item wdt:P31 wd:Q3024240. }\n'  # not "historical country"
        '}\n'
    )
    with open('geoshapes.csv', 'w', newline='', encoding='utf-8') as fp:
        writer = csv.writer(fp)
        writer.writerow(['country', 'geoshape URL', 'geoshape'])
        for country in countries:
            assert country['item']['type'] == 'uri'
            prefix = 'http://www.wikidata.org/entity/'
            value = country['item']['value']
            assert value.startswith(prefix)
            value = value[len(prefix):]

            assert country['shape']['type'] == 'uri'
            shape_uri = country['shape']['value']

            try:
                logger.info("Getting geoshape %s", shape_uri)
                shape_resp = requests.get(shape_uri)
                shape_resp.raise_for_status()
                shape = json.dumps(
                    shape_resp.json(),
                    # Compact
                    sort_keys=True, indent=None, separators=(',', ':'),
                )
            except requests.exceptions.HTTPError as e:
                logger.error("Error getting geoshape: %s", e)
                shape = None

            writer.writerow([value, shape_uri, shape])


if __name__ == '__main__':
    main()
