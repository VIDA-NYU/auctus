#!/usr/bin/env python3
import csv
import functools
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


def makes_file(name):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped():
            if os.path.exists(name):
                logger.info("Skipping: %s", func.__doc__.splitlines()[0])
                return
            logger.info("Running: %s", func.__doc__.splitlines()[0])
            try:
                with open(name, 'w', newline='', encoding='utf-8') as fp:
                    writer = csv.writer(fp)
                    func(writer)
            except BaseException:
                os.remove(name)
                raise
        return wrapped
    return wrapper


def literal(item):
    assert item['type'] == 'literal'
    return item['value']


def q_entity_uri(item):
    assert item['type'] == 'uri'
    prefix = 'http://www.wikidata.org/entity/'
    value = item['value']
    assert value.startswith(prefix)
    return value[len(prefix):]


def uri(item):
    assert item['type'] == 'uri'
    return item['value']


@makes_file('countries.csv')
def countries(writer):
    """Get all countries with label and standard codes.
    """
    rows = sparql_query(
        'SELECT ?item ?itemLabel ?fips ?iso2 ?iso3\n'
        'WHERE\n'
        '{\n'
        '  ?item wdt:P31 wd:Q6256.\n'  # item "instance of" "country"
        '  ?item wdt:P901 ?fips.\n'  # item "FIPS 10-4 (countries and region)"
        '  ?item wdt:P297 ?iso2.\n'  # item "ISO 3166-1 alpha-2 code"
        '  ?item wdt:P298 ?iso3.\n'  # item "ISO 3166-1 alpha-3 code"
        '  MINUS{ ?item wdt:P31 wd:Q3024240. }\n'  # not "historical country"
        '  SERVICE wikibase:label {\n'
        '    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".\n'
        '  }\n'
        '}\n'
    )

    writer.writerow(['country', 'label', 'fips', 'iso2', 'iso3'])
    for row in rows:
        value = q_entity_uri(row['item'])
        label = literal(row['itemLabel'])
        fips = literal(row['fips'])
        iso2 = literal(row['iso2'])
        iso3 = literal(row['iso3'])

        writer.writerow([value, label, fips, iso2, iso3])


@makes_file('geoshapes.csv')
def geoshapes(writer):
    """Get all countries with their geometry.
    """
    rows = sparql_query(
        'SELECT ?item ?shape\n'
        'WHERE\n'
        '{\n'
        '  ?item wdt:P31 wd:Q6256.\n'  # item "instance of" "country"
        '  ?item wdt:P3896 ?shape.\n'  # item "geoshape" shape
        '  MINUS{ ?item wdt:P31 wd:Q3024240. }\n'  # not "historical country"
        '}\n'
    )

    writer.writerow(['country', 'geoshape URL', 'geoshape'])
    for row in rows:
        value = q_entity_uri(row['item'])
        shape_uri = uri(row['shape'])

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


@makes_file('country_names.csv')
def country_names(writer):
    """Get the localized names of countries.
    """
    rows = sparql_query(
        'SELECT ?item ?name ?nameLang\n'
        'WHERE\n'
        '{\n'
        '  ?item wdt:P31 wd:Q6256.\n'  # item "instance of" "country"
        '  ?item wdt:P1448 ?name.\n'  # item "official name" name
        '  BIND(LANG(?name) AS ?nameLang).\n'  # nameLang = LANG(name)
        '  MINUS{ ?item wdt:P31 wd:Q3024240. }\n'  # not "historical country"
        '}\n'
    )

    writer.writerow(['country', 'name', 'name_lang'])
    for row in rows:
        value = q_entity_uri(row['item'])
        name = literal(row['name'])
        name_lang = literal(row['nameLang'])

        writer.writerow([value, name, name_lang])


def main():
    logging.basicConfig(level=logging.INFO)
    os.chdir(os.path.dirname(__file__) or '.')

    countries()
    geoshapes()
    country_names()


if __name__ == '__main__':
    main()
