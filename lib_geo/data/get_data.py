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


@makes_file('geoshapes0.csv')
def geoshapes0(writer):
    """Get all countries with their geometry.
    """
    rows = sparql_query(
        'SELECT ?area ?shape\n'
        'WHERE\n'
        '{\n'
        '  ?area wdt:P31 wd:Q6256.\n'  # area "instance of" "country"
        '  ?area wdt:P3896 ?shape.\n'  # area "geoshape" shape
        '  MINUS{ ?area wdt:P31 wd:Q3024240. }\n'  # not "historical country"
        '}\n'
    )

    writer.writerow(['admin', 'geoshape URL', 'geoshape'])
    for row in rows:
        area = q_entity_uri(row['area'])
        shape_uri = uri(row['shape'])

        # FIXME: Work around Wikidata bug: '+' in URL needs to be '_'
        last_slash = shape_uri.index('/')
        if '+' in shape_uri[last_slash + 1:]:
            shape_uri = (
                shape_uri[:last_slash + 1] +
                shape_uri[last_slash + 1:].replace('+', '_')
            )

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

        writer.writerow([area, shape_uri, shape])


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


WIKIDATA_ADMIN_LEVELS = [
    'Q6256',        # "country"
    'Q10864048',    # "first-level administrative country subdivision"
    'Q13220204',    # "second-level administrative country subdivision"
    'Q13221722',    # "third-level administrative country subdivision"
    'Q14757767',    # "fourth-level administrative country subdivision"
    'Q15640612',    # "fifth-level administrative country subdivision"
]


def get_admin_level(level):
    if level == 0:
        rows = sparql_query(
            'SELECT ?country ?countryLabel\n'
            'WHERE\n'
            '{\n'
            '  ?country wdt:P31 wd:Q6256.\n'
            '  SERVICE wikibase:label {\n'
            '    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".\n'
            '  }\n'
            '}\n'
        )
        for row in rows:
            yield (
                None,
                q_entity_uri(row['country']),
                literal(row['countryLabel']),
            )
    elif level == 1:
        rows = sparql_query(
            'SELECT ?parent ?area ?areaLabel\n'
            'WHERE\n'
            '{\n'
            # parent "instance of" "country" (country = admin level 0)
            '  ?parent wdt:P31 wd:Q6256.\n'
            # parent "contains administrative territorial entity" child
            '  ?parent wdt:P150 ?area.\n'
            # child "instance of" ["subclass of" "admin level 1"]
            '  ?area wdt:P31 [wdt:P279 wd:Q10864048].\n'
            '  SERVICE wikibase:label {\n'
            '    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".\n'
            '  }\n'
            '}\n'
        )
        for row in rows:
            yield (
                q_entity_uri(row['parent']),
                q_entity_uri(row['area']),
                literal(row['areaLabel']),
            )
    else:
        rows = sparql_query(
            'SELECT ?parent ?area ?areaLabel\n'
            'WHERE\n'
            '{{\n'
            # tmp0 "instance of" "country" (country = admin level 0)
            '  ?tmp0 wdt:P31 wd:Q6256.\n'
            # go down 0 to level-2 levels, to the immediate parent
            '{levels}\n'
            '  ?parent wdt:P150 ?area.\n'
            '  ?area wdt:P31 [wdt:P279 wd:{klass}].\n'
            '  SERVICE wikibase:label {{\n'
            '    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".\n'
            '  }}\n'
            '}}\n'.format(
                levels='\n'.join(
                    f'?tmp{i} wdt:P150? ?tmp{i+1}.'
                    for i in range(level - 2)
                ),
                klass=WIKIDATA_ADMIN_LEVELS[level]
            )
        )
        for row in rows:
            yield (
                q_entity_uri(row['parent']),
                q_entity_uri(row['area']),
                literal(row['areaLabel']),
            )


@makes_file('areas0.csv')
def areas0(writer):
    """Get the level 0 of areas (countries).
    """
    writer.writerow(['parent', 'admin', 'admin level', 'admin name'])

    for parent, admin, admin_name in get_admin_level(0):
        writer.writerow([parent, admin, 0, admin_name])


@makes_file('areas1.csv')
def areas1(writer):
    """Get one level of administrative areas.
    """
    writer.writerow(['parent', 'admin', 'admin level', 'admin name'])

    for parent, admin, admin_name in get_admin_level(1):
        writer.writerow([parent, admin, 1, admin_name])


@makes_file('areas2.csv')
def areas2(writer):
    """Get two levels of administrative areas.
    """
    writer.writerow(['parent', 'admin', 'admin level', 'admin name'])

    for parent, admin, admin_name in get_admin_level(2):
        writer.writerow([parent, admin, 2, admin_name])


def main():
    logging.basicConfig(level=logging.INFO)
    os.chdir(os.path.dirname(__file__) or '.')

    countries()
    geoshapes0()
    country_names()
    areas0()
    areas1()
    areas2()


if __name__ == '__main__':
    main()
