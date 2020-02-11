import csv
import json
import os


PROFILERS = ['mitll', 'simon', 'dsbox', 'datamart']


not_types = {
    'https://metadata.datadrivendiscovery.org/types/PrimaryKey',
    'https://metadata.datadrivendiscovery.org/types/PrimaryMultiKey',
    'https://metadata.datadrivendiscovery.org/types/UniqueKey',
    'https://metadata.datadrivendiscovery.org/types/Attribute',
    'https://metadata.datadrivendiscovery.org/types/Target',
    'https://metadata.datadrivendiscovery.org/types/TrueTarget',
    'https://metadata.datadrivendiscovery.org/types/SuggestedGroupingKey',
    'https://metadata.datadrivendiscovery.org/types/SuggestedTarget',
    'https://metadata.datadrivendiscovery.org/types/Boundary',
    'https://metadata.datadrivendiscovery.org/types/IntervalStart',
    'https://metadata.datadrivendiscovery.org/types/IntervalEnd',
}


def d3m_type(types):
    types = [
        t for t in types
        if t not in not_types
    ]
    if 'https://metadata.datadrivendiscovery.org/types/CategoricalData' in types:
        return 'https://metadata.datadrivendiscovery.org/types/CategoricalData'
    if not types:
        return '(unknown)'
    return ', '.join(types)


def datamart_type(col):
    if 'http://schema.org/Boolean' in col['semantic_types']:
        return 'http://schema.org/Boolean'
    elif 'http://schema.org/Enumeration' in col['semantic_types']:
        return 'http://schema.org/Enumeration'
    elif 'http://schema.org/DateTime' in col['semantic_types']:
        return 'http://schema.org/DateTime'
    else:
        return col['structural_type']


def main():
    with open('profiled_datasets.csv', 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(['dataset', 'column'] + PROFILERS)
        for dataset in os.listdir('profiled_datasets'):
            profiles = {}
            for prof in os.listdir(os.path.join('profiled_datasets', dataset)):
                with open(os.path.join('profiled_datasets', dataset, prof)) as fp:
                    obj = json.load(fp)
                    if prof == 'datamart':
                        profiles[prof] = [
                            datamart_type(p)
                            for p in obj
                        ]
                    elif prof in ('simon', 'dsbox'):
                        profiles[prof] = [
                            d3m_type(p)
                            for p in obj
                        ]
                    else:
                        profiles[prof] = obj
            if not profiles:
                continue
            nb_columns = max(len(p) for p in profiles.values())
            defaults = ['(not present)'] * nb_columns
            for i in range(nb_columns):
                writer.writerow(
                    [dataset, str(i)] +
                    [(profiles.get(prof, []) + defaults)[i] for prof in PROFILERS]
                )


if __name__ == '__main__':
    main()
