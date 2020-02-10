import json
import logging
import os
import sys


logger = logging.getLogger('benchmark_profilers')


DATASETS_LABELED = os.path.join(
    os.path.dirname(__file__),
    '../data/training_datasets/seed_datasets_archive',
)
DATASETS_UNLABELED = os.path.join(
    os.path.dirname(__file__),
    '../data/seed_datasets_current',
)
OUTPUT = os.path.join(
    os.path.dirname(__file__),
    'profiled_datasets',
)
# http://public.datadrivendiscovery.org/simon_models_1.tar.gz
SIMON_MODELS = '/media/remram/datamart/simon_models'


def main():
    logging.basicConfig(level=logging.INFO)
    if not os.path.isdir(OUTPUT):
        os.mkdir(OUTPUT)

    # Build list of datasets
    datasets = []
    for name in os.listdir(DATASETS_UNLABELED):
        path = os.path.join(
            DATASETS_UNLABELED,
            name,
            '%s_dataset' % name,
        )
        if not os.path.exists(os.path.join(path, 'datasetDoc.json')):
            logger.error("Can't find dataset in folder %s", name)
            sys.exit(1)
        if name.endswith('_MIN_METADATA'):
            name = name[:-13]
        logger.info("Found dataset %s", name)
        datasets.append((path, name))
    logger.info("%d datasets to process", len(datasets))

    # Process them
    for path, name in datasets:
        logger.info("Processing %s...", name)

        outputs = os.path.join(OUTPUT, name)
        if not os.path.isdir(outputs):
            os.mkdir(outputs)

        # MIT-LL
        out = os.path.join(outputs, 'mitll')
        if not os.path.exists(out):
            try:
                types = get_mitll_types(name)
            except Exception:
                logger.exception("Error getting MIT-LL metadata")
            else:
                with open(out, 'w') as fp:
                    json.dump(types, fp)

        # Simon
        out = os.path.join(outputs, 'simon')
        if not os.path.exists(out):
            try:
                types = get_simon_types(path)
            except Exception:
                logger.exception("Error running Simon")
            else:
                with open(out, 'w') as fp:
                    json.dump(types, fp)

        # Datamart
        out = os.path.join(outputs, 'datamart')
        if not os.path.exists(out):
            try:
                types = get_datamart_types(path)
            except Exception:
                logger.exception("Error running Datamart")
            else:
                with open(out, 'w') as fp:
                    json.dump(types, fp)


def get_mitll_types(name):
    """Get the dataset written by MIT-LL.
    """
    dataset_doc = os.path.join(
        DATASETS_LABELED,
        name,
        '%s_dataset' % name,
        'datasetDoc.json',
    )
    with open(dataset_doc) as fp:
        doc = json.load(fp)

    types = []
    for column in doc['dataResources'][0]['columns']:
        types.append(column['colType'])
    return types


def get_simon_types(path):
    from d3m.container import Dataset
    ds = Dataset.load('file://%s/datasetDoc.json' % os.path.abspath(path))

    def hyperparams(cls, **kwargs):
        hp_cls = cls.metadata.query()['primitive_code']['class_type_arguments']['Hyperparams']
        return hp_cls(hp_cls.defaults(), **kwargs)

    from d3m.primitives.data_transformation.dataset_to_dataframe import Common as DatasetToDataframe
    to_dataframe = DatasetToDataframe(
        hyperparams=hyperparams(DatasetToDataframe)
    )
    res = to_dataframe.fit()
    assert res.has_finished and res.value is None
    res = to_dataframe.produce(inputs=ds)
    assert res.has_finished
    dfU = res.value

    from d3m.primitives.data_cleaning.column_type_profiler import Simon
    simon = Simon(
        hyperparams=hyperparams(Simon),
        volumes={'simon_models_1': SIMON_MODELS},
    )
    simon.set_training_data(inputs=dfU)
    res = simon.fit()
    assert res.has_finished and res.value is None
    res = simon.produce(inputs=dfU)
    assert res.has_finished
    dfM = res.value

    return d3m_metadata_to_types(dfM.metadata)


def d3m_metadata_to_types(metadata):
    from d3m.metadata.base import ALL_ELEMENTS

    types = []
    for idx in range(metadata.query([ALL_ELEMENTS])['dimension']['length']):
        types.append(metadata.query([ALL_ELEMENTS, idx])['semantic_types'])

    return types


def get_datamart_types(path):
    from datamart_profiler import process_dataset

    metadata = process_dataset(os.path.join(path, 'tables/learningData.csv'))

    return metadata['columns']


if __name__ == '__main__':
    main()
