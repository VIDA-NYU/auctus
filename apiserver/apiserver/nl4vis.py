import contextlib
import io
import logging
import json
import os
import prometheus_client
import shutil
import zipfile

from datamart_augmentation.augmentation import AugmentationError
from datamart_core.augment import augment
from datamart_core.common import hash_json, contextdecorator
from datamart_core.fscache import cache_get, cache_get_or_set
from datamart_core.materialize import get_dataset, make_zip_recursive
from datamart_core.prom import PromMeasureRequest
from datamart_materialize import make_writer

from .base import BUCKETS, BaseHandler
from .graceful_shutdown import GracefulHandler
from .profile import ProfilePostedData, get_data_profile_from_es, \
    profile_token_re
from .search import get_augmentation_search_results

import pandas as pd
import nl4dv


logger = logging.getLogger(__name__)


PROM_NL4VIS = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_nl4vis_count',
        "Nl4vis requests",
    ),
    time=prometheus_client.Histogram(
        'req_nl4vis_seconds',
        "Nl4vis request time",
        buckets=BUCKETS,
    ),
)


class NL4VIS(BaseHandler, GracefulHandler, ProfilePostedData):
    @PROM_NL4VIS.sync()
    def post(self):
        type_ = self.request.headers.get('Content-type', '')
        if not type_.startswith('multipart/form-data'):
            return self.send_error_json(
                400,
                "Use multipart/form-data to send the 'data' file and "
                "'task' JSON",
            )

        question = self.get_body_argument('question', None)
        # if question is not None:
        #     question = question.encode('utf-8')

        data_id = self.get_body_argument('data_id', None)

        logger.info("Got nl4vis, question")

        data_sample = self.application.elasticsearch.get(
                'datamart', data_id
            )['_source']['sample']

        logger.info("line 64")
        data_frame = pd.read_csv(io.StringIO(data_sample))
        logger.info("line 66")
        nl4dv_instance = nl4dv.NL4DV(data_value=data_frame)
        logger.info("line 68")
        vis_lists = nl4dv_instance.analyze_query(question)['visList']

        for vis in vis_lists:
            vis['data_sample'] = {
                "values": data_frame.to_json(orient="records")
            }
            vis["vlSpec"]["data"] = {"name": "values"}
        
        logger.info("line 71")
        results = []
        results.append(dict(
            test_nl4vis='Hello, this is a test of nl4vis',
            visualizations=vis_lists,
            visualizations_number=len(vis_lists)
            ))

        

        logger.info("Got nl4vis, data_id")

        # d_id = "datamart.upload.14c7a6657bd44e379c29e4b66822f080"
        # data_test = self.application.elasticsearch.get(
        #         'datamart', d_id
        #     )
        # meta_test = data_test['_source']
        # logger.info(data_test.keys())
        # logger.info(type(data_test))
        # logger.info(data_test)
        # # logger.info(meta_test.keys())
        # logger.info(type(meta_test))
        # logger.info(meta_test)
        # results[0]['data_test'] = data_test
        # results[0]['meta_test'] = meta_test



        return self.send_json(results)


        

