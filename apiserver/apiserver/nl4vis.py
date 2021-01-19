# import io
import logging
import json
import prometheus_client

from datamart_core.prom import PromMeasureRequest

from .base import BUCKETS, BaseHandler
from .graceful_shutdown import GracefulHandler
from .profile import ProfilePostedData

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
    # def initialize(self, *, data_path=None):
    #     self.data_path = None
    #     self.data_frame = None
    #     self.data_csv = None
    #     self.nl4dv_instance = None

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
        # logger.info("Got nl4vis, question")

        # if self.data_path != "/datasets/"+data_id+"/main.csv":
        #     self.data_path = "/datasets/"+data_id+"/main.csv"

        #     self.data_frame = pd.read_csv(self.data_path)
        #     self.data_csv = self.data_frame.to_csv()
        #     self.nl4dv_instance = nl4dv.NL4DV(data_value=self.data_frame)

        data_source = self.application.elasticsearch.get(
            'datamart', data_id
        )['_source']
        # data_sample = data_source['sample']

        columns = data_source['columns']
        filter_col = []
        for col in columns:
            if col['structural_type'] != 'http://schema.org/Text':
                filter_col.append(col['name'])

        # logger.info(data_source)

        # logger.info("line 64")
        # data_frame = pd.read_csv(data_path)
        # df_sample = pd.read_csv(io.StringIO(data_sample))
        # logger.info("line 66")
        # nl4dv_instance = nl4dv.NL4DV(data_value=data_frame)
        # logger.info("line 68")

        # vis_lists = self.nl4dv_instance.analyze_query(question)['visList']
        data_path = "/datasets/" + data_id + "/main.csv"
        data_frame = pd.read_csv(data_path)
        data_frame = data_frame.dropna(subset=filter_col)
        nl4dv_instance = nl4dv.NL4DV(data_value=data_frame)
        vis_lists = nl4dv_instance.analyze_query(question)['visList']

        for vis in vis_lists:
            # vis['data_sample'] = {
            #     "values":[
            #         {
            #             "gold": g,
            #             "height": h
            #         }
            #         for g, h in zip(list(data_frame["gold"]),list(data_frame["height"]))
            #     ]
            # }
            vis["vlSpec"]["data"] = {"name": "values"}

        # js_array = []
        # with open(data_path) as csvf:
        #     for row in csv.DictReader(csvf):
        #         js_array.append(row)
        # vis_data = json.dumps(js_array, indent=4)

        logger.info("line 71")

        vis_data = {
            "values": json.loads(data_frame.to_json(orient="records"))
        }

        results = []
        results.append(dict(
            test_nl4vis='Hello, this is a test of nl4vis',
            visualizations=vis_lists,
            visualizations_number=len(vis_lists),
            vis_data=vis_data
        ))

        # print(json.dumps(vis_lists[0]['vlSpec'], indent=4))

        # print(json.dumps(results[0]['visualizations_number'], indent=4))

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
