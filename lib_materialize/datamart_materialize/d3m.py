import json
import os


class D3mWriter(object):
    needs_metadata = True

    def __init__(self, destination, metadata):
        self.destination = destination
        os.mkdir(destination)
        os.mkdir(os.path.join(destination, 'tables'))

        with open(os.path.join(destination, 'datasetDoc.json'), 'w') as fp:
            # TODO: Convert metadata to D3M schema
            json.dump(metadata, fp)

    def open_file(self, mode='wb', name=None, **kwargs):
        if name is not None:
            raise ValueError("D3mWriter can only write single-table datasets "
                             "for now")
        return open(os.path.join(self.destination,
                                 'tables', 'learningData.csv'),
                    mode, **kwargs)

    def finish(self):
        return None
