import logging
import tornado.ioloop

from .discovery import UrlDiscoverer, BingDiscoverer
from .web import make_web_discovery_app


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    UrlDiscoverer('datamart.url')
    BingDiscoverer('datamart.bing')

    app = make_web_discovery_app
    app.listen(8003)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()
