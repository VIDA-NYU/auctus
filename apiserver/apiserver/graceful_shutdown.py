import asyncio
import logging
import signal
import tornado.ioloop
import tornado.web

from datamart_core.common import log_future


logger = logging.getLogger(__name__)


class GracefulApplication(tornado.web.Application):
    """Application that exits on SIGTERM once no GracefulHandlers are running.
    """
    def __init__(self, *args, **kwargs):
        super(GracefulApplication, self).__init__(*args, **kwargs)

        self.is_closing = False
        self.nb_requests = 0
        self.close_condition = asyncio.Condition()

        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        logger.warning("Got signal %s, exiting...", signum)
        self.is_closing = True
        tornado.ioloop.IOLoop.current().add_callback_from_signal(self.try_exit)

    def try_exit(self):
        async def do_exit():
            async with self.close_condition:
                while self.nb_requests > 0:
                    logger.info("%d requests in progress, waiting...",
                                self.nb_requests)
                    await self.close_condition.wait()
            logger.warning("Closing gracefully")
            tornado.ioloop.IOLoop.current().stop()

        log_future(asyncio.get_event_loop().create_task(do_exit()), logger)


class GracefulHandler(tornado.web.RequestHandler):
    """Handlers that will prevent the application to exit until they're done.
    """
    def prepare(self):
        super(GracefulHandler, self).prepare()
        self.application.nb_requests += 1

    def on_finish(self):
        super(GracefulHandler, self).on_finish()

        app = self.application

        async def do_decrease():
            async with app.close_condition:
                app.nb_requests -= 1
                app.close_condition.notify_all()

        log_future(asyncio.get_event_loop().create_task(do_decrease()), logger)
