import aiohttp
import asyncio
import logging
import uuid

from datamart_core import AsyncDiscoverer
from datamart_core.common import log_future, msg2json

from .crawl import DatasetFinder, bing_search


logger = logging.getLogger(__name__)


class WebDiscoverer(AsyncDiscoverer, DatasetFinder):
    """Base class for web discoverer, which can find CSV in web pages.
    """
    async def dataset_found(self, url, page):
        metadata = page.get('metadata', {})

        dataset_id = uuid.uuid5(uuid.NAMESPACE_URL, str(url)).hex

        await self.record_dataset(dict(direct_url=url),
                                  metadata,
                                  dataset_id=dataset_id)


class UrlDiscoverer(WebDiscoverer):
    """Discoverer reading URLs of interest from an AMQP queue.
    """

    async def _run(self):
        await super(WebDiscoverer, self)._run()

        # Declare the urls queue
        self.urls_queue = await self.channel.declare_queue('urls')

        log_future(self.loop.create_task(self._consume_urls()),
                   logger,
                   should_never_exit=True)

    async def _consume_urls(self):
        async with aiohttp.ClientSession() as session:
            async for message in self.urls_queue:
                obj = msg2json(message)
                log_future(
                    self.loop.create_task(self.find_datasets(session, obj)),
                    logger,
                    message="Exception processing URL",
                )
                message.ack()


class BingDiscoverer(WebDiscoverer):
    """Discoverer feeding on-demand queries into Bing Web Search.
    """
    async def handle_query(self, query, publish):
        keywords = set()
        if 'about' in query.get('dataset', {}):
            keywords.update(query['dataset']['about'].split())
        # TODO: Keywords from other interesting fields?
        keywords = ' '.join(keywords)

        async with aiohttp.ClientSession() as session:
            results = await bing_search(session, keywords)

            # Try all the top results
            futures = []
            for page in results:
                futures.append(self.loop.create_task(
                    self.find_datasets(session, dict(url=page['url']))
                ))
            await asyncio.wait(futures)
