import aiohttp
import asyncio
from bs4 import BeautifulSoup
import logging
import os
from yarl import URL


logger = logging.getLogger(__name__)


def get_mimetype(resp):
    if 'Content-Type' not in resp.headers:
        return None
    else:
        mimetype = resp.headers['Content-Type']
        mimetype = mimetype.split(';', 1)[0]
        return mimetype.lower()


class DatasetFinder(object):
    GOOD_TYPES = {'text/csv', 'application/octet-stream', 'text/plain'}
    BAD_EXTS = ['.html', '.html5', '.php', '.php5']
    MAX_FILES = 20

    def __init__(self):
        super(DatasetFinder, self).__init__()

        self.loop = asyncio.get_event_loop()

    async def find_datasets(self, session, page):
        logger.info("Processing URL %s", page['url'])
        try:
            async with session.get(page['url'],
                                   headers=page.get('headers')) as resp:
                mimetype = get_mimetype(resp)
                if mimetype in self.GOOD_TYPES:
                    logger.info("Checking file...")
                    url = await self.check_file(resp)
                    if url:
                        await self.dataset_found(url, page)
                        return
                elif mimetype != 'text/html':
                    logger.info("Ignoring URL, type is %s", mimetype)
                    return
                content = await resp.read()
        except aiohttp.ClientError:
            logger.info("Exception getting %s", page['url'])
            return

        # Find all links
        logger.info("Processing HTML...")
        soup = BeautifulSoup(content, 'html5lib')
        links = soup.find_all('a')
        links = list({elem.attrs['href'] for elem in links
                      if 'href' in elem.attrs})
        links = list(str(resp.url.join(URL(link))) for link in links)
        total_links = len(links)

        # If some of the links are CSVs
        csvs = [link for link in links if link.endswith('.csv')]
        if csvs:
            # Only keep those
            links = csvs
        else:
            # Else, at least discard the obviously HTML ones
            newlinks = []
            for link in links:
                link = link.lower()
                if not any(link.endswith(ext) for ext in self.BAD_EXTS):
                    newlinks.append(link)
            links = newlinks

        logger.info("Got %d/%d links...", len(links), total_links)
        if len(links) > 20:
            logger.info("Too many links, only checking %d", self.MAX_FILES)
            links = links[:self.MAX_FILES]

        # Try the links
        async def do_link(link):
            try:
                async with session.get(link) as resp:
                    mimetype = get_mimetype(resp)
                    if mimetype and mimetype not in self.GOOD_TYPES:
                        logger.info("Ignoring %s", mimetype)
                    return await self.check_file(resp)
            except aiohttp.ClientError:
                logger.info("Exception getting link %s", link)

        futures = []
        for link in links:
            futures.append(self.loop.create_task(
                do_link(link)
            ))
        for url in asyncio.as_completed(futures):
            try:
                url = await url
            except Exception:
                logger.exception("Exception processing link")
            else:
                if url:
                    await self.dataset_found(url, page)
        logger.info("URL processing done")

    async def check_file(self, resp):
        content = await resp.content.read(8192)
        lines = content.splitlines()
        if len(lines) <= 5:
            logger.info("File: got only %d lines?", len(lines))
            return
        if not lines[1].strip():
            del lines[1]
        commas = [line.count(b',') for line in lines[1:-1]]
        if not commas[1]:
            logger.info("File: no commas")
            return
        if any(c != commas[1] for c in commas):
            logger.info("File: inconsistent number of commas")
            return
        logger.info("File: is a CSV")
        return str(resp.url)

    async def dataset_found(self, url, page):
        raise NotImplementedError


BING_API_KEY = os.environ['BING_API_KEY']


async def bing_search(session, keywords):
    logger.info("Bing search: %s", keywords)
    async with session.get(
        'https://api.cognitive.microsoft.com/bing/v7.0/search',
        params={'q': keywords},
        headers={'Ocp-Apim-Subscription-Key': BING_API_KEY},
    ) as resp:
        data = await resp.json()

    results = data['webPages']['value']
    logger.info("Got %d/%d results", len(results),
                data['webPages']['totalEstimatedMatches'])
    return results