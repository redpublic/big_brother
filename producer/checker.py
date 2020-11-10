import asyncio
import aiohttp
import time
import logging

from models.metric import Metric


logger = logging.getLogger(__name__)


class HttpChecker:
    """
    Makes http request to configured resource and emits Metric object to queue

    Methods
    -------

    run()
        Callback for ScheduledTask
    """
    def __init__(self, name, host, timeout, queue):
        self.name = name
        self.host = host
        self.timeout = timeout
        self.queue = queue

    async def run(self):
        async with aiohttp.ClientSession() as session:
            logger.debug(f'Make request to {self.host}')
            start = time.time()
            try:
                async with session.get(self.host, timeout=self.timeout) as response:
                    end = time.time()
                    metric = Metric(
                        status=response.status,
                        response_time=int((end - start) * 1000),
                        host=self.host,
                        timestamp=end)
                    self.queue.put_nowait(metric)
            except Exception as e:
                logging.info(f'Error on request {self.host}: {e}')
                metric = Metric(
                    status=0,
                    response_time=0,
                    host=self.host,
                    timestamp=time.time())
                self.queue.put_nowait(metric)
