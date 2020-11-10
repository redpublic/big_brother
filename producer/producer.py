import asyncio
import time
import logging

from .checker import HttpChecker
from .scheduled_task import ScheduledTask
from .kafka_producer import KafkaProducer


logger = logging.getLogger(__name__)


class Producer:
    """
    Starts scheduled http clients and kafka producer. Clients communicate with kafka producer via queue.

    Methods
    -------

    async start()
        Start http checkers and kafka producer
    """
    def __init__(self, resources, kafka_config):
        self.resources = resources
        self.kafka_config = kafka_config

    async def start(self):
        queue = asyncio.Queue()

        runnables = []
        for name, config in self.resources.items():
            timeout = config['timeout']
            period = config['period']
            host = config['host']
            checker = HttpChecker(name, host, timeout, queue)
            task = ScheduledTask(checker, period=period)
            runnable = task.start()
            runnables.append(runnable)

        kafka_producer = KafkaProducer(self.kafka_config, queue)
        kafka_producer_task = kafka_producer.start()
        runnables.append(kafka_producer_task)

        await asyncio.gather(*runnables)
