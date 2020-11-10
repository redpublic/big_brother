import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer
from models.serializer import ExtendedJsonSerializer


logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Reads messages from queue and sends them to kafka

    Methods
    -------

    async start()
        Start kafka producer and consume metrics from queue.

    async stop()
        Stop consuming metrics from queue
    """
    _is_running = False

    def __init__(self, kafka_config, queue):
        self.queue = queue
        self.producer = AIOKafkaProducer(bootstrap_servers=kafka_config['bootstrap_server'])

    async def start(self):
        logger.info('Kafka producer is starting')

        await self.producer.start()
        self._is_running = True

        logger.info('Kafka producer has started')

        while self._is_running:
            msg = await self.queue.get()
            serialized_msg = json.dumps(msg, cls=ExtendedJsonSerializer).encode()
            await self.producer.send_and_wait('aiven_metrics', serialized_msg)
        await self._stop()

    async def _stop(self):
        await self.producer.stop()
        logger.info('Kafka producer has stopped')

    async def stop(self):
        self._is_running = False
