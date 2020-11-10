import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from config import kafka_config
from models.metric import Metric


logger = logging.getLogger(__name__)


class MetricHandler:
    """
    Starts kafka consumer and database target.
    Converts kafka message to `Metric` object and passes metric object to database target.
    Database target saves metric object to database. Target object should implement `async handle_metric` method

    Methods
    -------

    async start()
        Starts consumer and target, then listens for events from consumer

    async stop()
        Stop consumer and target
    """
    def __init__(self, target, kafka_config):
        self.consumer = AIOKafkaConsumer(
            kafka_config['topic'],
            bootstrap_servers=kafka_config['bootstrap_server'],
            group_id=kafka_config['group_id'],
            auto_offset_reset='latest')
        self.target = target

    async def start(self):
        logger.info('Metric handler is starting')
        await self.consumer.start()
        await self.target.start()
        logger.info('Metric handler has started')

        try:
            async for msg in self.consumer:
                logger.debug(f'Metric handler has received message: {msg}')
                await self._handle_msg(msg)
        finally:
            await self.stop()

    async def stop(self):
        await self.consumer.stop()
        await self.target.stop()
        logger.info('Metric handler has stopped')

    async def _handle_msg(self, msg):
        metric = self._deserialize(msg)
        await self.target.handle_metric(metric)

    def _deserialize(self, msg):
        return Metric(**json.loads(msg.value.decode()))
