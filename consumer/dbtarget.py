import asyncio
import asyncpg
import logging

from datetime import datetime
from config import dbconfig


logger = logging.getLogger(__name__)


class DatabaseTarget:
    """
    Class to store metrics to database.

    Methods
    -------

    async start()
        Creates database connection

    async stop()
        Closes database connection

    async handle_metric(metric)
        Callback which will be invoked by MetricHandler
    """
    def __init__(self, dbconfig):
        self.dbconfig = dbconfig

    async def start(self):
        logger.info('Database client is starting')
        dsn = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=self.dbconfig['user'],
            password=self.dbconfig['password'],
            host=self.dbconfig['host'],
            port=self.dbconfig['port'],
            database=self.dbconfig['database'])
        self.conn = await asyncpg.connect(dsn)


    async def stop(self):
        logger.info('Database client has stopped')
        await self.conn.close()

    async def handle_metric(self, metric):
        await self.conn.execute('''
            INSERT INTO metrics(status, response_time, host, timestamp)
            VALUES($1, $2, $3, to_timestamp($4) at time zone 'UTC')''',
            metric.status,
            metric.response_time,
            metric.host,
            metric.timestamp)
