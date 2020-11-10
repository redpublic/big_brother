import asyncio

from config import dbconfig, kafka_config

from consumer.dbtarget import DatabaseTarget
from consumer.handler import MetricHandler


async def main():
    dbtarget = DatabaseTarget(dbconfig)
    handler = MetricHandler(dbtarget, kafka_config)
    handler_task = handler.start()

    await asyncio.gather(handler_task)


if __name__ == '__main__':
    asyncio.run(main())
