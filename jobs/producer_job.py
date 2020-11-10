import asyncio

from config import resources, kafka_config

from producer.producer import Producer


async def main():
    provider = Producer(resources, kafka_config)
    await provider.start()


if __name__ == '__main__':
    asyncio.run(main())
