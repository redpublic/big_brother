import asyncio
import asyncpg
import pytest
import time

from producer.producer import Producer
from consumer.dbtarget import DatabaseTarget
from consumer.handler import MetricHandler
from config import kafka_config, dbconfig


@pytest.fixture
async def consumer():
    dbtarget = DatabaseTarget(dbconfig)
    return MetricHandler(dbtarget, kafka_config)


@pytest.fixture
async def dbconn():
    dsn = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
        user=dbconfig['user'],
        password=dbconfig['password'],
        host=dbconfig['host'],
        port=dbconfig['port'],
        database=dbconfig['database'])
    conn = await asyncpg.connect(dsn)
    yield conn
    await conn.execute('DELETE FROM metrics')


@pytest.mark.asyncio
async def test_produce_success(consumer, dbconn):
    start = time.time()
    resources = {
        'google': {
            'host': 'https://google.com',
            'timeout': 3,
            'period': 1
        },
        'tutby': {
            'host': 'https://tut.by',
            'timeout': 3,
            'period': 1
        }
    }
    producer = Producer(resources, kafka_config)
    producer_task = asyncio.create_task(producer.start())
    consumer_task = asyncio.create_task(consumer.start())
    await asyncio.sleep(2)
    producer_task.cancel()
    consumer_task.cancel()
    end = time.time()
    metrics = await dbconn.fetch(
        'SELECT status, response_time, host, date_part(\'epoch\', timestamp) as timestamp FROM metrics')
    google_metrics = [metric for metric in metrics if metric['host'] == resources['google']['host']]
    tutby_metrics = [metric for metric in metrics if metric['host'] == resources['tutby']['host']]
    assert len(google_metrics) == 2
    assert len(tutby_metrics) == 2
    for metric in metrics:
        assert metric['timestamp'] < end
        assert metric['timestamp'] > start
        assert metric['status'] == 200


@pytest.mark.asyncio
async def test_produce_client_errors(consumer, dbconn):
    start = time.time()
    resources = {
        'unknown': {
            'host': 'unknownhost',
            'timeout': 3,
            'period': 1
        }
    }
    producer = Producer(resources, kafka_config)
    producer_task = asyncio.create_task(producer.start())
    consumer_task = asyncio.create_task(consumer.start())
    await asyncio.sleep(2)
    producer_task.cancel()
    consumer_task.cancel()
    end = time.time()
    metrics = await dbconn.fetch(
        'SELECT status, response_time, host, date_part(\'epoch\', timestamp) as timestamp FROM metrics')
    unknown_metrics = [metric for metric in metrics if metric['host'] == resources['unknown']['host']]
    assert len(unknown_metrics) == 2
    for metric in metrics:
        assert metric['timestamp'] < end
        assert metric['timestamp'] > start
        assert metric['status'] == 0


@pytest.mark.asyncio
async def test_produce_different_schedule(consumer, dbconn):
    start = time.time()
    resources = {
        'google': {
            'host': 'https://google.com',
            'timeout': 3,
            'period': 2
        },
        'tutby': {
            'host': 'https://tut.by',
            'timeout': 3,
            'period': 1
        }
    }
    producer = Producer(resources, kafka_config)
    producer_task = asyncio.create_task(producer.start())
    consumer_task = asyncio.create_task(consumer.start())
    await asyncio.sleep(2)
    producer_task.cancel()
    consumer_task.cancel()
    end = time.time()
    metrics = await dbconn.fetch(
        'SELECT status, response_time, host, date_part(\'epoch\', timestamp) as timestamp FROM metrics')
    google_metrics = [metric for metric in metrics if metric['host'] == resources['google']['host']]
    tutby_metrics = [metric for metric in metrics if metric['host'] == resources['tutby']['host']]
    assert len(google_metrics) == 1
    assert len(tutby_metrics) == 2
    for metric in metrics:
        assert metric['timestamp'] < end
        assert metric['timestamp'] > start
        assert metric['status'] == 200


@pytest.mark.asyncio
async def test_produce_different_timeout(consumer, dbconn):
    start = time.time()
    resources = {
        'google': {
            'host': 'https://google.com',
            'timeout': 0.1,
            'period': 2
        },
        'tutby': {
            'host': 'https://tut.by',
            'timeout': 3,
            'period': 1
        }
    }
    producer = Producer(resources, kafka_config)
    producer_task = asyncio.create_task(producer.start())
    consumer_task = asyncio.create_task(consumer.start())
    await asyncio.sleep(2)
    producer_task.cancel()
    consumer_task.cancel()
    end = time.time()
    metrics = await dbconn.fetch(
        'SELECT status, response_time, host, date_part(\'epoch\', timestamp) as timestamp FROM metrics')
    google_metrics = [metric for metric in metrics if metric['host'] == resources['google']['host']]
    tutby_metrics = [metric for metric in metrics if metric['host'] == resources['tutby']['host']]
    assert len(google_metrics) == 1
    assert len(tutby_metrics) == 2
    for metric in metrics:
        assert metric['timestamp'] < end
        assert metric['timestamp'] > start
    for metric in google_metrics:
        assert metric['status'] == 0
