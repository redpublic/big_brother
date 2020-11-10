import logging.config

# Http checkers config
resources = {
    'local': {
        'host': 'unknownhost',
        'timeout': 3,
        'period': 5
    },
    'google': {
        'host': 'http://google.com',
        'timeout': 3,
        'period': 2
    },
    'tutby': {
        'host': 'http://tut.by',
        'timeout': 3,
        'period': 10
    }
}

# Kafka config
kafka_config = {
    'topic': 'aiven_metrics',
    'bootstrap_server': 'localhost:29092',
    'group_id': 'aiven_metrics_consumer_3'
}

# Database
dbconfig = {
    'user': 'aiven',
    'host': 'localhost',
    'password': '',
    'port': '5432',
    'database': 'aiven'
}

# Logger
loglevel = 'DEBUG'
logconfig = {
    'version': 1,
    'loggers': {
        'consumer': {
            'level': loglevel
        },
        'producer': {
            'level': loglevel
        },
    }
}
logging.config.dictConfig(logconfig)
