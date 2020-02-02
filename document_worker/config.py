import configparser
import logging
import pika
from typing import List, Tuple


class MissingConfigurationError(Exception):

    def __init__(self, missing: List[Tuple[str, str]]):
        self.missing = missing


class MongoConfig:

    def __init__(self, host: str, port: int, username: str, password: str,
                 database: str, collection: str, fs_collection: str,
                 auth_database: str, auth_mechanism: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.collection = collection
        self.fs_collection = fs_collection
        self.auth_database = auth_database
        self.auth_mechanism = auth_mechanism

    @property
    def mongo_client_kwargs(self):
        kwargs = {
            'host': self.host, 'port': self.port
        }
        if self.auth_enabled:
            kwargs['username'] = self.username
            kwargs['password'] = self.password
            kwargs['authSource'] = self.auth_database or self.database
            kwargs['authMechanism'] = self.auth_mechanism

        return kwargs

    @property
    def auth_enabled(self):
        return self.username is not None and self.password is not None


class MQueueConfig:

    def __init__(self, host: str, port: int, vhost: str, username: str, password: str, queue: str):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.username = username
        self.password = password
        self.queue = queue

    @property
    def auth_enabled(self):
        return self.username is not None or self.password is not None

    @property
    def connection_parameters(self) -> pika.ConnectionParameters:
        conn_params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost
        )
        if self.auth_enabled:
            conn_params.credentials = pika.credentials.PlainCredentials(
                username=self.username,
                password=self.password,
            )
        return conn_params


class LoggingConfig:

    def __init__(self, level: int, message_format: str):
        self.level = level
        self.message_format = message_format


class DocumentWorkerConfig(configparser.ConfigParser):

    MONGO_SECTION = 'mongo'
    MQ_SECTION = 'mq'
    LOGGING_SECTION = 'logging'

    DEFAULTS = {
        MONGO_SECTION: {
            'host': 'localhost',
            'port': 27017,
            'username': None,
            'password': None,
            'fs_collection': 'fs',
            'auth_database': None,
            'auth_mechanism': 'SCRAM-SHA-256'
        },
        MQ_SECTION: {
            'host': 'localhost',
            'port': 5672,
            'vhost': '/',
            'username': None,
            'password': None,
        },
        LOGGING_SECTION: {
            'level': logging.WARNING,
            'format': '%(asctime)s | %(levelname)s | %(module)s: %(message)s',
        }
    }

    REQUIRED = {
        MONGO_SECTION: ['database', 'collection'],
        MQ_SECTION: ['queue'],
    }

    def __init__(self):
        super().__init__()

    def empty_option(self, section: str, option: str) -> bool:
        return self.get(section, option, fallback='') == ''

    def validate(self):
        missing = []
        for section, options in self.REQUIRED.items():
            for option in options:
                if self.empty_option(section, option):
                    missing.append((section, option))
        if len(missing) > 0:
            raise MissingConfigurationError(missing)

    def get_or_default(self, section: str, option: str) -> str:
        if section in self.DEFAULTS and option in self.DEFAULTS[section]:
            return self.get(section, option, fallback=self.DEFAULTS[section][option])
        return self.get(section, option)

    def getint_or_default(self, section: str, option: str) -> int:
        if section in self.DEFAULTS and option in self.DEFAULTS[section]:
            return self.get(section, option, fallback=self.DEFAULTS[section][option])
        return self.get(section, option)

    @property
    def mongo(self) -> MongoConfig:
        return MongoConfig(
            self.get_or_default(self.MONGO_SECTION, 'host'),
            self.getint_or_default(self.MONGO_SECTION, 'port'),
            self.get_or_default(self.MONGO_SECTION, 'username'),
            self.get_or_default(self.MONGO_SECTION, 'password'),
            self.get_or_default(self.MONGO_SECTION, 'database'),
            self.get_or_default(self.MONGO_SECTION, 'collection'),
            self.get_or_default(self.MONGO_SECTION, 'fs_collection'),
            self.get_or_default(self.MONGO_SECTION, 'auth_database'),
            self.get_or_default(self.MONGO_SECTION, 'auth_mechanism'),
        )

    @property
    def mq(self) -> MQueueConfig:
        return MQueueConfig(
            self.get_or_default(self.MQ_SECTION, 'host'),
            self.getint_or_default(self.MQ_SECTION, 'port'),
            self.get_or_default(self.MQ_SECTION, 'vhost'),
            self.get_or_default(self.MQ_SECTION, 'username'),
            self.get_or_default(self.MQ_SECTION, 'password'),
            self.get_or_default(self.MQ_SECTION, 'queue'),
        )

    @property
    def logging(self) -> LoggingConfig:
        return LoggingConfig(
            self.getint_or_default(self.LOGGING_SECTION, 'level'),
            self.get_or_default(self.LOGGING_SECTION, 'format'),
        )
