import configparser
import logging
import pika
import shlex
import yaml
from typing import List

from document_worker.consts import DocumentNamingStrategy


class MissingConfigurationError(Exception):

    def __init__(self, missing: List[str]):
        self.missing = missing


class MongoConfig:

    def __init__(self, host: str, port: int, username: str, password: str,
                 database: str, collection: str, fs_collection: str,
                 templates_collection: str, assets_fs_collection: str,
                 auth_database: str, auth_mechanism: str):
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.fs_collection = fs_collection
        self.templates_collection = templates_collection
        self.assets_fs_collection = assets_fs_collection
        self.username = username
        self.password = password
        self.auth_database = auth_database
        self.auth_mechanism = auth_mechanism

    def __str__(self):
        return f'MongoConfig\n' \
               f'- host = {self.host} ({type(self.host)})\n' \
               f'- port = {self.port} ({type(self.port)})\n' \
               f'- database = {self.database} ({type(self.database)})\n' \
               f'- collection = {self.collection} ({type(self.collection)})\n' \
               f'- fs_collection = {self.fs_collection} ({type(self.fs_collection)})\n' \
               f'- templates_collection = {self.templates_collection} ({type(self.templates_collection)})\n' \
               f'- assets_fs_collection = {self.assets_fs_collection} ({type(self.assets_fs_collection)})\n' \
               f'- username = {self.username} ({type(self.username)})\n' \
               f'- password = {self.password} ({type(self.password)})\n' \
               f'- auth_database = {self.auth_database} ({type(self.auth_database)})\n' \
               f'- auth_mechanism = {self.auth_mechanism} ({type(self.auth_mechanism)})\n'

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
        self.queue = queue
        self.username = username
        self.password = password

    def __str__(self):
        return f'MQueueConfig\n' \
               f'- host = {self.host} ({type(self.host)})\n' \
               f'- port = {self.port} ({type(self.port)})\n' \
               f'- vhost = {self.vhost} ({type(self.vhost)})\n' \
               f'- queue = {self.queue} ({type(self.queue)})\n' \
               f'- username = {self.username} ({type(self.username)})\n' \
               f'- password = {self.password} ({type(self.password)})\n'

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

    def __init__(self, level, message_format: str):
        self.level = level
        self.message_format = message_format

    def __str__(self):
        return f'MQueueConfig\n' \
               f'- level = {self.level} ({type(self.level)})\n' \
               f'- message_format = {self.message_format} ({type(self.message_format)})\n'


class DocumentsConfig:

    def __init__(self, naming_strategy: str):
        self.naming_strategy = DocumentNamingStrategy.get(naming_strategy)

    def __str__(self):
        return f'DocumentsConfig\n' \
               f'- naming_strategy = {self.naming_strategy}\n'


class CommandConfig:

    def __init__(self, executable: str, args: str, timeout: float):
        self.executable = executable
        self.args = args
        self.timeout = timeout

    @property
    def command(self) -> List[str]:
        return [self.executable] + shlex.split(self.args)

    def __str__(self):
        return f'CommandConfig\n' \
               f'- executable = {self.executable} ({type(self.executable)})\n' \
               f'- args = {self.args} ({type(self.args)})\n' \
               f'- timeout = {self.timeout} ({type(self.timeout)})\n'


class DocumentWorkerConfig:

    def __init__(self, cfg_parser):
        self.mongo = cfg_parser.mongo  # type: MongoConfig
        self.mq = cfg_parser.mq  # type: MQueueConfig
        self.logging = cfg_parser.logging  # type: LoggingConfig
        self.documents = cfg_parser.documents  # type: DocumentsConfig
        self.pandoc = cfg_parser.pandoc  # type: CommandConfig
        self.wkhtmltopdf = cfg_parser.wkhtmltopdf  # type: CommandConfig

    def __str__(self):
        return f'{str(self.mongo)}\n' \
               f'{str(self.mq)}\n' \
               f'{str(self.logging)}\n' \
               f'{str(self.documents)}\n' \
               f'{str(self.pandoc)}\n' \
               f'{str(self.wkhtmltopdf)}\n'


class DocumentWorkerYMLConfigParser:

    MONGO_SECTION = 'mongo'
    MONGO_AUTH_SUBSECTION = 'auth'
    MQ_SECTION = 'mq'
    MQ_AUTH_SUBSECTION = 'auth'
    LOGGING_SECTION = 'logging'
    DOCS_SECTION = 'documents'
    DOCS_NAMING_SUBSECTION = 'naming'
    EXTERNAL_SECTION = 'externals'
    PANDOC_SUBSECTION = 'pandoc'
    WKHTMLTOPDF_SUBSECTION = 'wkhtmltopdf'

    DEFAULTS = {
        MONGO_SECTION: {
            'host': 'localhost',
            'port': 27017,
            'collection': 'documents',
            'fs_collection': 'documentFs',
            'templates_collection': 'templates',
            'assets_fs_collection': 'templateAssetFs',
            MONGO_AUTH_SUBSECTION: {
                'username': None,
                'password': None,
                'database': None,
                'mechanism': 'SCRAM-SHA-256'
            },
        },
        MQ_SECTION: {
            'host': 'localhost',
            'port': 5672,
            'vhost': '/',
            'queue': 'document.generation',
            MQ_AUTH_SUBSECTION: {
                'username': None,
                'password': None,
            },
        },
        LOGGING_SECTION: {
            'level': 'WARNING',
            'format': '%(asctime)s | %(levelname)s | %(module)s: %(message)s',
        },
        DOCS_SECTION: {
            DOCS_NAMING_SUBSECTION: {
                'strategy': 'sanitize'
            }
        },
        EXTERNAL_SECTION: {
            PANDOC_SUBSECTION: {
                'executable': 'pandoc',
                'args': '--standalone',
                'timeout': None,
            },
            WKHTMLTOPDF_SUBSECTION: {
                'executable': 'wkhtmltopdf',
                'args': '',
                'timeout': None,
            },
        },
    }

    REQUIRED = [
        ['mongo', 'database']
    ]

    def __init__(self):
        self.cfg = dict()

    @staticmethod
    def can_read(content):
        try:
            yaml.load(content, Loader=yaml.FullLoader)
            return True
        except Exception:
            return False

    def read_file(self, fp):
        self.cfg = yaml.load(fp, Loader=yaml.FullLoader)

    def read_string(self, content):
        self.cfg = yaml.load(content, Loader=yaml.FullLoader)

    def has(self, *path):
        x = self.cfg
        for p in path:
            if not hasattr(x, 'keys') or p not in x.keys():
                return False
            x = x[p]
        return True

    def _get_default(self, *path):
        x = self.DEFAULTS
        for p in path:
            x = x[p]
        return x

    def get_or_default(self, *path):
        x = self.cfg
        for p in path:
            if not hasattr(x, 'keys') or p not in x.keys():
                return self._get_default(*path)
            x = x[p]
        return x

    def validate(self):
        missing = []
        for path in self.REQUIRED:
            if not self.has(*path):
                missing.append('.'.join(path))
        if len(missing) > 0:
            raise MissingConfigurationError(missing)

    @property
    def mongo(self) -> MongoConfig:
        return MongoConfig(
            host=self.get_or_default(self.MONGO_SECTION, 'host'),
            port=self.get_or_default(self.MONGO_SECTION, 'port'),
            database=self.get_or_default(self.MONGO_SECTION, 'database'),
            collection=self.get_or_default(self.MONGO_SECTION, 'collection'),
            fs_collection=self.get_or_default(self.MONGO_SECTION, 'fs_collection'),
            templates_collection=self.get_or_default(self.MONGO_SECTION, 'templates_collection'),
            assets_fs_collection=self.get_or_default(self.MONGO_SECTION, 'assets_fs_collection'),
            username=self.get_or_default(self.MONGO_SECTION, self.MONGO_AUTH_SUBSECTION, 'username'),
            password=self.get_or_default(self.MONGO_SECTION, self.MONGO_AUTH_SUBSECTION, 'password'),
            auth_database=self.get_or_default(self.MONGO_SECTION, self.MONGO_AUTH_SUBSECTION, 'database'),
            auth_mechanism=self.get_or_default(self.MONGO_SECTION, self.MONGO_AUTH_SUBSECTION, 'mechanism'),
        )

    @property
    def mq(self) -> MQueueConfig:
        return MQueueConfig(
            host=self.get_or_default(self.MQ_SECTION, 'host'),
            port=self.get_or_default(self.MQ_SECTION, 'port'),
            vhost=self.get_or_default(self.MQ_SECTION, 'vhost'),
            queue=self.get_or_default(self.MQ_SECTION, 'queue'),
            username=self.get_or_default(self.MQ_SECTION, self.MQ_AUTH_SUBSECTION, 'username'),
            password=self.get_or_default(self.MQ_SECTION, self.MQ_AUTH_SUBSECTION, 'password'),
        )

    @property
    def logging(self) -> LoggingConfig:
        return LoggingConfig(
            level=self.get_or_default(self.LOGGING_SECTION, 'level'),
            message_format=self.get_or_default(self.LOGGING_SECTION, 'format'),
        )

    @property
    def documents(self) -> DocumentsConfig:
        return DocumentsConfig(
            naming_strategy=self.get_or_default(self.DOCS_SECTION, self.DOCS_NAMING_SUBSECTION, 'strategy')
        )

    def _command_config(self, *path: str) -> CommandConfig:
        return CommandConfig(
            executable=self.get_or_default(*path, 'executable'),
            args=self.get_or_default(*path, 'args'),
            timeout=self.get_or_default(*path, 'timeout'),
        )

    @property
    def pandoc(self) -> CommandConfig:
        return self._command_config(self.EXTERNAL_SECTION, self.PANDOC_SUBSECTION)

    @property
    def wkhtmltopdf(self) -> CommandConfig:
        return self._command_config(self.EXTERNAL_SECTION, self.WKHTMLTOPDF_SUBSECTION)


class DocumentWorkerCFGConfigParser(configparser.ConfigParser):

    MONGO_SECTION = 'mongo'
    MQ_SECTION = 'mq'
    LOGGING_SECTION = 'logging'
    DOCS_SECTION = 'documents'
    PANDOC_SECTION = 'pandoc'
    WKHTMLTOPDF_SECTION = 'wkhtmltopdf'

    DEFAULTS = {
        MONGO_SECTION: {
            'host': 'localhost',
            'port': 27017,
            'username': None,
            'password': None,
            'collection': 'documents',
            'fs_collection': 'documentFs',
            'templates_collection': 'templates',
            'assets_fs_collection': 'templateAssetFs',
            'auth_database': None,
            'auth_mechanism': 'SCRAM-SHA-256'
        },
        MQ_SECTION: {
            'host': 'localhost',
            'port': 5672,
            'vhost': '/',
            'username': None,
            'password': None,
            'queue': 'document.generation',
        },
        LOGGING_SECTION: {
            'level': logging.WARNING,
            'format': '%(asctime)s | %(levelname)s | %(module)s: %(message)s',
        },
        DOCS_SECTION: {
            'naming_strategy': 'sanitize'
        },
        PANDOC_SECTION: {
            'executable': 'pandoc',
            'args': '--standalone',
            'timeout': None,
        },
        WKHTMLTOPDF_SECTION: {
            'executable': 'wkhtmltopdf',
            'args': '',
            'timeout': None,
        },
    }

    REQUIRED = {
        MONGO_SECTION: ['database'],
    }

    def __init__(self):
        super().__init__()

    @staticmethod
    def can_read(fp):
        try:
            cfg = configparser.ConfigParser()
            cfg.read_string(fp)
            return True
        except Exception:
            return False

    def empty_option(self, section: str, option: str) -> bool:
        return self.get(section, option, fallback='') == ''

    def validate(self):
        missing = []
        for section, options in self.REQUIRED.items():
            for option in options:
                if self.empty_option(section, option):
                    missing.append(f'{section}: {option}')
        if len(missing) > 0:
            raise MissingConfigurationError(missing)

    def get_or_default(self, section: str, option: str) -> str:
        if section in self.DEFAULTS and option in self.DEFAULTS[section]:
            return self.get(section, option, fallback=self.DEFAULTS[section][option])
        return self.get(section, option)

    def getint_or_default(self, section: str, option: str) -> int:
        if section in self.DEFAULTS and option in self.DEFAULTS[section]:
            return self.getint(section, option, fallback=self.DEFAULTS[section][option])
        return self.getint(section, option)

    def getfloat_or_default(self, section: str, option: str) -> float:
        if section in self.DEFAULTS and option in self.DEFAULTS[section]:
            return self.getfloat(section, option, fallback=self.DEFAULTS[section][option])
        return self.getfloat(section, option)

    @property
    def mongo(self) -> MongoConfig:
        return MongoConfig(
            host=self.get_or_default(self.MONGO_SECTION, 'host'),
            port=self.getint_or_default(self.MONGO_SECTION, 'port'),
            database=self.get_or_default(self.MONGO_SECTION, 'database'),
            collection=self.get_or_default(self.MONGO_SECTION, 'collection'),
            fs_collection=self.get_or_default(self.MONGO_SECTION, 'fs_collection'),
            templates_collection=self.get_or_default(self.MONGO_SECTION, 'templates_collection'),
            assets_fs_collection=self.get_or_default(self.MONGO_SECTION, 'assets_fs_collection'),
            username=self.get_or_default(self.MONGO_SECTION, 'username'),
            password=self.get_or_default(self.MONGO_SECTION, 'password'),
            auth_database=self.get_or_default(self.MONGO_SECTION, 'auth_database'),
            auth_mechanism=self.get_or_default(self.MONGO_SECTION, 'auth_mechanism'),
        )

    @property
    def mq(self) -> MQueueConfig:
        return MQueueConfig(
            host=self.get_or_default(self.MQ_SECTION, 'host'),
            port=self.getint_or_default(self.MQ_SECTION, 'port'),
            vhost=self.get_or_default(self.MQ_SECTION, 'vhost'),
            queue=self.get_or_default(self.MQ_SECTION, 'queue'),
            username=self.get_or_default(self.MQ_SECTION, 'username'),
            password=self.get_or_default(self.MQ_SECTION, 'password'),
        )

    @property
    def logging(self) -> LoggingConfig:
        return LoggingConfig(
            level=self.get_or_default(self.LOGGING_SECTION, 'level'),
            message_format=self.get_or_default(self.LOGGING_SECTION, 'format'),
        )

    @property
    def documents(self) -> DocumentsConfig:
        return DocumentsConfig(
            naming_strategy=self.get_or_default(self.DOCS_SECTION, 'naming_strategy')
        )

    def _command_config(self, section: str) -> CommandConfig:
        return CommandConfig(
            executable=self.get_or_default(section, 'executable'),
            args=self.get_or_default(section, 'args'),
            timeout=self.getfloat_or_default(section, 'timeout'),
        )

    @property
    def pandoc(self) -> CommandConfig:
        return self._command_config(self.PANDOC_SECTION)

    @property
    def wkhtmltopdf(self) -> CommandConfig:
        return self._command_config(self.WKHTMLTOPDF_SECTION)
