DEFAULT_ENCODING = 'utf-8'
EXIT_SUCCESS = 0
VERSION = '3.6.1'
PROG_NAME = 'docworker'
LOGGER_NAME = 'docworker'
CURRENT_METAMODEL = 7
NULL_UUID = '00000000-0000-0000-0000-000000000000'


class DocumentState:
    QUEUED = 'QueuedDocumentState'
    PROCESSING = 'InProgressDocumentState'
    FAILED = 'ErrorDocumentState'
    FINISHED = 'DoneDocumentState'


class TemplateAssetField:
    UUID = 'uuid'
    FILENAME = 'fileName'
    CONTENT_TYPE = 'contentType'


class FormatField:
    UUID = 'uuid'
    NAME = 'name'
    STEPS = 'steps'


class StepField:
    NAME = 'name'
    OPTIONS = 'options'


class DocumentNamingStrategy:
    UUID = 'uuid'
    SANITIZE = 'sanitize'
    SLUGIFY = 'slugify'

    _DEFAULT = SANITIZE
    _NAMES = {
        'uuid': UUID,
        'sanitize': SANITIZE,
        'slugify': SLUGIFY,
    }

    @classmethod
    def get(cls, name: str):
        return cls._NAMES.get(name.lower(), cls._DEFAULT)
