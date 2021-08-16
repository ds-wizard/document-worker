import logging

from typing import Any, Dict

from document_worker.consts import LOGGER_NAME


class DocWorkerLogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'traceId'):
            record.traceId = '-'
        if not hasattr(record, 'documentId'):
            record.traceId = '-'
        return True


class DocWorkerLogger(logging.Logger):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addFilter(DocWorkerLogFilter())


class _DocWorkerLoggerWrapper:

    ATTR_MAP = {
        'trace_id': 'traceId',
        'document_id': 'documentId',
    }

    def __init__(self, trace_id: str, document_id: str):
        self._extra = dict()  # type: Dict[str, Any]
        self._logger = logging.getLogger(LOGGER_NAME)
        self.trace_id = trace_id
        self.document_id = document_id

    def __setattr__(self, name: str, value: Any):
        if name in self.ATTR_MAP.keys():
            self._extra[self.ATTR_MAP[name]] = value
        else:
            super().__setattr__(name, value)

    def __getattr__(self, name: str):
        if name in self.ATTR_MAP.keys():
            return self._extra[self.ATTR_MAP[name]]
        else:
            return super().__getattribute__(name)

    def _log(self, level: int, message: str, **kwargs):
        self._logger.log(level=level, msg=message, extra=self._extra, **kwargs)

    def log(self, *args, **kwargs):
        self._logger.log(*args, extra=self._extra, **kwargs)

    def debug(self, message: str, **kwargs):
        self._log(level=logging.DEBUG, message=message, **kwargs)

    def info(self, message: str, **kwargs):
        self._log(level=logging.INFO, message=message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log(level=logging.WARNING, message=message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log(level=logging.ERROR, message=message, **kwargs)

    def set_level(self, level: str):
        self._logger.setLevel(level)


LOGGER = _DocWorkerLoggerWrapper(
    trace_id='-',
    document_id='-',
)
