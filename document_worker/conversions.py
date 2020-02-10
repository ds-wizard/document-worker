import itertools
import logging
import shlex
import subprocess

from document_worker.formats import Format, Formats
from document_worker.config import DocumentWorkerConfig
from document_worker.consts import EXIT_SUCCESS, DEFAULT_ENCODING


def run_conversion(args: list, input: bytes, name: str,
                   source_format: Format, target_format: Format, timeout=None) -> bytes:
    command = ' '.join(args)
    logging.info(f'Calling "{command}" to convert from {source_format} to {target_format}')
    p = subprocess.Popen(args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(input=input, timeout=timeout)
    exit_code = p.returncode
    if exit_code != EXIT_SUCCESS:
        raise FormatConversionException(
            name, source_format, target_format,
            f'Failed to execute (exit code: {exit_code}): {stderr.decode(DEFAULT_ENCODING)}'
        )
    return stdout


class FormatConversionException(Exception):

    def __init__(self, convertor, source_format, target_format, message):
        self.convertor = convertor
        self.source_format = source_format
        self.target_format = target_format
        self.message = message


class WkHtmlToPdf:

    SOURCE_FORMATS = [Formats.HTML]
    TARGET_FORMATS = [Formats.PDF]

    ARGS1 = ['--quiet']
    ARGS2 = ['--encoding', DEFAULT_ENCODING, '-', '-']

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: Format, target_format: Format,
                 data: bytes, metadata: dict) -> bytes:
        template_args = self.extract_template_args(metadata)
        command = self.config.wkhtmltopdf.command + self.ARGS1 + template_args + self.ARGS2
        return run_conversion(
            command, data, type(self).__name__, source_format, target_format,
            timeout=self.config.wkhtmltopdf.timeout
        )

    @staticmethod
    def extract_template_args(metadata: dict):
        return shlex.split(metadata.get('wkhtmltopdf', ''))


class Pandoc:

    SOURCE_FORMATS = [Formats.HTML]
    TARGET_FORMATS = [Formats.LaTeX, Formats.RST, Formats.ODT,
                      Formats.DOCX, Formats.Markdown]

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: Format, target_format: Format,
                 data: bytes, metadata: dict) -> bytes:
        args = ['-f', 'html', '-t', target_format.name, '-o', '-']
        template_args = self.extract_template_args(metadata)
        command = self.config.pandoc.command + template_args + args
        return run_conversion(
            command, data, type(self).__name__, source_format, target_format,
            timeout=self.config.pandoc.timeout
        )

    @staticmethod
    def extract_template_args(metadata: dict):
        return shlex.split(metadata.get('pandoc', ''))


class FormatConvertor:

    CONVERTORS = [WkHtmlToPdf(), Pandoc()]

    def __init__(self, config: DocumentWorkerConfig):
        self.convertors = dict()
        for c in self.CONVERTORS:
            c.config = config
            for conv in itertools.product(c.SOURCE_FORMATS, c.TARGET_FORMATS):
                self.convertors[conv] = c

    def can_convert(self, source_format: Format, target_format: Format):
        return source_format == target_format or (source_format, target_format) in self.convertors

    def convert(self, source_format: Format, target_format: Format,
                data: bytes, metadata: dict) -> bytes:
        if source_format == target_format:
            return data
        convertor = self.convertors[source_format, target_format]
        return convertor(source_format, target_format, data, metadata)
