import logging
import rdflib
import shlex
import subprocess

from document_worker.config import DocumentWorkerConfig
from document_worker.consts import EXIT_SUCCESS, DEFAULT_ENCODING
from document_worker.documents import FileFormat, FileFormats


def run_conversion(args: list, input_data: bytes, name: str,
                   source_format: FileFormat, target_format: FileFormat, timeout=None) -> bytes:
    command = ' '.join(args)
    logging.info(f'Calling "{command}" to convert from {source_format} to {target_format}')
    p = subprocess.Popen(args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(input=input_data, timeout=timeout)
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

    ARGS1 = ['--quiet']
    ARGS2 = ['--encoding', DEFAULT_ENCODING, '-', '-']

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: FileFormat, target_format: FileFormat,
                 data: bytes, metadata: dict) -> bytes:
        template_args = self.extract_template_args(metadata)
        command = self.config.wkhtmltopdf.command + self.ARGS1 + template_args + self.ARGS2
        return run_conversion(
            command, data, type(self).__name__, source_format, target_format,
            timeout=self.config.wkhtmltopdf.timeout
        )

    @staticmethod
    def extract_template_args(metadata: dict):
        return shlex.split(metadata.get('args', ''))


class Pandoc:

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: FileFormat, target_format: FileFormat,
                 data: bytes, metadata: dict) -> bytes:
        args = ['-f', source_format.name, '-t', target_format.name, '-o', '-']
        template_args = self.extract_template_args(metadata)
        command = self.config.pandoc.command + template_args + args
        return run_conversion(
            command, data, type(self).__name__, source_format, target_format,
            timeout=self.config.pandoc.timeout
        )

    @staticmethod
    def extract_template_args(metadata: dict):
        return shlex.split(metadata.get('args', ''))


class RdfLibConvert:

    FORMATS = {
        FileFormats.RDF_XML: 'xml',
        FileFormats.N3: 'n3',
        FileFormats.NTRIPLES: 'ntriples',
        FileFormats.TURTLE: 'turtle',
        FileFormats.TRIG: 'trig',
        FileFormats.JSONLD: 'json-ld',
    }

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: FileFormat, target_format: FileFormat,
                 data: bytes, metadata: dict) -> bytes:
        g = rdflib.Graph().parse(
            data=data.decode(DEFAULT_ENCODING),
            format=self.FORMATS.get(source_format)
        )
        result = g.serialize(
            format=self.FORMATS.get(target_format)
        )
        return result
