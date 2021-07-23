import pathlib
import rdflib
import shlex
import subprocess

from document_worker.config import DocumentWorkerConfig
from document_worker.consts import EXIT_SUCCESS, DEFAULT_ENCODING
from document_worker.context import Context
from document_worker.documents import FileFormat, FileFormats


def run_conversion(*, args: list, workdir: str, input_data: bytes, name: str,
                   source_format: FileFormat, target_format: FileFormat, timeout=None) -> bytes:
    command = ' '.join(args)
    Context.logger.info(f'Calling "{command}" to convert from {source_format} to {target_format}')
    p = subprocess.Popen(args,
                         cwd=workdir,
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

    ARGS1 = ['--quiet', '--load-error-handling', 'ignore']
    ARGS2 = ['--encoding', DEFAULT_ENCODING, '-', '-']

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: FileFormat, target_format: FileFormat,
                 data: bytes, metadata: dict, workdir: str) -> bytes:
        config_args = shlex.split(self.config.wkhtmltopdf.args)
        template_args = self.extract_template_args(metadata)
        args_access = ['--disable-local-file-access', '--allow', workdir]
        args = self.ARGS1 + template_args + config_args + args_access + self.ARGS2
        command = self.config.wkhtmltopdf.command + args
        return run_conversion(
            args=command,
            workdir=workdir,
            input_data=data,
            name=type(self).__name__,
            source_format=source_format,
            target_format=target_format,
            timeout=self.config.wkhtmltopdf.timeout,
        )

    @staticmethod
    def extract_template_args(metadata: dict):
        return shlex.split(metadata.get('args', ''))


class Pandoc:

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: FileFormat, target_format: FileFormat,
                 data: bytes, metadata: dict, workdir: str) -> bytes:
        args = ['-f', source_format.name, '-t', target_format.name, '-o', '-']
        config_args = shlex.split(self.config.pandoc.args)
        template_args = self.extract_template_args(metadata)
        command = self.config.pandoc.command + template_args + config_args + args
        return run_conversion(
            args=command,
            workdir=workdir,
            input_data=data,
            name=type(self).__name__,
            source_format=source_format,
            target_format=target_format,
            timeout=self.config.pandoc.timeout,
        )

    @staticmethod
    def extract_template_args(metadata: dict):
        return shlex.split(metadata.get('args', ''))


class Prince:

    ARGS = ['-', '-o', '-']

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: FileFormat, target_format: FileFormat,
                 data: bytes, metadata: dict, workdir: str) -> bytes:
        config_args = shlex.split(self.config.prince.args)
        template_args = self.extract_template_args(metadata)
        args = self.ARGS + template_args + config_args
        command = self.config.prince.command + args
        return run_conversion(
            args=command,
            workdir=workdir,
            input_data=data,
            name=type(self).__name__,
            source_format=source_format,
            target_format=target_format,
            timeout=self.config.prince.timeout,
        )

    @staticmethod
    def extract_template_args(metadata: dict):
        return shlex.split(metadata.get('args', ''))


class Relaxed:

    SOURCE_FILENAME = '/tmp/docworker/document.html'
    TARGET_FILENAME = '/tmp/docworker/document.pdf'
    ARGS = [SOURCE_FILENAME, '--no-sandbox', '--build-once', TARGET_FILENAME]

    def __init__(self, config: DocumentWorkerConfig = None):
        self.config = config

    def __call__(self, source_format: FileFormat, target_format: FileFormat,
                 data: bytes, metadata: dict, workdir: str) -> bytes:
        config_args = shlex.split(self.config.relaxed.args)
        template_args = self.extract_template_args(metadata)
        args = self.ARGS + template_args + config_args
        command = self.config.relaxed.command + args
        pathlib.Path(self.SOURCE_FILENAME).write_bytes(data)
        run_conversion(
            args=command,
            workdir=workdir,
            input_data=data,
            name=type(self).__name__,
            source_format=source_format,
            target_format=target_format,
            timeout=self.config.relaxed.timeout,
        )
        # TODO: check if OK, otherwise the file does not exist
        return pathlib.Path(self.TARGET_FILENAME).read_bytes()

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
