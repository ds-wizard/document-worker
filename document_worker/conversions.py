import itertools
import logging
import subprocess

from document_worker.formats import Format, Formats

# TODO: unify encoding across modules
DEFAULT_ENCODING = 'utf-8'
EXIT_SUCCESS = 0


def run_conversion(args: list, input: bytes, name: str,
                   source_format: Format, target_format: Format) -> bytes:
    p = subprocess.Popen(args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(input=input)
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
    # TODO: config

    SOURCE_FORMATS = [Formats.HTML]
    TARGET_FORMATS = [Formats.PDF]

    def __call__(self, source_format: Format, target_format: Format,
                data: bytes, metadata: dict) -> bytes:
        # TODO: detect and handle fails
        logging.info(f'Calling wkhtmltopdf to convert from {source_format} to {target_format}')
        return run_conversion(
            ['wkhtmltopdf', '--quiet', '--encoding', DEFAULT_ENCODING, '-', '-'],
            data, type(self).__name__, source_format, target_format
        )


class Pandoc:
    # TODO: config

    SOURCE_FORMATS = [Formats.HTML]
    TARGET_FORMATS = [Formats.LaTeX, Formats.RST, Formats.ODT,
                      Formats.DOCX, Formats.Markdown]

    def __call__(self, source_format: Format, target_format: Format,
                data: bytes, metadata: dict) -> bytes:
        # TODO: detect and handle fails
        logging.info(f'Calling pandoc to convert from {source_format} to {target_format}')
        return run_conversion(
            ['pandoc', '-s', '-f', 'html', '-t', target_format.name, '-o', '-'],
            data, type(self).__name__, source_format, target_format
        )


class FormatConvertor:

    CONVERTORS = [WkHtmlToPdf(), Pandoc()]

    def __init__(self):
        # TODO: config
        self.convertors = dict()
        for c in self.CONVERTORS:
            for conv in itertools.product(c.SOURCE_FORMATS, c.TARGET_FORMATS):
                self.convertors[conv] = c

    def can_convert(self, source_format: Format, target_format: Format):
        return source_format == target_format or (source_format, target_format) in self.convertors

    def convert(self, source_format: Format, target_format: Format,
                data: bytes, metadata: dict) -> bytes:
        if source_format == target_format:
            return data
        # TODO: detect and handle fails
        convertor = self.convertors[source_format, target_format]
        return convertor(source_format, target_format, data, metadata)
