import json
import logging
import uuid

from document_worker.conversions import FormatConvertor
from document_worker.formats import Format, Formats
from document_worker.templates import TemplateRegistry


# TODO: unify encoding across modules
DEFAULT_ENCODING = 'utf-8'


class DocumentBuilderException(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class DocumentFile:

    def __init__(self, format: Format, content: bytes, encoding: str = DEFAULT_ENCODING):
        self.format = format
        self.content = content
        self.encoding = encoding

    def filename(self, name: str) -> str:
        return f'{name}.{self.format.file_extension}'


class DocumentBuilder:

    def __init__(self, template_registry: TemplateRegistry, format_convertor: FormatConvertor):
        self.template_registry = template_registry
        self.format_convertor = format_convertor

    def build_document(self, template_uuid: uuid.UUID, context: dict, target_format: Format):
        if target_format == Formats.JSON:
            return DocumentFile(
                target_format,
                json.dumps(context, indent=2, sort_keys=True).encode(DEFAULT_ENCODING)
            )
        if not self.template_registry.has_template(template_uuid):
            raise DocumentBuilderException(f'Template {template_uuid} not found')
        template = self.template_registry[template_uuid]
        source_format = template.output_format
        if not self.format_convertor.can_convert(source_format, target_format):
            raise DocumentBuilderException(f'Cannot convert from {source_format} to {target_format}')
        try:
            # TODO: consider retry
            base_doc = template.render(context).encode(encoding=DEFAULT_ENCODING, errors='ignore')
        except Exception as e:
            logging.debug('Handling exception when rendering template', exc_info=True)
            raise DocumentBuilderException(f'Failed to render document: {e}')
        try:
            # TODO: consider retry
            final_doc = self.format_convertor.convert(source_format, target_format, base_doc, template.metadata)
        except Exception as e:
            logging.debug('Handling exception when converting document', exc_info=True)
            raise DocumentBuilderException(f'Failed to convert document: {e}')
        return DocumentFile(target_format, final_doc)
