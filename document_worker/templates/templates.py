import json
import logging
import os
import uuid
import typing

from document_worker.files import DocumentFile
from document_worker.templates.formats import Format


class TemplateException(Exception):

    def __init__(self, template_metafile: str, template_uuid: uuid.UUID, message: str):
        self.template_metafile = template_metafile
        self.template_uuid = template_uuid
        self.message = message


class TemplateMetaField:
    UUID = 'uuid'
    NAME = 'name'
    FORMATS = 'formats'


class Template:

    META_REQUIRED = [TemplateMetaField.UUID,
                     TemplateMetaField.NAME,
                     TemplateMetaField.FORMATS]

    def __init__(self, config, meta_file: str):
        self.config = config
        self.meta_file = meta_file
        self.uuid = None
        logging.info(f'Loading metadata from {meta_file}')
        self.metadata = self._load_metadata()
        logging.info(f'Verifying metadata from {meta_file}')
        self._verify_metadata()
        self.uuid = uuid.UUID(self.metadata[TemplateMetaField.UUID])
        self.name = self.metadata[TemplateMetaField.NAME]
        self.basedir = os.path.dirname(meta_file)
        logging.info(f'Setting up formats for template "{self.name}" ({self.uuid})')
        self.formats = self._load_formats()

    def raise_exc(self, message: str):
        raise TemplateException(self.meta_file, self.uuid, message)

    def _load_metadata(self) -> dict:
        try:
            with open(self.meta_file, mode='r') as f:
                return json.load(f)
        except Exception as e:
            self.raise_exc(f'Cannot read template meta file: {e}')

    def _verify_metadata(self):
        for required_field in self.META_REQUIRED:
            if required_field not in self.metadata:
                self.raise_exc(f'Missing required field {required_field}')

    def _load_formats(self) -> typing.Dict[uuid.UUID, Format]:
        valid_formats = []
        for index, format_meta in enumerate(self.metadata[TemplateMetaField.FORMATS]):
            try:
                valid_formats.append(Format(self, format_meta))
            except Exception as e:
                logging.error(f'Format #{index} of template "{self.name}" '
                              f'cannot be loaded - {e}')
        return {f.uuid: f for f in valid_formats}

    def has_format(self, format_uuid: uuid.UUID) -> bool:
        return format_uuid in self.formats.keys()

    def __getitem__(self, format_uuid: uuid.UUID) -> Format:
        return self.formats[format_uuid]

    def render(self, format_uuid: uuid.UUID, context: dict) -> DocumentFile:
        return self[format_uuid].execute(context)


class TemplateRegistry:

    META_FILE = 'template.json'

    def __init__(self, config, templates_dir):
        self.config = config
        self.templates_dir = templates_dir
        self.templates = dict()

    def load_templates(self):
        logging.info(f'Loading document templates from {self.templates_dir}')
        for root, dirs, files in os.walk(self.templates_dir):
            for filename in files:
                if filename == self.META_FILE:
                    self._load_template(os.path.join(root, filename))

    def _load_template(self, meta_file: str):
        try:
            template = Template(self.config, meta_file)
            if template.uuid in self.templates:
                logging.warning(f'Duplicate template UUID {template.uuid}'
                                f' - ignoring {meta_file}')
            else:
                self.templates[template.uuid] = template
                logging.info(f'Template from {meta_file} loaded (UUID: {template.uuid})')
        except TemplateException as e:
            logging.error(f'Template from {meta_file} (UUID: {e.template_uuid}) '
                          f'cannot be loaded - {e.message}')
        except Exception as e:
            logging.error(f'Template from {meta_file} '
                          f'cannot be loaded - {type(e).__name__}: {e}')

    def has_template(self, template_uuid: uuid.UUID) -> bool:
        return template_uuid in self.templates.keys()

    def has_format(self, template_uuid: uuid.UUID, format_uuid: uuid.UUID) -> bool:
        return self.has_template(template_uuid) and self[template_uuid].has_format(format_uuid)

    def __getitem__(self, template_uuid: uuid.UUID) -> Template:
        return self.templates[template_uuid]

    def render(self, template_uuid: uuid.UUID, format_uuid: uuid.UUID, context: dict) -> DocumentFile:
        return self[template_uuid].render(format_uuid, context)
