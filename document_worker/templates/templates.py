import logging
import pymongo
import uuid
import typing

from document_worker.consts import TemplateField, FormatField
from document_worker.documents import DocumentFile
from document_worker.templates.formats import Format


class TemplateException(Exception):

    def __init__(self, template_id: str, message: str):
        self.template_id = template_id
        self.message = message


class Template:

    META_REQUIRED = [TemplateField.ID,
                     TemplateField.NAME,
                     TemplateField.METAMODEL_VERSION,
                     TemplateField.FORMATS,
                     TemplateField.FILES]

    def __init__(self, config, template_id: str, template_data: dict):
        self.config = config
        self.template_id = template_id
        logging.info(f'Loading {template_id}')
        self.metadata = template_data
        logging.info(f'Verifying {template_id}')
        self._verify_metadata()
        self.name = self.metadata[TemplateField.NAME]
        logging.info(f'Setting up formats for template {self.template_id}')
        self.formats = dict()

    @property
    def files(self):
        return self.metadata[TemplateField.FILES]

    def raise_exc(self, message: str):
        raise TemplateException(self.template_id, message)

    def _verify_metadata(self):
        for required_field in self.META_REQUIRED:
            if required_field not in self.metadata:
                self.raise_exc(f'Missing required field {required_field}')

    def prepare_format(self, format_uuid: uuid.UUID) -> bool:
        str_uuid = str(format_uuid)
        for format_meta in self.metadata[TemplateField.FORMATS]:
            if str_uuid == format_meta[FormatField.UUID]:
                try:
                    self.formats[format_uuid] = Format(self, format_meta)
                except Exception as e:
                    logging.error(f'Format {str_uuid} of template {self.template_id} '
                                  f'cannot be loaded - {e}')
                return True
        return False

    def has_format(self, format_uuid: uuid.UUID) -> bool:
        return any(map(
            lambda f: f[FormatField.UUID] == format_uuid,
            self.metadata[TemplateField.FORMATS]
        ))

    def __getitem__(self, format_uuid: uuid.UUID) -> Format:
        return self.formats[format_uuid]

    def render(self, format_uuid: uuid.UUID, context: dict) -> DocumentFile:
        return self[format_uuid].execute(context)


class TemplateRegistry:

    def __init__(self, config):
        self.config = config
        self.mongo_client = pymongo.MongoClient(**self.config.mongo.mongo_client_kwargs)
        self.mongo_db = self.mongo_client[self.config.mongo.database]
        self.mongo_collection = self.mongo_db[self.config.mongo.templates_collection]

    def get_template(self, template_id: str) -> typing.Optional[Template]:
        template_data = self.mongo_collection.find_one({TemplateField.ID: template_id})
        return None if template_data is None else Template(
            self.config, template_id, template_data
        )
