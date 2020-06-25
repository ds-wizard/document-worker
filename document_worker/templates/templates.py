import base64
import gridfs
import logging
import os
import pymongo
import shutil
import typing
import uuid

from document_worker.consts import TemplateField, FormatField, TemplateAssetField, TemplateFileField
from document_worker.documents import DocumentFile
from document_worker.templates.formats import Format


class TemplateException(Exception):

    def __init__(self, template_id: str, message: str):
        self.template_id = template_id
        self.message = message


class Asset:

    def __init__(self, asset_uuid: str, filename: str, content_type: str,
                 data: bytearray):
        self.asset_uuid = asset_uuid
        self.filename = filename
        self.content_type = content_type
        self.data = data

    @property
    def data_base64(self) -> str:
        return base64.b64encode(self.data).decode('ascii')

    @property
    def src_value(self):
        return f'data:{self.content_type};base64,{self.data_base64}'


class Template:

    META_REQUIRED = [TemplateField.ID,
                     TemplateField.NAME,
                     TemplateField.METAMODEL_VERSION,
                     TemplateField.FORMATS,
                     TemplateField.FILES]

    def __init__(self, config, template_dir: str, template_id: str, template_data: dict, mongo_db):
        self.config = config
        self.template_dir = template_dir
        self.mongo_db = mongo_db
        self.template_id = template_id
        logging.info(f'Loading {template_id}')
        self.metadata = template_data
        logging.info(f'Verifying {template_id}')
        self._verify_metadata()
        self.name = self.metadata[TemplateField.NAME]
        logging.info(f'Setting up formats for template {self.template_id}')
        self.formats = dict()
        self.assets = dict()
        self.download_template_files()
        self.download_template_assets()

    @property
    def files(self):
        return self.metadata[TemplateField.FILES]

    def raise_exc(self, message: str):
        raise TemplateException(self.template_id, message)

    def _verify_metadata(self):
        for required_field in self.META_REQUIRED:
            if required_field not in self.metadata:
                self.raise_exc(f'Missing required field {required_field}')

    def fetch_asset(self, filename: str) -> typing.Optional[Asset]:
        logging.info(f'Fetching asset "{filename}"')
        if filename in self.assets.keys():
            return self.assets[filename]
        found_asset = None
        for asset in self.metadata.get(TemplateField.ASSETS, []):
            if asset.get(TemplateAssetField.FILENAME, '') == filename:
                found_asset = asset
        if found_asset is None:
            logging.warning(f'Asset "{filename}" not found in template')
            return None
        assets_fs = gridfs.GridFS(self.mongo_db, self.config.mongo.assets_fs_collection)
        file = assets_fs.find_one({'filename': found_asset[TemplateAssetField.UUID]})
        if file is None:
            logging.error(f'Asset "{filename}" not found in GridFS')
            return None
        return Asset(
            asset_uuid=found_asset[TemplateAssetField.UUID],
            filename=found_asset[TemplateAssetField.FILENAME],
            content_type=found_asset[TemplateAssetField.CONTENT_TYPE],
            data=file.read()
        )

    def store_file(self, filename, data, **kwargs):
        full_path = os.path.join(self.template_dir, filename)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, **kwargs) as f:
            f.write(data)

    def download_template_files(self):
        logging.info(f'Storing files of template {self.template_id} locally')
        for template_file in self.files:
            filename = template_file[TemplateFileField.FILENAME]
            data = template_file[TemplateFileField.CONTENT]
            self.store_file(filename, data, mode='w', encoding='utf-8')

    def download_template_assets(self):
        logging.info(f'Storing assets of template {self.template_id} locally')
        assets_fs = gridfs.GridFS(self.mongo_db, self.config.mongo.assets_fs_collection)
        for asset in self.metadata.get(TemplateField.ASSETS, []):
            filename = asset[TemplateAssetField.FILENAME]
            file = assets_fs.find_one({'filename': asset[TemplateAssetField.UUID]})
            data = file.read()
            self.store_file(filename, data, mode='wb')

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

    def __init__(self, config, workdir):
        self.config = config
        self.workdir = workdir
        self.mongo_client = pymongo.MongoClient(**self.config.mongo.mongo_client_kwargs)
        self.mongo_db = self.mongo_client[self.config.mongo.database]
        self.mongo_collection = self.mongo_db[self.config.mongo.templates_collection]

    def get_template(self, template_id: str) -> typing.Optional[Template]:
        template_data = self.mongo_collection.find_one({TemplateField.ID: template_id})

        template_dir = os.path.join(self.workdir, template_id.replace(':', '_'))
        if os.path.exists(template_dir):
            shutil.rmtree(template_dir)
        os.mkdir(template_dir)

        return None if template_data is None else Template(
            self.config, template_dir, template_id, template_data, self.mongo_db
        )
