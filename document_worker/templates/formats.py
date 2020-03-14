import logging
import uuid

from document_worker.templates.steps import create_step, FormatStepException
from document_worker.files import DocumentFile


class FormatMetaField:
    UUID = 'uuid'
    NAME = 'name'
    STEPS = 'steps'


class StepMetaField:
    NAME = 'name'
    OPTIONS = 'options'


class Format:

    FORMAT_META_REQUIRED = [FormatMetaField.UUID,
                            FormatMetaField.NAME,
                            FormatMetaField.STEPS]

    STEP_META_REQUIRED = [StepMetaField.NAME,
                          StepMetaField.OPTIONS]

    def __init__(self, template, metadata: dict):
        self.template = template
        self._verify_metadata(metadata)
        self.uuid = uuid.UUID(metadata[FormatMetaField.UUID])
        self.name = metadata[FormatMetaField.NAME]
        logging.info(f'Setting up format "{self.name}" ({self.uuid})')
        self.steps = self._create_steps(metadata)
        if len(self.steps) < 1:
            self.template.raise_exc(f'Format {self.name} has no steps')

    def _verify_metadata(self, metadata: dict):
        for required_field in self.FORMAT_META_REQUIRED:
            if required_field not in metadata:
                self.template.raise_exc(f'Missing required field {required_field} for format')
        for step in metadata[FormatMetaField.STEPS]:
            for required_field in self.STEP_META_REQUIRED:
                if required_field not in step:
                    self.template.raise_exc(f'Missing required field {required_field} for step in format "{self.name}"')

    def _create_steps(self, metadata: dict):
        steps = []
        for step_meta in metadata[FormatMetaField.STEPS]:
            step_name = step_meta[StepMetaField.NAME]
            step_options = step_meta[StepMetaField.OPTIONS]
            try:
                steps.append(
                    create_step(self.template.config, self.template, step_name, step_options)
                )
            except FormatStepException as e:
                import logging
                logging.warning('Handling job exception', exc_info=True)
                self.template.raise_exc(f'Cannot load step "{step_name}" of format "{self.name}": {e.message}')
            except Exception as e:
                import logging
                logging.warning('Handling job exception', exc_info=True)
                self.template.raise_exc(f'Cannot load step "{step_name}" of format "{self.name}" ({e})')
        return steps

    def execute(self, context: dict) -> DocumentFile:
        result = self.steps[0].execute_first(context)
        for step in self.steps[1:]:
            result = step.execute_follow(result)
        return result
