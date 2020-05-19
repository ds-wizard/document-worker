import click
from typing import IO

from document_worker.config import DocumentWorkerConfig, DocumentWorkerCFGConfigParser, DocumentWorkerYMLConfigParser, MissingConfigurationError
from document_worker.worker import DocumentWorker


def validate_config(ctx, param, value: IO):
    content = value.read()
    can_yaml = DocumentWorkerYMLConfigParser.can_read(content)
    can_cfg = DocumentWorkerCFGConfigParser.can_read(content)

    cfg_parser = DocumentWorkerYMLConfigParser()
    if not can_yaml and can_cfg:
        cfg_parser = DocumentWorkerCFGConfigParser()
    elif not can_yaml:
        click.echo('Error: Cannot parse config file', err=True)
        exit(1)

    try:
        cfg_parser.read_string(content)
        cfg_parser.validate()
        return DocumentWorkerConfig(cfg_parser)
    except MissingConfigurationError as e:
        click.echo('Error: Missing configuration', err=True)
        for missing_item in e.missing:
            click.echo(f' - {missing_item}')
        exit(1)


@click.command(name='docworker')
@click.argument('config', type=click.File('r'), callback=validate_config)
@click.argument('templates_dir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
def main(config: DocumentWorkerConfig, templates_dir):
    worker = DocumentWorker(config, templates_dir)
    try:
        worker.run()
    except Exception as e:
        click.echo(f'Ended with error: {e}')
        exit(2)
