# Data Stewardship Wizard Document Worker

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/ds-wizard/document-worker)](https://github.com/ds-wizard/document-worker/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/datastewardshipwizard/document-worker)](https://hub.docker.com/r/datastewardshipwizard/document-worker)
[![Document Worker CI](https://github.com/ds-wizard/document-worker/workflows/Document%20Worker%20CI/badge.svg?branch=master)](https://github.com/ds-wizard/document-worker/actions)
[![GitHub](https://img.shields.io/github/license/ds-wizard/document-worker)](LICENSE)

*Worker for assembling and transforming documents*

## Dependencies

-  MongoDB (with GridFS)
-  RabbitMQ
-  [wkhtmltopdf](https://github.com/wkhtmltopdf/wkhtmltopdf)
-  [pandoc](https://github.com/jgm/pandoc)

## Templates

We are using HTML Jinja2 templates described by a JSON file within specified directory. The JSON file can look like this:

```json
{
    "uuid": "4bfe909b-7dbc-40a7-8609-085e9af1df98",
    "name": "My cool template",
    "rootFile": "my/relative/dir/index.html.j2",
    "wkhtmltopdf": "",
    "pandoc": ""
}
```

The `wkhtmltopdf` and `pandoc` fields are optional and you can specify extra command line options and arguments for calls of those commands for converting document. Path specified in `rootFile` is relative to JSON file, then paths in Jinja2 are relative to the root file.


## Docker

Docker image is prepared with basic dependencies and worker installed. It is available though Docker Hub: [datastewardshipwizard/document-worker](https://hub.docker.com/r/datastewardshipwizard/document-worker).

### Build image

You can easily build the image yourself:

```bash
$ docker build . -t docworker:local
```

### Mount points

-  `/app/config.yml` = configuration file (see [example](config.yml))
-  `/app/templates` = directory with templates
-  `/usr/share/fonts/<type>/<name>` = fonts according to [Debian wiki](https://wiki.debian.org/Fonts/PackagingPolicy) (for wkhtmltopdf)

### Fonts

We bundle Docker image with default fonts (for PDF generation, see `fonts` folder):

- [Noto Fonts](https://github.com/googlefonts/noto-fonts) (some variants)
- [Symbola](https://fontlibrary.org/en/font/symbola)

## License

This project is licensed under the Apache License v2.0 - see the
[LICENSE](LICENSE) file for more details.
