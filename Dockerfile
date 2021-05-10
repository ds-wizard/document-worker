FROM python:3.9-slim-buster

RUN mkdir -p /app/templates

RUN apt-get update && apt-get install -qq -y wget libpq-dev gcc

RUN wget --quiet https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.buster_amd64.deb && \
    apt-get -qy install ./wkhtmltox_0.12.6-1.buster_amd64.deb && \
    rm -f wkhtmltox_0.12.6-1.buster_amd64.deb

RUN wget --quiet https://github.com/jgm/pandoc/releases/download/2.13/pandoc-2.13-1-amd64.deb && \
    apt-get -qy install ./pandoc-2.13-1-amd64.deb && \
    rm -f pandoc-2.13-1-amd64.deb

COPY fonts /usr/share/fonts/truetype/custom
RUN fc-cache

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . /app

RUN cd addons && bash install.sh && cd ..
RUN python setup.py install

ENV DOCWORKER_CONFIG /app/config.yml
ENV DOCWORKER_WORKDIR /tmp/docworker

RUN mkdir /tmp/docworker

CMD ["docworker"]
