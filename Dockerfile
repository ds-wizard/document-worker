FROM python:3.8-slim-buster

RUN mkdir -p /app/templates

RUN apt-get update && apt-get install -qq -y wget

RUN wget --quiet https://github.com/wkhtmltopdf/wkhtmltopdf/releases/download/0.12.5/wkhtmltox_0.12.5-1.buster_amd64.deb && \
    apt-get -qy install ./wkhtmltox_0.12.5-1.buster_amd64.deb && \
    rm -f wkhtmltox_0.12.5-1.buster_amd64.deb

RUN wget --quiet https://github.com/jgm/pandoc/releases/download/2.9.1.1/pandoc-2.9.1.1-1-amd64.deb && \
    apt-get -qy install ./pandoc-2.9.1.1-1-amd64.deb && \
    rm -f pandoc-2.9.1.1-1-amd64.deb

COPY fonts /usr/share/fonts/truetype/custom
RUN fc-cache

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . /app

RUN python setup.py install

CMD ["docworker", "/app/config.cfg", "/app/templates"]
