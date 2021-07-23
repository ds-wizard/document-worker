FROM python:3.9-slim-buster

RUN mkdir -p /app/templates

RUN apt-get update && apt-get install -qq -y wget libpq-dev gcc build-essential python3-dev python3-pip python3-setuptools python3-wheel python3-cffi libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info gdebi libx11-xcb1 libxcb1 libnss3 libxss1 libasound2

RUN wget --quiet https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.buster_amd64.deb && \
    apt-get -qy install ./wkhtmltox_0.12.6-1.buster_amd64.deb && \
    rm -f wkhtmltox_0.12.6-1.buster_amd64.deb

RUN wget --quiet https://github.com/jgm/pandoc/releases/download/2.14.1/pandoc-2.14.1-1-amd64.deb && \
    apt-get -qy install ./pandoc-2.14.1-1-amd64.deb && \
    rm -f pandoc-2.14.1-1-amd64.deb

RUN wget --quiet https://www.princexml.com/download/prince_14.2-1_debian10_amd64.deb && \
    gdebi -n prince_14.2-1_debian10_amd64.deb && \
    rm -f prince_14.2-1_debian10_amd64.deb

RUN wget --quiet -O - https://deb.nodesource.com/setup_12.x | bash - && apt-get install -y nodejs && npm i -g -unsafe-perm relaxedjs

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
