FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -qq -y wkhtmltopdf wget

RUN wget https://github.com/jgm/pandoc/releases/download/2.9.1.1/pandoc-2.9.1.1-1-amd64.deb && \
    dpkg -i pandoc-2.9.1.1-1-amd64.deb && \
    rm -f pandoc-2.9.1.1-1-amd64.deb

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . /app

RUN python setup.py install

CMD ["docworker", "/app/config.cfg", "/app/templates"]
