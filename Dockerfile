FROM python:3.8-slim-buster

COPY . /app
WORKDIR /app

RUN apt-get update && apt-get install -y wkhtmltopdf wget

RUN wget https://github.com/jgm/pandoc/releases/download/2.9.1.1/pandoc-2.9.1.1-1-amd64.deb && \
    dpkg -i pandoc-2.9.1.1-1-amd64.deb && \
    rm -f pandoc-2.9.1.1-1-amd64.deb

RUN pip install -r requirements.txt

RUN python setup.py install

CMD ["docworker", "/app/config.cfg", "/app/templates"]
