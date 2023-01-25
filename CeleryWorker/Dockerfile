FROM python:3.9-slim

COPY requirements.txt /
RUN pip install -r /requirements.txt

ADD . /CeleryWorker
WORKDIR /CeleryWorker

ENTRYPOINT celery -A CeleryWorker worker --loglevel=info
