FROM python:3.7.6-slim-buster
RUN apt-get update \
   && apt-get -y install build-essential \
   libpq-dev libssl-dev libffi-dev \
   libxml2-dev libxslt1-dev libssl1.1 \
   postgresql-client
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
RUN mkdir /code
WORKDIR /code
COPY tasks /code/tasks
COPY telegram_notify.py /code
COPY luigi.cfg /code
