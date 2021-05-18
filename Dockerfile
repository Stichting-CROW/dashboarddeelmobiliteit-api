FROM python:3.7-slim

RUN apt-get clean \
    && apt-get -y update
RUN apt-get -y install python3-dev \
    && apt-get -y install build-essential libpq-dev


COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY . /srv/flask_app
WORKDIR /srv/flask_app

RUN chmod +x ./start.sh
CMD ["./start.sh"]
