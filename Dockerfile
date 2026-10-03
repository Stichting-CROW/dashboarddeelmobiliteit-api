FROM python:3.9-slim

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY . /srv/flask_app
WORKDIR /srv/flask_app

RUN chmod +x ./start.sh
CMD ["./start.sh"]
