FROM python:3.12-rc-slim-buster

RUN apt-get update && apt-get install -y wget && \
    apt-get update && apt-get install -y sudo && \
    apt-get update && apt-get install -y gnupg2 gnupg1

# influxdb.key GPG Fingerprint: 05CE15085FC09D18E99EFB22684A14CF2582E0C5
RUN wget -qO- https://repos.influxdata.com/influxdb.key | apt-key add -
RUN echo "deb https://repos.influxdata.com/debian buster stable" | tee /etc/apt/sources.list.d/influxdb.list
RUN apt-get update && apt-get install -y influxdb

WORKDIR /usr/src/app

COPY ./scripts/* ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python","./entrypoint.py"]
