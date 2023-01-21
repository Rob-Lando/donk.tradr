# Use the official InfluxDB image as the base image
FROM influxdb:1.8-alpine

# Install Python and pip
RUN apk update && \
    apk add python3=3.8 python3-dev=3.8 py3-pip=20.2 && \
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /usr/src/app

COPY ./scripts/* ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python","./entrypoint.py"]
