# Use the official InfluxDB image as the base image
FROM influxdb:latest

# Install Python and pip
RUN apk update && \
    apk add python3 python3-dev py3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

WORKDIR /usr/src/app

COPY ./scripts/* ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python","./entrypoint.py"]
