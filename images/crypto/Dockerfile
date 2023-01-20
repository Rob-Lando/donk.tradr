# Use the official InfluxDB image as the base image
FROM influxdb:latest

# Install Python and pip
RUN apk add --no-cache python3 python3-dev && \
    python3 -m ensurepip --upgrade && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
    if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
    rm -r /root/.cache

WORKDIR /usr/src/app

COPY ./scripts/* ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python","./entrypoint.py"]
