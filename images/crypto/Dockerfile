# Use the official InfluxDB image as the base image
FROM influxdb:2.6-alpine

RUN ls -al /usr/bin/

WORKDIR /usr/src/app

COPY ./scripts/* ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python","./entrypoint.py"]
