FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY datamart_core datamart_core
RUN pip install -e /usr/src/app/datamart_core
COPY discovery/noaa /usr/src/app/noaa
RUN pip install -e noaa
CMD ["python", "-m", "datamart_noaa_discovery.noaa_discovery"]
