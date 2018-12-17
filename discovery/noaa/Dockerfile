FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY lib_core lib_core
RUN pip install -e /usr/src/app/lib_core
COPY discovery/noaa /usr/src/app/noaa
RUN pip install -e noaa
CMD ["python", "-m", "noaa_discovery.noaa_discovery"]
