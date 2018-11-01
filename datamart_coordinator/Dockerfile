FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY datamart_coordinator/setup.py \
     datamart_coordinator/
COPY datamart_coordinator/datamart_coordinator \
     datamart_coordinator/datamart_coordinator
RUN pip install -e /usr/src/app/datamart_coordinator
CMD ["datamart_coordinator"]
