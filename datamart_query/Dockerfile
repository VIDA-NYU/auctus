FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY datamart_core datamart_core
RUN pip install -e /usr/src/app/datamart_core
COPY datamart_query datamart_query
RUN pip install -e /usr/src/app/datamart_query
CMD ["datamart_query"]
