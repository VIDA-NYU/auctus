FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY lib_core lib_core
RUN pip install -e /usr/src/app/lib_core
COPY lib_materialize lib_materialize
RUN pip install -e /usr/src/app/lib_materialize
COPY query query
RUN pip install -e /usr/src/app/query
CMD ["query"]
