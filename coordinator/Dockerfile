FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY coordinator coordinator
RUN pip install -e /usr/src/app/coordinator
CMD ["coordinator"]
