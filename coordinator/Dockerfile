FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY lib_core lib_core
RUN pip install -e /usr/src/app/lib_core
COPY lib_augmentation lib_augmentation
RUN pip install -e /usr/src/app/lib_augmentation
COPY coordinator coordinator
RUN pip install -e /usr/src/app/coordinator
CMD ["coordinator"]
