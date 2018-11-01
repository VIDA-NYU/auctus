FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
RUN pip install numpy pandas
COPY datamart_core/setup.py \
     datamart_core/
COPY datamart_core/datamart_core \
     datamart_core/datamart_core
RUN pip install -e /usr/src/app/datamart_core
COPY datamart_profiler/setup.py \
     datamart_profiler/
COPY datamart_profiler/datamart_profiler \
     datamart_profiler/datamart_profiler
RUN pip install -e /usr/src/app/datamart_profiler
CMD ["datamart_profiler"]
