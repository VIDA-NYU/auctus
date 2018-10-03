FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY datamart_core/setup.py \
     datamart_core/
COPY datamart_core/datamart_core \
     datamart_core/datamart_core
RUN pip install -e /usr/src/app/datamart_core
COPY datamart_ingest/setup.py \
     datamart_ingest/
COPY datamart_ingest/datamart_ingest \
     datamart_ingest/datamart_ingest
RUN pip install -e /usr/src/app/datamart_ingest
CMD ["datamart_ingest"]
