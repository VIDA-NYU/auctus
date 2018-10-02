FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY datamart_coordinator/setup.py \
     datamart_coordinator/
COPY datamart_coordinator/datamart_coordinator \
     datamart_coordinator/datamart_coordinator
RUN pip install -e /usr/src/app/datamart_coordinator
COPY datamart_cli/setup.py \
     datamart_cli/
COPY datamart_cli/datamart_cli.py \
     datamart_cli/
RUN pip install -e /usr/src/app/datamart_cli
CMD ["datamart_coordinator"]
