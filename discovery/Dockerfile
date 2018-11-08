FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY datamart_core datamart_core
RUN pip install -e /usr/src/app/datamart_core
COPY discovery /usr/src/app/discovery
ENTRYPOINT ["/bin/bash", "-c", "if [ \"$1\" = d3m ]; then python discovery/d3m_seeds.py; elif [ \"$1\" = example ]; then python discovery/example_discovery.py; else \"$@\"; fi", "--"]
CMD ["example"]
