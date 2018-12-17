FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
COPY lib_core lib_core
RUN pip install -e /usr/src/app/lib_core
COPY discovery/d3m_seeds_discovery.py discovery/example_discovery.py /usr/src/app/
ENTRYPOINT ["/bin/bash", "-c", "if [ \"$1\" = d3m ]; then python d3m_seeds_discovery.py; elif [ \"$1\" = example ]; then python example_discovery.py; else \"$@\"; fi", "--"]
CMD ["example"]
