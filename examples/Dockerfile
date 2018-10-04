FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY datamart_core/setup.py \
     datamart_core/
COPY datamart_core/datamart_core \
     datamart_core/datamart_core
RUN pip install -e /usr/src/app/datamart_core
COPY examples /usr/src/app/examples
ENTRYPOINT ["/bin/bash", "-c", "if [ \"$1\" = d3m ]; then python examples/example_d3m_discovery.py; elif [ \"$1\" = discovery ]; then python examples/example_discovery.py; else \"$@\"; fi", "--"]
CMD ["discovery"]
