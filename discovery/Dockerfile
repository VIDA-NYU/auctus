FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock
ARG version
ENV DATAMART_VERSION ${version}
RUN echo x${DATAMART_VERSION}x && test -n "${DATAMART_VERSION}"

COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY discovery/d3m_seeds_discovery.py discovery/example_discovery.py discovery/test_discovery.py /usr/src/app/
COPY tests/data /usr/src/app/tests/data
ENTRYPOINT ["/bin/bash", "-c", "if [ \"$1\" = d3m ]; then python d3m_seeds_discovery.py; elif [ \"$1\" = example ]; then python example_discovery.py; elif [ \"$1\" = testsuite ]; then python test_discovery.py; else \"$@\"; fi", "--"]
CMD ["example"]
