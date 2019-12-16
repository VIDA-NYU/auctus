FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock docker/install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
# CI: RUN pip install coverage==4.5.4
# CI: COPY .coveragerc /usr/src/app/

COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY discovery/example_discovery.py discovery/test_discovery.py /usr/src/app/discovery/
COPY tests/data /usr/src/app/tests/data
ARG version
ENV DATAMART_VERSION ${version}
RUN test -n "${DATAMART_VERSION}"
ENTRYPOINT ["/tini", "--", "/bin/bash", "-c", "set -m; if [ \"$1\" = example ]; then exec python discovery/example_discovery.py; elif [ \"$1\" = testsuite ]; then exec python discovery/test_discovery.py; else exec \"$@\"; fi", "--"]
CMD ["example"]
