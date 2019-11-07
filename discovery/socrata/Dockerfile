FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock docker/install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY discovery/socrata /usr/src/app/socrata
RUN pip install --no-deps -e /usr/src/app/socrata
ARG version
ENV DATAMART_VERSION ${version}
RUN test -n "${DATAMART_VERSION}"
ENTRYPOINT ["/tini", "--"]
CMD ["python", "-m", "socrata_discovery"]
