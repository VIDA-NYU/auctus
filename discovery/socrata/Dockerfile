FROM python:3.5 AS json

RUN pip --disable-pip-version-check install toml
COPY poetry.lock /root/poetry.lock
RUN python -c "import json, toml; json.dump(toml.load(open('/root/poetry.lock')), open('/root/poetry.lock.json', 'w'))"

FROM python:3.5

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python && /root/.poetry/bin/poetry config virtualenvs.create false
COPY docker/install_deps.py /usr/src/app/
COPY --from=json /root/poetry.lock.json /usr/src/app/
RUN python3 install_deps.py poetry.lock.json

COPY lib_core lib_core
RUN pip --disable-pip-version-check install --no-deps -e /usr/src/app/lib_core
COPY discovery/socrata /usr/src/app/socrata
RUN pip --disable-pip-version-check install --no-deps -e /usr/src/app/socrata
ARG version
ENV DATAMART_VERSION ${version}
RUN test -n "${DATAMART_VERSION}"
ENTRYPOINT ["/tini", "--"]
CMD ["python", "-m", "socrata_discovery"]
