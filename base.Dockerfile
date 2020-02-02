FROM python:3.7 AS json

RUN pip --disable-pip-version-check install toml
COPY poetry.lock /root/poetry.lock
RUN python -c "import json, toml; json.dump(toml.load(open('/root/poetry.lock')), open('/root/poetry.lock.json', 'w'))"

FROM python:3.7

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python && /root/.poetry/bin/poetry config virtualenvs.create false
COPY docker/install_deps.py /usr/src/app/
COPY --from=json /root/poetry.lock.json /usr/src/app/
RUN python3 install_deps.py poetry.lock.json
# CI: RUN pip --disable-pip-version-check install coverage==4.5.4
# CI: COPY .coveragerc /usr/src/app/

# This is the common part of the Dockerfiles
# It is copied in all of them, and this file is used for the CI
