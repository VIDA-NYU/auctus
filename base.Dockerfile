FROM python:3.7

ENV TINI_VERSION v0.18.0
RUN curl -Lo /tini https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini && \
    chmod +x /tini

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python && /root/.poetry/bin/poetry config virtualenvs.create false
RUN pip --disable-pip-version-check install toml
COPY docker/install_deps.py poetry.lock /usr/src/app/
RUN python -c "import json, toml; json.dump(toml.load(open('poetry.lock')), open('poetry.lock.json', 'w'))"
RUN python3 install_deps.py poetry.lock.json
RUN pip install -e "git+https://gitlab.com/ViDA-NYU/datamart/lazo-index-service.git@4d4f646ea30f540bfacd77cf3819c206eb220e66#egg=lazo-index-service&subdirectory=src/main/python"
# CI: RUN pip --disable-pip-version-check install coverage==5.0.3
# CI: COPY .coveragerc /usr/src/app/

# This is the common part of the Dockerfiles
# It is copied in all of them, and this file is used for the CI
